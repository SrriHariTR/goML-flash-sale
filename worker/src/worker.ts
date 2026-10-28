import { Kafka, type EachMessagePayload } from "kafkajs";
import { Pool } from "pg";

const brokers = (process.env.KAFKA_BROKERS || "kafka:9092")
  .split(",")
  .map((broker) => broker.trim());

const topic = process.env.KAFKA_TOPIC || "flash-sale-reservations";

const db = new Pool({
  host: process.env.PGHOST || "postgres",
  port: Number(process.env.PGPORT || 5432),
  database: process.env.PGDATABASE,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
});

const kafka = new Kafka({
  clientId: "flash-sale-worker",
  brokers,
});

const producer = kafka.producer();

const consumer = kafka.consumer({
  groupId: "flash-sale-reservation-workers",
});

let stopping = false;

function delay(ms: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

async function ensureProcessedEventsTable() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS processed_events (
      event_id UUID PRIMARY KEY,
      event_type VARCHAR(100) NOT NULL,
      processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

async function handleMessage({
  topic: messageTopic,
  partition,
  message,
}: EachMessagePayload) {
  if (!message.value) {
    console.warn("Skipping empty Kafka message", {
      topic: messageTopic,
      partition,
      offset: message.offset,
    });
    return;
  }

  let event: {
    eventId?: string;
    eventType?: string;
    reservationId?: string;
  };

  try {
    event = JSON.parse(message.value.toString());
  } catch {
    console.error("Skipping invalid JSON Kafka message", {
      topic: messageTopic,
      partition,
      offset: message.offset,
    });
    return;
  }

  if (!event.eventId || !event.eventType) {
    console.error("Skipping event without eventId or eventType", {
      topic: messageTopic,
      partition,
      offset: message.offset,
    });
    return;
  }

  const client = await db.connect();

  try {
    await client.query("BEGIN");

    const result = await client.query(
      `INSERT INTO processed_events (event_id, event_type)
       VALUES ($1, $2)
       ON CONFLICT (event_id) DO NOTHING
       RETURNING event_id`,
      [event.eventId, event.eventType]
    );

    await client.query("COMMIT");

    if (result.rowCount === 1) {
      console.log("Event processed", {
        eventId: event.eventId,
        eventType: event.eventType,
        reservationId: event.reservationId,
        offset: message.offset,
      });
    } else {
      console.log("Duplicate event ignored", {
        eventId: event.eventId,
        offset: message.offset,
      });
    }
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

async function publishOutboxEvents() {
  while (!stopping) {
    const client = await db.connect();
    let transactionOpen = false;
    let failed = false;
    let eventCount = 0;

    try {
      await client.query("BEGIN");
      transactionOpen = true;

      const result = await client.query(
        `SELECT id, aggregate_id, event_type, payload
         FROM outbox_events
         WHERE status = 'PENDING'
         ORDER BY created_at
         LIMIT 20
         FOR UPDATE SKIP LOCKED`
      );

      eventCount = result.rows.length;

      for (const row of result.rows) {
        try {
          await producer.send({
            topic,
            messages: [
              {
                key: row.aggregate_id,
                value: JSON.stringify(row.payload),
                headers: {
                  eventId: row.aggregate_id,
                  eventType: row.event_type,
                },
              },
            ],
          });

          await client.query(
            `UPDATE outbox_events
             SET status = 'PUBLISHED',
                 published_at = NOW(),
                 attempts = attempts + 1,
                 last_error = NULL
             WHERE id = $1`,
            [row.id]
          );

          console.log("Outbox event published", {
            eventId: row.aggregate_id,
            eventType: row.event_type,
          });
        } catch (error) {
          failed = true;

          const errorMessage =
            error instanceof Error ? error.message : String(error);

          await client.query(
            `UPDATE outbox_events
             SET attempts = attempts + 1,
                 last_error = $2
             WHERE id = $1`,
            [row.id, errorMessage]
          );

          console.error("Outbox publishing failed; event remains pending", {
            eventId: row.aggregate_id,
            error: errorMessage,
          });

          break;
        }
      }

      await client.query("COMMIT");
      transactionOpen = false;
    } catch (error) {
      if (transactionOpen) {
        await client.query("ROLLBACK").catch(() => undefined);
      }

      failed = true;
      console.error("Outbox polling failed:", error);
    } finally {
      client.release();
    }

    if (failed || eventCount === 0) {
      await delay(1000);
    }
  }
}


async function expireReservations() {
  while (!stopping) {
    const client = await db.connect();
    let expiredCount = 0;
    let failed = false;

    try {
      await client.query("BEGIN");

      const result = await client.query(
        `SELECT id, flash_sale_id, customer_id, quantity
         FROM reservations
         WHERE status = 'PENDING'
           AND expires_at <= NOW()
         ORDER BY expires_at
         LIMIT 20
         FOR UPDATE SKIP LOCKED`
      );

      for (const reservation of result.rows) {
        // Release inventory inside the same transaction as the
        // reservation status change.
        const stockResult = await client.query(
          `UPDATE flash_sales
           SET reserved_quantity = reserved_quantity - $2
           WHERE id = $1
             AND reserved_quantity >= $2
           RETURNING id`,
          [reservation.flash_sale_id, reservation.quantity]
        );

        if (stockResult.rowCount !== 1) {
          throw new Error(
            `Could not safely release inventory for reservation ${reservation.id}`
          );
        }

        const reservationResult = await client.query(
          `UPDATE reservations
           SET status = 'EXPIRED'
           WHERE id = $1
             AND status = 'PENDING'
             AND expires_at <= NOW()
           RETURNING id`,
          [reservation.id]
        );

        if (reservationResult.rowCount !== 1) {
          throw new Error(
            `Reservation ${reservation.id} was not transitioned to EXPIRED`
          );
        }

        const eventIdResult = await client.query(
          `SELECT gen_random_uuid()::text AS event_id`
        );

        const eventId = eventIdResult.rows[0].event_id;

        const payload = {
          eventId,
          eventType: "reservation.expired",
          reservationId: reservation.id,
          flashSaleId: reservation.flash_sale_id,
          customerId: reservation.customer_id,
          quantity: reservation.quantity,
          status: "EXPIRED",
          expiredAt: new Date().toISOString(),
        };

        await client.query(
          `INSERT INTO outbox_events
             (aggregate_type, aggregate_id, event_type, payload)
           VALUES ('reservation', $1, 'reservation.expired', $2::jsonb)`,
          [reservation.id, JSON.stringify(payload)]
        );

        expiredCount++;

        console.log("Reservation expired and inventory released", {
          reservationId: reservation.id,
          quantity: reservation.quantity,
          eventId,
        });
      }

      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      failed = true;
      console.error("Reservation expiry processing failed:", error);
    } finally {
      client.release();
    }

    if (failed || expiredCount === 0) {
      await delay(1000);
    }
  }
}


async function startWorker() {
  try {
    await db.query("SELECT 1");
    await ensureProcessedEventsTable();
    console.log("PostgreSQL connected; processed_events is ready");

    await producer.connect();
    console.log("Kafka producer connected");

    await consumer.connect();
    console.log("Kafka consumer connected");

    await consumer.subscribe({ topic, fromBeginning: true });

    // Start consumption without blocking the outbox publishing loop.
    void consumer.run({
      eachMessage: handleMessage,
    }).catch((error) => {
      console.error("Kafka consumer stopped unexpectedly:", error);
      process.exitCode = 1;
    });

    console.log(`Listening for events on ${topic}`);

    await Promise.all([
  publishOutboxEvents(),
  expireReservations(),
]);
  } catch (error) {
    console.error("Worker startup failed:", error);
    process.exitCode = 1;
    stopping = true;
  }
}

async function shutdown() {
  if (stopping) return;

  stopping = true;
  console.log("Shutting down worker...");

  await consumer.disconnect().catch(() => undefined);
  await producer.disconnect().catch(() => undefined);
  await db.end().catch(() => undefined);
}

process.on("SIGINT", () => void shutdown());
process.on("SIGTERM", () => void shutdown());

void startWorker();
