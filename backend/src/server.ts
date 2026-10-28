import express from "express";
import { Pool } from "pg";
import type { PoolClient } from "pg";
import { createClient } from "redis";
import dotenv from "dotenv";

dotenv.config();

const app = express();
const port = Number(process.env.PORT || 3000);

app.use(express.json({ limit: "16kb" }));

const db = new Pool({
  host: process.env.PGHOST || "postgres",
  port: Number(process.env.PGPORT || 5432),
  database: process.env.PGDATABASE,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
});

const redis = createClient({
  url: process.env.REDIS_URL || "redis://redis:6379",
});


const RATE_LIMIT = 10;
const RATE_WINDOW_MS = 60_000;

const rateLimitScript = `
  local key = KEYS[1]
  local now = tonumber(ARGV[1])
  local window = tonumber(ARGV[2])
  local limit = tonumber(ARGV[3])

  redis.call("ZREMRANGEBYSCORE", key, "-inf", now - window)

  local count = redis.call("ZCARD", key)

  if count >= limit then
    local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
    local retryAfter = 1

    if oldest[2] then
      retryAfter = math.max(
        1,
        math.ceil((tonumber(oldest[2]) + window - now) / 1000)
      )
    end

    return {0, retryAfter}
  end

  redis.call("ZADD", key, now, ARGV[4])
  redis.call("PEXPIRE", key, window)

  return {1, 0}
`;


redis.on("error", (error) => {
  console.error("Redis connection error:", error.message);
});

app.get("/health", async (_req, res) => {
  try {
    await db.query("SELECT 1");
    await redis.ping();

    res.status(200).json({
      status: "healthy",
      service: "flash-sale-api",
      database: "connected",
      redis: "connected",
    });
  } catch (error) {
    console.error("Health check failed:", error);
    res.status(503).json({
      status: "unhealthy",
      service: "flash-sale-api",
    });
  }
});

app.get("/", (_req, res) => {
  res.json({
    message: "Flash-Sale API is running",
    health: "/health",
    reservationEndpoint: "POST /api/reservations",
  });
});

app.post("/api/reservations", async (req, res) => {
  const customerId =
    typeof req.body?.customerId === "string"
      ? req.body.customerId.trim()
      : "";

  const quantity =
    req.body?.quantity === undefined ? 1 : req.body.quantity;

  const idempotencyKey = req.get("Idempotency-Key")?.trim();

  if (!customerId || customerId.length > 200) {
    res.status(400).json({
      error: "customerId is required and must be at most 200 characters.",
    });
    return;
  }

  if (
    typeof quantity !== "number" ||
    !Number.isSafeInteger(quantity) ||
    quantity < 1 ||
    quantity > 500
  ) {
    res.status(400).json({
      error: "quantity must be an integer between 1 and 500.",
    });
    return;
  }

  if (
    !idempotencyKey ||
    idempotencyKey.length > 200
  ) {
    res.status(400).json({
      error: "Provide an Idempotency-Key header of 1–200 characters.",
    });
    return;
  }

  
  // Check for a legitimate retry before consuming a rate-limit slot.
  try {
    const existing = await db.query(
      `SELECT customer_id, quantity
       FROM reservations
       WHERE idempotency_key = $1`,
      [idempotencyKey]
    );

    if (existing.rows.length > 0) {
      if (
        existing.rows[0].customer_id !== customerId ||
        existing.rows[0].quantity !== quantity
      ) {
        res.status(409).json({
          error:
            "This Idempotency-Key was already used with a different request.",
        });
        return;
      }

      // Allow same-key retries to reach the existing idempotency handler.
    } else {
      const now = Date.now();
      const uniqueRequestId =
        `${now}:${Math.random().toString(36).slice(2)}`;

      const result = await redis.eval(rateLimitScript, {
        keys: [`flash-sale:rate:${customerId}`],
        arguments: [
          String(now),
          String(RATE_WINDOW_MS),
          String(RATE_LIMIT),
          uniqueRequestId,
        ],
      }) as number[];

      if (Number(result[0]) === 0) {
        const retryAfter = Number(result[1]) || 1;

        res.setHeader("Retry-After", String(retryAfter));
        res.status(429).json({
          error: "Too many reservation attempts. Please retry later.",
          retryAfterSeconds: retryAfter,
        });
        return;
      }
    }
  } catch (error) {
    console.error("Rate limiter failed:", error);

    // Fail closed: do not accept reservations if the limiter is unavailable.
    res.status(503).json({
      error: "Checkout is temporarily unavailable. Please retry shortly.",
    });
    return;
  }


  let client: PoolClient | undefined;
  let transactionOpen = false;

  try {
    client = await db.connect();

    await client.query("BEGIN");
    transactionOpen = true;

    // Serialize requests that use the same idempotency key.
    await client.query(
      "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
      [idempotencyKey]
    );

    // Return an existing reservation for a legitimate retry.
    const existingResult = await client.query(
      `SELECT id, flash_sale_id, customer_id, quantity,
              status, expires_at, created_at
       FROM reservations
       WHERE idempotency_key = $1`,
      [idempotencyKey]
    );

    if (existingResult.rows.length > 0) {
      const existing = existingResult.rows[0];

      await client.query("COMMIT");
      transactionOpen = false;

      if (
        existing.customer_id !== customerId ||
        existing.quantity !== quantity
      ) {
        res.status(409).json({
          error:
            "This Idempotency-Key was already used with a different request.",
        });
        return;
      }

      res.status(200).json({
        reservationId: existing.id,
        flashSaleId: existing.flash_sale_id,
        quantity: existing.quantity,
        status: existing.status,
        expiresAt: existing.expires_at,
        replayed: true,
      });
      return;
    }

    // Find the currently active sale.
    const saleResult = await client.query(
      `SELECT id
       FROM flash_sales
       WHERE status = 'ACTIVE'
         AND starts_at <= NOW()
         AND ends_at > NOW()
       ORDER BY starts_at
       LIMIT 1`,
    );

    if (saleResult.rows.length === 0) {
      await client.query("ROLLBACK");
      transactionOpen = false;

      res.status(409).json({
        error: "No active flash sale is available.",
      });
      return;
    }

    const flashSaleId = saleResult.rows[0].id;

    // The database performs the final atomic inventory check.
    const inventoryResult = await client.query(
      `UPDATE flash_sales
       SET reserved_quantity = reserved_quantity + $2
       WHERE id = $1
         AND status = 'ACTIVE'
         AND starts_at <= NOW()
         AND ends_at > NOW()
         AND reserved_quantity + sold_quantity + $2
             <= total_quantity
       RETURNING id`,
      [flashSaleId, quantity]
    );

    if (inventoryResult.rows.length === 0) {
      await client.query("ROLLBACK");
      transactionOpen = false;

      res.status(409).json({
        error: "Insufficient inventory or the sale has ended.",
      });
      return;
    }

    const reservationResult = await client.query(
      `INSERT INTO reservations (
         id, flash_sale_id, customer_id, idempotency_key,
         quantity, status, expires_at, created_at
       )
       VALUES (
         gen_random_uuid(), $1, $2, $3, $4,
         'PENDING', NOW() + INTERVAL '5 minutes', NOW()
       )
       RETURNING id, flash_sale_id, customer_id, quantity,
                 status, expires_at, created_at`,
      [flashSaleId, customerId, idempotencyKey, quantity]
    );

    const reservation = reservationResult.rows[0];

    const event = {
      eventId: reservation.id,
      eventType: "reservation.created",
      reservationId: reservation.id,
      flashSaleId: reservation.flash_sale_id,
      customerId: reservation.customer_id,
      quantity: reservation.quantity,
      status: reservation.status,
      expiresAt: reservation.expires_at,
      createdAt: reservation.created_at,
    };

    // Persist the event in the same transaction as the reservation.
    await client.query(
      `INSERT INTO outbox_events (
         aggregate_type, aggregate_id, event_type, payload
       )
       VALUES ('reservation', $1, 'reservation.created', $2::jsonb)`,
      [reservation.id, JSON.stringify(event)]
    );

    await client.query("COMMIT");
    transactionOpen = false;

    res.status(202).json({
      message: "Reservation accepted.",
      reservationId: reservation.id,
      flashSaleId: reservation.flash_sale_id,
      quantity: reservation.quantity,
      status: reservation.status,
      expiresAt: reservation.expires_at,
      replayed: false,
    });
  } catch (error) {
    if (client && transactionOpen) {
      await client.query("ROLLBACK").catch((rollbackError) => {
        console.error("Transaction rollback failed:", rollbackError);
      });
    }

    console.error("Reservation request failed:", error);

    res.status(500).json({
      error: "Unable to process the reservation. Please retry safely.",
    });
  } finally {
    client?.release();
  }
});

async function startServer() {
  try {
    await db.query("SELECT 1");
    await redis.connect();

    app.listen(port, "0.0.0.0", () => {
      console.log(`Flash-Sale API listening on port ${port}`);
    });
  } catch (error) {
    console.error("Failed to start Flash-Sale API:", error);
    process.exit(1);
  }
}

startServer();
