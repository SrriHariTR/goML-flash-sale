import { useState } from "react";
import "./App.css";

type Reservation = {
  reservationId?: string;
  id?: string;
  status?: string;
  expiresAt?: string;
  replayed?: boolean;
};

function App() {
  const [customerId, setCustomerId] = useState("");
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState("");
  const [reservation, setReservation] = useState<Reservation | null>(null);

  async function reservePhone() {
    if (!customerId.trim()) {
      setMessage("Please enter your customer ID.");
      return;
    }

    setLoading(true);
    setMessage("");
    setReservation(null);

    try {
      const response = await fetch("/api/reservations", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": `web-${customerId.trim()}-${crypto.randomUUID()}`,
        },
        body: JSON.stringify({
          customerId: customerId.trim(),
          quantity: 1,
        }),
      });

      const data = await response.json();

      if (!response.ok) {
        setMessage(data.error || `Reservation failed (${response.status}).`);
        return;
      }

      setReservation(data);
      setMessage(
        response.status === 202
          ? "Reservation accepted successfully!"
          : "Reservation request processed.",
      );
    } catch {
      setMessage("Unable to reach the server. Please try again.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="page">
      <header className="topbar">
        <a className="brand" href="/">
          FLASH<span>SALE</span>
        </a>
        <span className="system-status">
          <span className="dot" /> Reservation system
        </span>
      </header>

      <section className="hero">
        <p className="eyebrow">LIMITED-TIME DROP</p>
        <h1>
          One phone.
          <br />
          One chance.
        </h1>
        <p className="subtitle">
          A limited batch of smartphones. Submit your reservation and let the
          system handle the rush fairly.
        </p>
        <div className="sale-tag">LIMITED STOCK · 1 PER REQUEST</div>
      </section>

      <section className="product-card">
        <div className="product-visual" aria-label="Smartphone illustration">
          <div className="phone">
            <div className="phone-camera" />
            <div className="phone-screen">
              <span>FLASH</span>
              <strong>SALE</strong>
              <div className="screen-orb" />
            </div>
          </div>
        </div>

        <div className="product-info">
          <p className="eyebrow">FEATURED DEVICE</p>
          <h2>Next-gen smartphone</h2>
          <p className="description">
            High demand. Limited inventory. Every reservation is validated
            against the available stock.
          </p>

          <div className="stock-note">
            <span className="stock-icon">01</span>
            <div>
              <strong>One unit per request</strong>
              <p>Inventory is protected against overselling.</p>
            </div>
          </div>

          <label htmlFor="customerId">Customer ID</label>
          <input
            id="customerId"
            value={customerId}
            onChange={(event) => setCustomerId(event.target.value)}
            placeholder="Enter your customer ID"
            maxLength={200}
            disabled={loading}
          />

          <button onClick={reservePhone} disabled={loading}>
            {loading ? "Processing reservation…" : "Reserve your phone →"}
          </button>

          {message && (
            <div
              className={`message ${reservation ? "success" : "error"}`}
              role="status"
            >
              {message}
            </div>
          )}

          {reservation && (
            <div className="reservation-details">
              <strong>Reservation details</strong>
              <p>
                ID: {reservation.reservationId ?? reservation.id ?? "Accepted"}
              </p>
              {reservation.status && <p>Status: {reservation.status}</p>}
              {reservation.expiresAt && (
                <p>
                  Expires: {new Date(reservation.expiresAt).toLocaleString()}
                </p>
              )}
              {reservation.replayed && (
                <p>This is your existing reservation.</p>
              )}
            </div>
          )}
        </div>
      </section>

      <footer>
        <span>FLASHSALE</span>
        <span>Built for high-demand checkout traffic</span>
      </footer>
    </main>
  );
}

export default App;
