# Flash-Sale Checkout System

A full-stack flash-sale reservation system designed to manage limited inventory under high demand while preventing overselling and duplicate reservations.

**Case Study:** Campus Placement Technical Case Study  
**Role:** Full-Stack Software Engineer  
**Author:** Srri Hari T R

## Overview

The system models an e-commerce flash sale in which **500 smartphones** are available for reservation at a scheduled launch time. The design addresses concurrent reservation requests, inventory consistency, request rate limiting, idempotency, and asynchronous event processing.

The project runs locally using Docker Compose and separates the frontend, API, database, cache, and background processing components.

> **Performance note:** The target of 100,000 concurrent customers is a design requirement, not a verified benchmark. The application has been tested locally, and dedicated large-scale load testing is still required.

## Architecture

The application uses the following components:

| Component                      | Responsibility                                                              |
| ------------------------------ | --------------------------------------------------------------------------- |
| React + TypeScript             | Customer-facing reservation interface                                       |
| Nginx                          | Serves the frontend and routes API requests                                 |
| Node.js + Express + TypeScript | Handles API requests and reservation logic                                  |
| PostgreSQL                     | Stores inventory, reservations, and outbox events                           |
| Redis                          | Implements request rate limiting                                            |
| Apache Kafka                   | Transports events for asynchronous processing                               |
| Background Worker              | Consumes and processes reservation events                                   |
| Transactional Outbox           | Records events in the same database transaction as associated state changes |
| Docker Compose                 | Runs the application services locally                                       |

### Request flow

1. A customer submits a reservation request through the frontend.
2. Nginx forwards the API request to the Express backend.
3. The backend validates the request and applies rate limiting.
4. PostgreSQL performs the inventory reservation operation.
5. The reservation and corresponding outbox event are recorded transactionally.
6. Events are published to Kafka and consumed by the background worker.
7. The frontend displays the reservation result.

PostgreSQL acts as the authoritative source for inventory state.

## Key Features

- **Inventory protection:** Database transactions and atomic reservation logic help prevent overselling.
- **Idempotency:** Repeated requests using the same idempotency key can return the existing reservation instead of creating a duplicate.
- **Rate limiting:** Redis limits excessive reservation attempts.
- **Asynchronous processing:** Kafka and a background worker process reservation events.
- **Transactional outbox:** Coordinates database state changes with event publication.
- **Reservation expiry:** Supports expiry-related processing and inventory-state verification.
- **Health monitoring:** Provides an API health endpoint for checking service and dependency connectivity.
- **Containerized environment:** Docker Compose simplifies local setup and testing.

## Technology Stack

- **Frontend:** React, TypeScript
- **Backend:** Node.js, Express, TypeScript
- **Database:** PostgreSQL
- **Caching and rate limiting:** Redis
- **Messaging:** Apache Kafka
- **Web server / reverse proxy:** Nginx
- **Containerization:** Docker, Docker Compose

## Local Setup

### Prerequisites

Install the following tools:

- [Git](https://git-scm.com/)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Docker Compose (included with current Docker Desktop installations)

### 1. Clone the repository

```bash
git clone https://github.com/SrriHariTR/goML-flash-sale.git
cd goML-flash-sale
```

### 2. Configure environment variables

Check the project documentation and environment example files, if present. Configure the required environment variables before starting the application.

Do not commit database passwords, API keys, tokens, or other secrets to GitHub.

### 3. Start the application

From the directory containing `docker-compose.yml`, run:

```bash
docker compose up --build -d
```

Check the service status:

```bash
docker compose ps
```

View the logs:

```bash
docker compose logs -f
```

### 4. Verify the API

If the API health endpoint is exposed on local port `3000`, run:

```bash
curl http://localhost:3000/health
```

Use the actual port and endpoint configured in your Compose file if they differ.

### 5. Stop the application

```bash
docker compose down
```

To remove containers and the Compose network, use the command above. Persistent database volumes are retained unless explicitly removed.

**Warning:** `docker compose down -v` removes Compose-managed volumes and can delete locally stored database data. Use it only when you intentionally want to reset the environment.

## Testing and Observed Results

Local tests covered service health, rate limiting, reservation handling, idempotency, background event processing, and inventory-state verification.

| Test                 | Observed result                                                                             |
| -------------------- | ------------------------------------------------------------------------------------------- |
| API health           | API reported healthy with PostgreSQL and Redis connectivity                                 |
| Rate limiting        | First 10 attempts returned HTTP 202; the 11th returned HTTP 429                             |
| Reservation batch    | 510 requests processed in approximately 3.3 seconds                                         |
| Batch responses      | 490 HTTP 202 responses and 20 HTTP 409 responses                                            |
| Idempotency          | Repeated request returned the existing reservation ID                                       |
| Inventory constraint | A database snapshot showed 6 pending reservations and 494 available units                   |
| Reservation expiry   | A later snapshot showed 522 expired reservations, 0 reserved units, and 500 available units |

The inventory and expiry results represent snapshots from different points in testing. They should not be interpreted as a single continuous test run without additional evidence.

HTTP 202 indicates that a request was accepted for processing, not that payment or purchase was completed. HTTP 409 indicates a conflict according to the API's reservation logic.

## Limitations and Future Improvements

- Run dedicated load tests to evaluate throughput and latency under high concurrency.
- Measure p50, p95, and p99 response times, error rates, and resource utilization.
- Test database contention and inventory correctness under concurrent requests.
- Validate recovery from API, database, Kafka, and worker failures.
- Test duplicate-event handling and end-to-end event delivery.
- Add production-grade monitoring, alerting, authentication, authorization, and secure secret management.
- Configure production HTTPS, infrastructure scaling, and load balancing.

## Learning Outcomes

This project explores practical full-stack engineering concepts, including transactional inventory management, REST API design, idempotency, rate limiting, asynchronous messaging, containerized deployment, and testing of consistency-sensitive workflows.

## Repository

[GitHub — goML-flash-sale](https://github.com/SrriHariTR/goML-flash-sale)

## Case Study Report

The complete technical case study, including the system architecture, design decisions, implementation, and testing results, is available below.

[**View Full-Stack Engineering Case Study (PDF)**](./SrriHariTR_FullStackEng_Report.pdf)
