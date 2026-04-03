# Developer Guidance: Implementing a Deep Health Check Endpoint

## 1. Overview

To improve the intelligence of our automated failover system, we are moving beyond a simple application health check to a **"deep health check"**.

The goal is to provide the Failover Orchestrator with a detailed, real-time status of the application and all its critical dependencies. This allows the orchestrator to make smarter, more granular decisions. For example, it can distinguish between a critical database failure that requires a regional failover and a degraded third-party API that might only require a notification.

This document outlines the requirements for the new deep health check endpoint that the application team will build.

---

## 2. The Task: Your Team's Responsibility

Your team is responsible for creating a new, non-cached API endpoint that the Failover Orchestrator can call.

*   **Endpoint:** `/actuator/deep-health` (or a similar, agreed-upon path)
*   **Method:** `GET`
*   **Security:** The endpoint must be accessible from within the VPC, but should not be publicly exposed.
*   **Performance:** The check should be lightweight and have a short, enforced timeout (e.g., < 5 seconds) to ensure the orchestrator is not delayed.

This endpoint will perform a series of checks against all critical downstream dependencies and return a structured JSON response detailing their status.

---

## 3. The Contract: Expected JSON Structure

The endpoint **MUST** return a JSON object with the following structure. Adhering to this contract is critical for the orchestrator to parse the response correctly.

### Top-Level Structure

```json
{
  "status": "UP" | "DEGRADED" | "DOWN",
  "components": {
    "database": { ... },
    "cache": { ... },
    "message_queue": { ... },
    "payment_gateway": { ... }
  }
}
```

*   `status`: The overall status of the application.
    *   `UP`: All components are healthy.
    *   `DEGRADED`: One or more components are in a `DEGRADED` state, but none are `DOWN`. The application is functional but may be slow or have partial errors.
    *   `DOWN`: One or more critical components are `DOWN`. The application is considered non-functional.
*   `components`: An object containing the status of each individual dependency. The keys (`database`, `cache`, etc.) should be consistent and agreed upon.

### Component Structure

Each component within the `components` object must have the following structure:

```json
"database": {
  "status": "UP" | "DEGRADED" | "DOWN",
  "details": "A human-readable string with more information about the status."
}
```

*   `status`: The specific status of this dependency.
*   `details`: Important context. For `DEGRADED` or `DOWN` statuses, this should include the error message or reason (e.g., "Connection timeout", "High latency on P99", "Read-only mode detected").

### Full Example

```json
{
  "status": "DEGRADED",
  "components": {
    "database": {
      "status": "UP",
      "details": "Connection successful. Read/write queries are nominal."
    },
    "cache": {
      "status": "DEGRADED",
      "details": "High latency on SET commands (P99 > 200ms)."
    },
    "message_queue": {
      "status": "UP",
      "details": "Connected to brokers. Consumer lag is minimal."
    },
    "payment_gateway": {
      "status": "DOWN",
      "details": "Connection timeout after 3 retries."
    }
  }
}
```

---

## 4. Required Dependency Checks

Your implementation of the deep health check should verify the health of the following types of dependencies.

### a. Database (Aurora)

This is the most critical check.

*   **Connectivity:** Can the application successfully open a connection from its connection pool?
*   **Read/Write Status:** The check MUST attempt a simple, non-destructive write operation (e.g., `UPDATE health_check SET timestamp = NOW()`) within a transaction that is immediately rolled back. This is the only reliable way to detect if the database is in read-only mode (a key state during failover). A simple `SELECT 1` is **not sufficient**.
*   **Status Mapping:**
    *   `UP`: Can connect and perform a write/rollback.
    *   `DOWN`: Cannot connect or the write operation fails (e.g., with a read-only error).

### b. Message Queues (e.g., Kafka)

*   **Connectivity:** Can the application connect to the Kafka brokers?
*   **Consumer Lag:** Is the consumer group lag for critical topics below a defined threshold?
*   **Status Mapping:**
    *   `UP`: Connected and lag is normal.
    *   `DEGRADED`: Connected, but lag is high.
    *   `DOWN`: Cannot connect to brokers.

### c. Caches (e.g., ElastiCache/Redis)

*   **Connectivity:** Can the application connect to the cache cluster?
*   **Functionality:** Can it successfully perform a `SET` and a `GET` operation?
*   **Status Mapping:**
    *   `UP`: Connect, SET, and GET operations are successful and fast.
    *   `DEGRADED`: Operations are successful but slow (high latency).
    *   `DOWN`: Cannot connect or operations fail.

### d. External APIs / Services

For any other critical microservices or third-party APIs (e.g., payment gateways).

*   **Connectivity:** Perform a lightweight "ping" or `GET /health` call to the dependency.
*   **Status Mapping:**
    *   `UP`: Dependency returns a successful response (e.g., HTTP 200).
    *   `DEGRADED`: Dependency returns success but with high latency, or returns a known "degraded" status of its own.
    *   `DOWN`: Dependency returns an error (5xx) or the connection times out.

---

## 5. How the Orchestrator Will Use This Data

The Failover Orchestrator will be configured with a set of rules to interpret this structured data. This allows for more intelligent actions:

*   **If `database` is `DOWN`...**
    *   **Action:** ...the orchestrator will trigger an **immediate regional failover**.
*   **If `payment_gateway` is `DOWN`...**
    *   **Action:** ...the orchestrator will **send a CRITICAL alert** to the on-call team but will **not** fail over the region, as other application functions may still be operational.
*   **If `cache` is `DEGRADED`...**
    *   **Action:** ...the orchestrator will **send a WARNING alert** and continue monitoring. No failover will be triggered.

By providing this detailed, structured health information, you will be empowering the failover system to be significantly safer, smarter, and more resilient.
