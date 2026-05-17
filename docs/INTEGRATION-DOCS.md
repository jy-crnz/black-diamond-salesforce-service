# Black Diamond Salesforce Service — Integration Documentation

This document describes the end-to-end data lifecycle, architectural boundaries, and systemic bridges connecting the Core Orchestrator, the Gateway Security layer, Salesforce CRM Bulk API 2.0, the MinIO Data Lake, and the Apache Kafka event broker network.

## 🗺️ Architectural Topology

The service operates as a stateless, cloud-native ingestion worker. It exposes a webhook extraction interface alongside system lifecycle boundaries managed by a container orchestrator. Rather than routing to a standard transactional database, the pipeline divides data ingestion pathing across a Dual-Write Partitioning Architecture:

```
        [ Client Request ] 
                │
                ▼
┌────────────────────────────────────────┐
│ Phase A: Security Gateway Interceptor  │ ──► Drops bad signatures / expired TTLs
└────────────────────────────────────────┘
                │
                ▼
┌────────────────────────────────────────┐
│ Phase B: API Layer (FastAPI Router)    │ ──► Exposes /sync, /health, & /cleanup
└────────────────────────────────────────┘
                │
                ▼
┌────────────────────────────────────────┐
│ Phase C: Core Extraction Orchestrator  │ ──► Manages JWT handshakes & Bulk API jobs
└────────────────────────────────────────┘
                │
                ▼
┌────────────────────────────────────────┐
│ In-Memory Processing & Transformation  │
│   (Pandas Deduplication & Filtering)   │
└────────────────────────────────────────┘
                │
        ┌───────┴───────┐
        ▼               ▼
 ┌─────────────┐ ┌─────────────┐
 │ PyArrow Min │ │ PII Masking │
 │ Compression │ │   Shield    │
 └──────┬──────┘ └──────┬──────┘
        │               │
        ▼               ▼
┌──────────────┐ ┌──────────────┐
│  Cold Lake   │ │  Real-Time   │
│  S3 Storage  │ │ Event Broker │
│ (MinIO Raw)  │ │(Apache Kafka)│
└──────────────┘ └──────────────┘
```

## 🔄 End-to-End ETL Pipeline Lifecycle

When an authorized external entity pushes a transaction to the `/v1/sync/salesforce` endpoint, the execution loop processes data via four distinct phases running within FastAPI's asynchronous background task runner:

### Phase A: Secure Inbound Ingress

1. An external orchestration engine issues an HTTP POST containing structural configurations (`scan_id`, `org_id`, `objects`, `filters`).

2. The custom HMAC Security Gateway intercepts the inbound request header matrix.

3. The gateway evaluates real-time clock skew using the `x-timestamp` parameter. If the message payload header is older or newer than 5 minutes (300 seconds), it drops immediately to mitigate replay attempts.

4. The gateway fetches the server's local vault configuration matching the client's `x-key-type` (e.g., `core` or `engineer`), recalculates the HMAC SHA-256 signature, and grants ingress upon successful identification.

### Phase B: Asynchronous Extraction Loop

1. The service utilizes an embedded Private Key Certificate (`server.key`) to sign a JSON Web Token (JWT) handshake request, establishing a connection with Salesforce to claim a short-lived OAuth token.

2. The orchestrator dispatches a native bulk query job:
   ```sql
   SELECT Id, Name, Email, Phone, AccountId, OwnerId, CreatedDate, LastModifiedDate FROM Contact
   ```

3. Salesforce instantiates a batch transaction and generates a case-sensitive tracker string (`sf_job_id`).

4. The service goes into an unblocked `asyncio.sleep()` polling sequence, query-checking the status route until Salesforce marks the chunk segments as completely processed.

### Phase C: In-Memory Deduplication & Anonymization

1. The service receives raw record data blocks streaming from the CRM without downloading them entirely onto disk.

2. The extraction orchestrator passes the row objects straight to the DeduplicationService. The service temporarily instantiates a type-safe Pandas DataFrame to sort elements by `LastModifiedDate` and runs an optimized `drop_duplicates(subset=["Id"])` clean sweep, passing the cleaned objects back as standard dictionary lists.

3. The pipeline splits execution into two parallel tracks:

   **Track 1 (Analytical Archives):** Passes raw records directly to the NormalizationService where PyArrow serializes rows into highly compressed, Snappy columnar Parquet schemas completely in RAM.

   **Track 2 (Real-Time Streams):** Passes records to the PiiService which scans matching properties (`Name`, `Email`, `Phone`) and redacts them instantly, swapping target identities with an opaque `[MASKED]` placeholder string.

### Phase D: Dual-Write Partitioned Distribution

1. The service pushes the compiled binary Parquet data stream to the MinIO Data Lake (Raw Zone) over an S3 connection wrapper. Files map into Hive-style folders sorted by client and transaction scopes:

   ```
   s3://glynac-raw-zone/{org_id}/{scan_id}/{object_name_lower}/page_001.parquet
   ```

2. Simultaneously, the SalesforceKafkaProducer marshals the masked real-time event blocks onto the open Apache Kafka bus, appending transactions directly onto dedicated event topic partitions (`sf.contacts.dev`).

## 🩺 System Probes & Operational Control Routes

To maintain compliance inside the enterprise infrastructure, the API router exposes two unauthenticated maintenance gateways:

### 1. Nomad Cluster Health Probe (`GET /v1/sync/health`)

Required by the remote container cluster to monitor system health. Rather than returning a static text string, this route actively tests end-to-end downstream connectivity across three dependency points during validation cycles:

- Confirms active OAuth sessions with the Salesforce CRM instance.
- Polls metadata records from the MinIO server to ensure target bucket read/write permissions exist.
- Queries live system performance metrics from the Apache Kafka broker to confirm the cluster ledger is listening.

If any downstream layer drops connection or times out, the service drops its response status code down to a 503 Service Unavailable and logs a degraded status alert.

### 2. Administrative Retention Cleanup (`POST /v1/sync/maintenance/cleanup`)

Authorized strictly for admin-level engineering roles. Triggering this gateway dispatches a database log optimization routine through the MaintenanceService. It automatically calculates the system retention window based on the environment configuration (`CLEANUP_DAYS`) and drops stale transactional log entries to keep metadata records highly optimized.

## 🛡️ Fault Tolerance & Resiliency Controls

### Nomad Draining Grace Periods

The container specifications mapped inside `nomad/stage/` enforce an explicit `shutdown_delay = "15s"`. When a new rollout triggers, Nomad instantly unregisters the old container instance from Consul service discovery so it stops receiving new traffic, but waits 15 seconds before sending a termination signal. This guarantees that background extraction processes currently streaming massive data chunks can finish cleanly without throwing partial or corrupted sync errors.

### In-Memory Stream Isolation

The compaction engine operates strictly using `io.BytesIO()` memory allocations. Ephemeral file writes are banned inside the container space. This guarantees that file allocation table leaks can never cause host drive capacity crashes, preserving storage resilience.

### Upstream Circuit Breaking

Inbound connection handling is wrapped within complete `try...except...finally` block scopes. If a downstream pipeline component fails mid-extraction, the application catches the event, stops execution, and calls out to the Salesforce backend to cancel the active query job resource. This prevents the service from hitting API transaction capacity limit drops when recovering from infrastructure timeouts.