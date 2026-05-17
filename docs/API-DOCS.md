# Black Diamond Salesforce Service — API Documentation

This microservice acts as a dedicated, high-performance extraction and transformation engine designed to pull CRM data from Salesforce via the Bulk API 2.0, archive raw columnar datasets in a MinIO storage layer, and stream real-time PII-masked events onto an Apache Kafka message bus.

## 🚀 Service Configuration

| Property | Value |
|----------|-------|
| **Default Port** | 5710 (Dev) / 5711 (Staging/Prod via Nomad) |
| **Base URL Prefix** | `/v1/sync` |
| **Content-Type** | `application/json` |

## 🔒 Authentication Gateway (HMAC SHA-256)

State-changing endpoints (`/salesforce` and `/maintenance/cleanup`) are strictly protected by an edge-intercepting HMAC SHA-256 Signature Validation gateway dependency. Unauthenticated, mismatched, or malformed requests are dropped with a 401 Unauthorized status before hitting background execution loop layers.

### Required HTTP Headers

| Header Name | Type | Description |
|-------------|------|-------------|
| `x-timestamp` | Integer | Current Unix timestamp of the request (e.g., 1778995993). |
| `x-signature` | String | Hexadecimal HMAC SHA-256 signature generated using the shared secret. |
| `x-key-type` | String | Identifies the key permission group. Must be either `core` or `engineer`. |

### Signature Derivation Formula

The signature must be generated deterministically on the client side by hashing a message constructed from the `x-timestamp` and `x-key-type` separated by a colon (`:`):

```python
import hmac
import hashlib

message = f"{x_timestamp}:{x_key_type}".encode("utf-8")
signature = hmac.new(SECRET_KEY, message, hashlib.sha256).hexdigest()
```

### ⚠️ Replay Attack Protection

The server enforces an explicit Time-To-Live (TTL) budget of **5 minutes (300 seconds)**. Requests containing timestamps older or newer than this window are automatically rejected.

## 🛣️ API Endpoints

### 1. Nomad Deployment Health Probe

Exposes comprehensive end-to-end downstream infrastructure connectivity status information. Used by the Nomad orchestrator for container liveness, readiness, and rolling-update scheduling probes.

**Method:** `GET`  
**Path:** `/v1/sync/health`  
**Authentication:** None

#### Example Happy Path Response (200 OK)

```json
{
  "status": "healthy",
  "service": "black-diamond-salesforce-service",
  "version": "1.0.0",
  "salesforce_connected": true,
  "minio_connected": true,
  "kafka_connected": true
}
```

#### Example Degraded Response (503 Service Unavailable)

Returned if any downstream cluster component fails to respond to metadata or connection polls.

```json
{
  "status": "degraded",
  "service": "black-diamond-salesforce-service",
  "version": "1.0.0",
  "salesforce_connected": true,
  "minio_connected": false,
  "kafka_connected": true
}
```

### 2. Trigger Salesforce Extraction Sync

Kicks off the asynchronous, multi-stage dual-write background extraction pipeline. It queries target Salesforce objects, strips duplicate records based on Id and LastModifiedDate, packages raw files as Parquet structures to MinIO, and streams PII-masked records to Kafka.

**Method:** `POST`  
**Path:** `/v1/sync/salesforce`  
**Authentication:** Required (HMAC Headers)

#### Request Body Schema (Application/JSON)

```json
{
  "scan_id": "scan-verification-004",
  "org_id": "glynac-org-001",
  "objects": [
    "Contact"
  ],
  "filters": {
    "last_modified_after": "2026-01-01T00:00:00Z"
  },
  "output_format": "parquet",
  "destination": {
    "minio_bucket": null,
    "kafka_publish": true,
    "clickhouse_load": false
  }
}
```

#### Example Response (202 Accepted)

```json
{
  "success": true,
  "scan_id": "scan-verification-004",
  "status": "started",
  "message": "Successfully initialized background Bulk extraction for 1 objects. Extraction processing asynchronously."
}
```

### 3. Administrative Housecleaning Gateway

Manually forces an optimization cycle to purge old tracking metadata logs and system database transaction records that exceed the defined retention timeline threshold (`CLEANUP_DAYS`).

**Method:** `POST`  
**Path:** `/v1/sync/maintenance/cleanup`  
**Authentication:** Required (HMAC Headers matching `engineer` or `admin` key scopes)  
**Request Body:** None

#### Example Response (200 OK)

```json
{
  "success": true,
  "purged_before": "2026-05-10T17:03:23.000000+00:00",
  "retention_window_days": 7,
  "status": "system_optimized"
}
```

## 🚨 Error Handling & Standard Responses

The application utilizes global exception hooks to format unhandled system failures predictably, preventing raw Python tracebacks from leaking infrastructure architecture to clients.

### 401 Unauthorized (Invalid / Missing Credentials)

Returned if a signature recalculation mismatch occurs, the timestamp verification window fails, or security headers are absent.

```json
{
  "detail": {
    "error": "UNAUTHORIZED",
    "message": "Invalid HMAC signature."
  }
}
```

### 400 Bad Request (Input Parameter Invalidation)

Returned if request structural guidelines are violated, such as passing an empty array inside the `objects` property array.

```json
{
  "detail": "Sync initialization failed: Extraction object list collection cannot be empty."
}
```

### 422 Unprocessable Entity (Missing Parameters)

Returned automatically by Pydantic framework layers if the payload structure skips mandatory variables.

```json
{
  "detail": [
    {
      "loc": ["body", "scan_id"],
      "msg": "field required",
      "type": "value_error.missing"
    }
  ]
}
```

### 500 Internal Server Error (Global Safety Net Catch)

Returned if an unhandled runtime error, storage layer crash, or pipeline execution timeout encounters a fatal collapse.

```json
{
  "success": false,
  "error": "Internal Server Error",
  "message": "An unexpected error occurred during processing. The engineering team has been notified."
}
```