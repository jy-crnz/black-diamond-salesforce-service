# Black Diamond Salesforce Service

An enterprise-grade, cloud-native distributed ingestion engine designed to act as a hardened extraction pipeline. This service securely streams multi-megabyte bulk account and contact records from Salesforce CRM via the Bulk API 2.0, executes high-performance structural transformations, and operates an automated Dual-Write Partitioning Architecture that archives raw columnar Parquet blocks into object storage while streaming synchronized real-time PII-masked events onto an Apache Kafka message broker bus.

## ✨ Core Engineering Features

**Asynchronous Dual-Write Architecture**  
Streams incoming record blocks concurrently to both MinIO Data Lakes (Raw Zone) and Apache Kafka event topics (sf.contacts.dev) using an unblocked, high-throughput asyncio execution cycle.

**In-Flight PII Anonymization Shield**  
Intercepts out-bound Kafka broadcast payloads to apply absolute cryptographic data redaction on targeted client fields (Name, Email, Phone) while preserving raw unstructured data states inside the analytical cloud store.

**In-Memory Block Deduplication**  
Implements a type-safe chunking deduplicator driven by Pandas to process collections of records, automatically sorting data elements and prioritizing the freshest entries via LastModifiedDate arrays before compilation.

**PyArrow Parquet Compaction Engine**  
Transforms unstructured row responses directly in-memory into highly optimized, Snappy-compressed columnar Parquet byte streams, completely avoiding local disk I/O bottlenecks.

**Microservice Security Gateway**  
Enforces perimeter authorization via edge-intercepting HMAC SHA-256 Signature Verification with an integrated clock skew budget to completely mitigate replay attacks.

**Orchestrator Probes & Lifecycle Cleanup**  
Exposes unauthenticated health gateways (`/v1/sync/health`) for cluster engine checks alongside an administrative endpoint (`/v1/sync/maintenance/cleanup`) to optimize state tracking logs.

## 📂 System Architecture & Directory Blueprint

Adhering to strict Feature-First & Layered Separation principles, the repository segregates structural infrastructure layers from business logic sub-domains:

```
black-diamond-salesforce-service/
├── .github/
│   └── workflows/
│       └── docker.yaml              # Automated Build, Test, and Ship CI/CD Pipeline
├── app/
│   ├── api/
│   │   └── routes/
│   │       └── sync.py              # Webhook, Health Probe, and Maintenance Endpoints
│   ├── clients/
│   │   └── bulk_api_client.py       # Async Salesforce Bulk API 2.0 Streaming Client
│   ├── core/
│   │   ├── config.py                # Environment Settings Loader via Pydantic
│   │   └── security.py              # HMAC SHA-256 Signature Verification Layer
│   ├── services/
│   │   ├── extraction_service.py    # Core Extraction Pipeline Orchestrator Loop
│   │   ├── normalization_service.py # PyArrow Snappy Parquet Compaction Converter
│   │   ├── deduplication_service.py # In-Memory List Chunk Deduplicator via Pandas
│   │   ├── pii_service.py           # In-Flight Data Masking & Anonymization Engine
│   │   └── maintenance_service.py   # Administrative Data Retention Manager
│   ├── storage/
│   │   ├── minio_client.py          # Cloud-Native Parquet Object Store Client
│   │   └── kafka_producer.py        # High-Throughput Event Bus Streaming Producer
│   └── main.py                      # FastAPI Application Root & Lifespan Registry
├── docs/
│   ├── API-DOCS.md                  # Detailed Endpoint & Header Contract Specification
│   ├── DATABASE_DESIGN-DOCS.md      # Analytical Engine Ingestion Tuning Sheets
│   └── INTEGRATION-DOCS.md          # Inter-service Data Life-cycle Topology Diagrams
├── nomad/
│   └── stage/
│       └── black-diamond-salesforce-service.hcl # Validated Staging Cluster Spec Sheet
├── .env.example                     # Public Environment Var Spec Sheet
├── check_kafka.py                   # Event Stream Diagnostic and Verification Tool
├── requirements.txt                 # Strictly Pinned Dependency Blueprint
└── test_sync.py                     # Route Mapping & Border Assertion Unit Suite
```

## ⚙️ Quick-Start Installation

### 1. Environment Setup

Clone this repository into your workspace, ensure you are running Python 3.11+, and instantiate an isolated virtual environment:

```powershell
# Create virtual environment
python -m venv venv

# Activate the environment (Windows PowerShell)
.\venv\Scripts\Activate.ps1
```

### 2. Dependency Ingestion

Install all strictly pinned production and compilation-heavy data dependencies (pandas, pyarrow, confluent-kafka, minio) recorded in the deterministic blueprint:

```powershell
pip install -r requirements.txt
```

### 3. Local Variable Configuration

Create a private .env file from the repository baseline example template:

```powershell
cp .env.example .env
```

Open your newly created .env file and insert your active environment parameters, including storage endpoints, broker hostnames, and Salesforce Connected App OAuth credentials.

## 🚀 Local Development Execution

### 1. Launching the Service

To spin up the service locally with active filesystem hot-reloading:

```powershell
uvicorn app.main:app --reload --port 5710
```

Once the application lifespan startup verifies your configuration and downstream connections, navigate to the built-in Interactive Swagger Documentation UI:

👉 [http://localhost:5710/docs](http://localhost:5710/docs)

### 2. Executing Automated Tests

To run the mock execution boundary test suite and ensure route mapping validation holds true:

```powershell
pytest test_sync.py -v
```

## 🔒 Diagnostic Testing & Security Verification

Because the application endpoints are hardened under a multi-key HMAC gateway, raw execution requests will result in an automated 401 Unauthorized rejection unless signed with real-time credentials.

### Webhook Execution Testing

Run the local test utility script to output active valid verification signatures:

```powershell
python generate_headers.py
```

Copy the resulting `x-timestamp` and `x-signature` tokens and apply them to the header arguments inside the Swagger UI client drawer.

### Kafka Event Stream Verification

To verify that data pipeline anonymization layers are masking fields actively in your stream topic workspace, use the direct partition diagnostic script:

```powershell
python check_kafka.py
```

## 🛡️ Critical Operational Notes

Before contributing updates or modifying the ingestion layers, adhere to these core production constraints:

**Build Determinism**  
Every dependency in `requirements.txt` must remain strictly pinned to an explicit patch version using `==`. Range matching with `>=` is forbidden to prevent regression anomalies inside container builds.

**Nomad Rolling Updates**  
The cluster orchestration specification in `nomad/stage/` enforces a `shutdown_delay = "15s"`. This grace period guarantees that containers never terminate abruptly mid-chunk processing during rollouts, eliminating dropped transactions.

**Kafka Append-Only Logging**  
Kafka offsets read from the cluster ledger are immutable. When running validation routines locally, ensure `check_kafka.py` targets unique transactional `scan_id` strings to bypass cached historical blocks.