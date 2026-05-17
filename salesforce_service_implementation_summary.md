# Black Diamond Salesforce Service — Engineering Implementation Summary

## 🎯 Executive Summary
This document outlines the engineering milestones, architectural pivots, and infrastructure hardening completed for the **Black Diamond Salesforce Service**. Built heavily around the core engineering principle that *"Architecture is kindness to your future self,"* this microservice was evolved from a conceptual Version 1 draft into a production-ready, stateless ETL extraction engine ready for deployment.

---

## 🏗️ Architectural Evolutions
We made several critical pivots from the original technical specification to guarantee enterprise-grade scalability, speed, and reliability:

1. **The Framework Upgrade (FastAPI over Flask):** Transitioned the API layer from Flask-RESTX to **FastAPI**. This allowed us to leverage native asynchronous `asyncio` background tasks to handle multi-megabyte Salesforce data streams without blocking the main web server threads.
2. **The Dual-Write Storage Pivot:** Shifted away from direct bulk-upserts into a transactional PostgreSQL/Supabase instance. We re-engineered the service into a **Stateless Dual-Write Pipeline** that compiles data in-memory into Snappy-compressed Parquet files for **MinIO** (Data Lake) while simultaneously streaming real-time events to **Apache Kafka**.
3. **In-Flight PII Masking Shield:** Introduced a dedicated `PiiService` that intercepts outgoing Kafka records and cryptographically masks sensitive fields (`Name`, `Email`, `Phone`) before they hit the open event broker, ensuring strict data privacy compliance.
4. **In-Memory Deduplication:** Implemented a Pandas-driven chunk deduplicator that sorts incoming CRM records by `LastModifiedDate` and drops duplicates purely in RAM, maintaining zero local disk I/O.

---

## 🛠️ Infrastructure & DevOps Hardening
To ensure deterministic builds and safe cluster deployments, the following DevOps pipelines were established:

* **Strict Dependency Pinning:** Hardened the `requirements.txt` to use explicit patch versions (`==` instead of `>=`) for critical data libraries (`pandas==3.0.3`, `pyarrow==24.0.0`, `confluent-kafka==2.14.0`). This guarantees deterministic container builds across all environments.
* **Nomad & Vault Orchestration:** Engineered HCL specification files for the staging environment (`nomad/stage/black-diamond-salesforce-service.hcl`), including automated HashiCorp Vault secret injection so no credentials live in the image.
* **Zero-Downtime Rollouts:** Added a `shutdown_delay = "15s"` parameter to the Nomad configurations, ensuring the old container finishes processing in-flight data chunks before terminating during a rolling update.
* **CI/CD Pipeline (GitHub Actions):** Created a `docker.yaml` workflow that automatically runs the Pytest suite, builds the Docker image with caching, and pushes to the internal Harbor registry (`harbor-registry.service.consul:8085`) on successful merges.

---

## 🩺 Endpoints & System Observability
Expanded the API boundary to support automated cluster health and data lifecycle management:

* **`GET /v1/sync/health`:** Built an unauthenticated health probe for Nomad that actively polls downstream connections (Salesforce OAuth, MinIO buckets, and Kafka brokers) rather than returning a static string.
* **`POST /v1/sync/maintenance/cleanup`:** Implemented an administrative endpoint powered by `maintenance_service.py` that automatically purges stale tracking logs exceeding the `CLEANUP_DAYS` threshold.

---

## 🔒 Security Auditing
* **HMAC Signature Gateway:** Secured all state-changing endpoints with a custom HMAC SHA-256 interceptor that validates time-to-live (TTL) to prevent replay attacks.
* **Git Secrets Prevention:** Patched `.gitignore` before the initial commit to completely block sensitive Salesforce certificates (`private.pem`, `public.crt`), environment files (`.env`), and local sandbox scripts from leaking into version control.

---

## 📝 Documentation Synchronization
To prevent technical debt and documentation drift, all major Markdown assets were completely rewritten to match the final deployed architecture:
* `README.md`
* `docs/API-DOCS.md`
* `docs/DATABASE_DESIGN-DOCS.md`
* `docs/INTEGRATION-DOCS.md`
* `salesforce-service-technical-design.md`

**Status:** 100% Complete, Tested, and Pushed to Version Control.
