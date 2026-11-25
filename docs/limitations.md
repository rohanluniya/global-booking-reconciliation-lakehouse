# Known Limitations & Dev-vs-Production Boundaries

This document explicitly discloses what is verified in `local_native` execution versus what requires a container-enabled multi-node environment.

## Host Environment Constraints (Observed)

1. **No Local Docker/Podman Daemon or Host JVM:** The workstation used to build and verify this repository (macOS `Darwin 24.6.0`, Apple Silicon) has no Docker/Podman runtime and blocks unverified downloaded JVM binaries.
   - **Verified Live:** Full `local_native` stack — Python `3.12.8`, durable partitioned `LocalEventBroker`, FastAvro v1/v2 contracts, all 13 Apache Iceberg V2 tables (`pyiceberg==0.12.0` + SQLite catalog + Parquet), DuckDB `2.0.0`, real `dbt build` (`dbt-core==1.11.17` + `dbt-duckdb==1.11.1`, 35/35 nodes), FastAPI + Control Tower UI on `:8040`, Prometheus `/metrics`, OpenTelemetry JSONL spans, OpenLineage JSON events, 7/7 chaos drills, and all benchmarks.
   - **Statically Validated Only (Not Live-Executed on Host):** Multi-profile `docker-compose.yml` (`postgres-oltp`, `kafka`, `apicurio`, `debezium-connect`, `rustfs`, `lakekeeper`, `spark-ledger`, `trino`, `airflow-scheduler`, `prometheus`, `grafana`, `loki`, `otel-collector`, `marquez`), `infra/docker/Dockerfile.standalone`, `infra/kubernetes/*.yaml`, `processing/spark/iceberg_ledger_reconciliation_job.py`, and `sql/trino/finance_reconciliation_queries.sql`. All YAML, JSON, and Python AST syntax + pinned image tags pass `scripts/verify_static.py`.

## Architectural & Integration Limitations

2. **OpenLineage -> Marquez Live Push:** In `local_native` mode, OpenLineage v2.0.2 `RunEvent` payloads are persisted locally to `.local_data/lineage/openlineage_events.jsonl` and exported to `results/lineage/openlineage_sample.json`. Automatic HTTP push to a live Marquez server (`marquezproject/marquez:0.51.1`) requires starting the Docker `observability` profile.
3. **OpenTelemetry Collector Export:** Local spans export cleanly via `JsonlSpanExporter` to `.local_data/traces/spans.jsonl`. OTLP gRPC export to `otel-collector:4317` is configured in `infra/observability/otel-collector-config.yaml` for container deployments.
4. **Concurrency & Multi-Writer Iceberg Commits:** SQLite-backed PyIceberg catalog (`local_native`) is ideal for single-node batch and API control-plane demos, not high-concurrency distributed writes. Distributed concurrent writers require Lakekeeper REST catalog (`quay.io/lakekeeper/catalog:v0.15.1`) backed by PostgreSQL + S3/RustFS.
5. **Authentication & Regulatory Compliance:** Static dev API keys in `.env.example` are for local demo use only; production requires OIDC/JWT + secret manager + TLS 1.3. The accounting engine demonstrates double-entry mechanics and period-close controls for software engineering — it is not a certified GAAP/IFRS/SOX general ledger.
