# Multilayer Architecture

Phase 2 adds immutable Layer 1 snapshots, independent Layer 2 branches, a nested daily-revenue child DAG, and a final Layer 3 analytics publication. PostgreSQL is the durable handoff between DAGs. Airflow XCom carries only small configuration and task metadata.

The base DAG keeps the Phase 1 current tables for backward compatibility while adding Layer 1 copies in the same transaction. Downstream DAGs reject missing or unsuccessful manifests and cannot read an unrelated latest run. Final publication serializes writers with a PostgreSQL transaction advisory lock.
