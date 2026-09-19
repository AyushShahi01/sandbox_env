# PROJECT_PLAN.md — Standalone ETL Sandbox

## 0. Instructions for the coding AI

Build **only the standalone ETL sandbox** described in this document. Deliver a complete, runnable repository with actual source code, Docker configuration, SQL, synthetic seed data, tests, and operating instructions. Do not stop at a scaffold or provide pseudocode. Follow the implementation sequence and acceptance criteria below; make reasonable engineering decisions where a minor detail is unspecified and record them in the README. When an external dependency or a command cannot be verified or run in your environment, state that explicitly rather than claiming success.

**Strict scope boundary:** Do **not** implement LangChain, LangGraph, Ollama, AI agents, remediation agents, Slack/Teams, ticketing, dashboards, Kafka, Airbyte, dbt, Kubernetes, or the wider managed-services system. This repository is a deliberately small, self-contained pipeline that may be monitored by a *separate* AI project later. Do not clone or copy another person's ETL repository or require downloads of third-party datasets.

**Target environment:** Windows 10/11, PowerShell, Docker Desktop with Linux containers/WSL2, Docker Compose v2, VS Code. All runtime services and pipeline dependencies run in Docker; do not depend on a separately installed Airflow, dbt, Airbyte, PostgreSQL, or the Windows Python 3.14 environment. Use locally generated synthetic e-commerce data only.

## 1. Goal and definition of done

Build one clear, end-to-end **e-commerce ETL DAG** that reads orders and customers from a PostgreSQL source database, transforms and validates the records, and publishes clean dimensional and fact tables to a separate PostgreSQL warehouse database. Orchestrate the work with Apache Airflow running in Docker Compose. It must be repeatable, safe to rerun, easy to troubleshoot, and capable of demonstrating one success, one intentional operational failure, and one intentionally bad-data failure.

A project is **done** only when a clean setup can:

1. Start from an empty checkout with documented commands and a private `.env` file derived from `.env.example`.
2. Start and initialize the databases and Airflow without a hidden manual step.
3. Seed deterministic synthetic source records (for example, 100 customers and 1,000 orders; exact numbers configurable and documented).
4. Display one parseable Airflow DAG named `sandbox_ecommerce_etl`.
5. Complete a successful run and populate `warehouse.dim_customers` and `warehouse.fact_orders` with validated data.
6. Rerun the same DAG safely without duplicate fact records or inconsistent totals.
7. Reject an intentionally invalid batch **before** changing published warehouse tables.
8. Produce an observable failed Airflow task for an explicitly injected operational error, with actionable logs and an unchanged warehouse where applicable.
9. Pass automated unit and integration tests with reproducible run commands.
10. Include a README that a new developer can follow on Windows PowerShell.

Do not claim these checks have passed unless they were actually executed. The implementation must not contain hardcoded production credentials or use real customer data.

## 2. Architecture and data flow

```text
Deterministic synthetic seed script
            |
            v
PostgreSQL: source_db
  source.customers
  source.orders
            |
            v
Apache Airflow: sandbox_ecommerce_etl
  check_source -> extract -> transform -> stage -> validate -> publish -> summarize
                         |         |          |
                         |         |          +-- critical check fails: stop; published tables unchanged
                         |         +-- warehouse staging keyed by batch/run ID
                         +-- Python transforms, no LLM
            |
            v
PostgreSQL: warehouse_db
  staging.customers / staging.orders (batch scoped)
  warehouse.dim_customers
  warehouse.fact_orders
  ops.etl_runs (optional lightweight audit table)
```

**Infrastructure choice:** One PostgreSQL container with **three separate databases** (`airflow_db`, `source_db`, `warehouse_db`) is acceptable for a lightweight local sandbox, provided each has an explicitly documented role, credentials are configured correctly, and no DAG points accidentally at the metadata database. If separate PostgreSQL containers are simpler for reliable initialization, document the reason and keep the same logical separation. Prefer one container to conserve laptop resources. Airflow services communicate with PostgreSQL by Compose service hostname and internal port `5432`, never by `localhost` from inside a container.

Use a **pinned, internally compatible Airflow image and Python version** selected against the official documentation for that exact version. Configure the appropriate web/API server, scheduler, metadata migration, user authentication, and dependency installation for the chosen major version. Do not mix Airflow 2 commands/configuration with Airflow 3. Record versions in the README, pin dependencies/constraints, and use `docker compose` (not deprecated `docker-compose`). Avoid unnecessary Airflow services, message brokers, and dashboards.

## 3. Repository structure to create

```text
autoops-etl-sandbox/
├── README.md
├── PROJECT_PLAN.md                  # This document
├── .env.example
├── .gitignore
├── Dockerfile                       # Only if needed to add pinned provider/dependencies
├── docker-compose.yml
├── requirements.txt                 # Explicit, compatible versions if needed
├── dags/
│   └── sandbox_ecommerce_etl.py
├── etl/
│   ├── __init__.py
│   ├── config.py
│   ├── db.py
│   ├── extract.py
│   ├── transform.py
│   ├── stage.py
│   ├── validate.py
│   ├── publish.py
│   └── reporting.py
├── database/
│   ├── init/                         # Fresh-volume DB/bootstrap SQL or scripts
│   │   └── 01_create_databases.sql
│   ├── source_schema.sql
│   ├── warehouse_schema.sql
│   └── verification_queries.sql
├── scripts/
│   ├── seed_source.py
│   ├── bootstrap.ps1                 # Optional: explicit, idempotent helper
│   └── verify_pipeline.py
├── tests/
│   ├── test_transform.py
│   ├── test_validation.py
│   ├── test_idempotency.py
│   └── test_dag_import.py
└── docs/
    ├── DATA_CONTRACT.md
    ├── RUNBOOK.md
    └── FAILURE_SCENARIOS.md
```

Adjust only if justified; keep boundaries and names consistent. No empty placeholder files. Make sure the `etl/` package is importable in **both** scheduler and task execution environments (e.g., install it in the custom image or configure a shared mounted code path, rather than relying on a Windows-only Python path). Never move large datasets via Airflow XCom; pass only run/batch identifiers and small metadata.

## 4. Dataset and contracts

Generate synthetic data locally with a fixed random seed and a deterministic base timestamp. Use 100 customers and 1,000 orders as documented defaults, but allow optional seed-size parameters. Do not download Olist or another third-party dataset.

**Source** (`source_db`, schema `source`):

- `customers`: `customer_id` primary key, `email` (synthetic), `country`, `created_at`.
- `orders`: `order_id` primary key, `customer_id` foreign key, `order_timestamp`, `quantity` positive integer, `unit_price` nonnegative numeric, `status` in a documented finite set (e.g. `placed`, `paid`, `cancelled`).
- Generate valid links from orders to customers, stable IDs, reproducible values, and realistic timestamps. Seed must be idempotent via upsert or explicitly documented safe reset of **sandbox source only**. Do not accidentally overwrite non-sandbox data.

**Warehouse** (`warehouse_db`):

- `staging.customers` and `staging.orders` hold batch-scoped, transformed rows with `batch_id` and appropriate uniqueness on `(batch_id, business_key)`; they must permit an invalid test batch to reach the validation step without contaminating published tables.
- `warehouse.dim_customers`: stable customer business key and relevant normalized attributes.
- `warehouse.fact_orders`: unique `order_id`, `customer_id`, `order_timestamp`, `quantity`, `unit_price`, calculated `order_total`, `status`, and any necessary load metadata. Choose `NUMERIC` for money, not floating-point.
- Document exact column types, constraints, nullability, transformation formulas, and the decision to include or exclude cancelled orders from financial totals in `docs/DATA_CONTRACT.md`.

**Transformation rules:** normalize string fields without altering business keys; validate status values and timestamps; calculate `order_total = quantity * unit_price` using decimal-safe arithmetic, rounded to two decimal places only where specified in the contract. Keep source data immutable during normal ETL runs. Define expected source-to-warehouse row counts and explain any intentional filtering.

## 5. DAG and task behavior

Create a single DAG named **`sandbox_ecommerce_etl`** with an understandable linear or minimally branched sequence:

1. `check_source`: test connectivity and verify source tables exist. Emit an actionable failure when unavailable.
2. `extract`: read source records into a bounded representation or perform controlled database-to-database transfer; do not XCom entire tables.
3. `transform`: apply deterministic, testable Python transformations; no external API or AI.
4. `stage`: replace/upsert **only the current batch's** staging rows in a transaction. Use a stable batch ID derived from the DAG run identity so retrying the same run targets the same batch.
5. `validate`: run critical checks against **this batch** and fail with precise counts/reasons if invalid.
6. `publish`: in one warehouse transaction, atomically replace the current published snapshot (or use a fully specified equivalent idempotent upsert/merge strategy) from the validated batch; commit only after the entire operation succeeds. Never truncate published tables outside that transaction.
7. `summarize`: log batch ID, source/staged/published counts, totals, elapsed time, and status. Optionally write a small operational audit row.

Tasks should be logically separate and use established, supported Airflow APIs for the pinned version. Avoid a DAG-parse-time network connection or database write. Keep the DAG import-safe and configure `catchup=False`, an explicit schedule appropriate for a manual demo (prefer `schedule=None` initially), `max_active_runs=1`, and a clear retry policy. For incident demonstrations, **default Airflow task retries to zero** so an operational failure remains visible; document how to change this if later needed. The entire pipeline should also be runnable as modular Python logic for tests without launching the Airflow UI.

A failed task must cause the DAG run to fail or block downstream publication. Do not silently swallow exceptions, call a failed load successful, or conceal validation failures behind permissive warnings.

## 6. Safety, idempotency, and quality gate

Implement these invariants and test them:

- **Before publication:** check required fields, missing/duplicate `order_id`, unknown customers, invalid status, `quantity <= 0`, `unit_price < 0`, invalid/future timestamps (define a clear policy), arithmetic discrepancies, and source/staged row-count consistency. Add straightforward freshness or count thresholds only if deterministic and documented.
- **No bad data published:** all critical validations operate on the candidate batch before `publish`. If validation fails, the previous `warehouse.dim_customers` and `warehouse.fact_orders` remain queryable and unchanged.
- **No duplicate results:** rerunning the same DAG run and triggering a new identical snapshot yield one published row per `order_id`, with stable row counts and aggregate totals.
- **No partial publication:** publish customer and order tables within a single PostgreSQL transaction. A simulated failure before commit rolls back all publication changes.
- **No concurrent snapshot races:** `max_active_runs=1`; consider database-level guarding if relevant to implementation.
- **Transparent retries:** staging writes are reentrant and operate on only the current batch. Do not assume a connection timeout means a transaction failed; determine idempotency from database design, not guesses.
- **Secrets:** `.env.example` contains names and dummy values only; `.env` is gitignored. Do not print passwords in logs. Container health checks and initialization should not expose secrets unnecessarily.

For reliable validation-failure testing, inject bad rows in a **candidate staging batch** through an explicit opt-in DAG run config or test fixture. Never require editing the production-facing warehouse table or corrupting normal seed data. If source corruption is used in a test, restore the prior source state and document the cleanup.

## 7. Controlled fault-injection scenarios

Implement explicit, sandbox-only failure switches via `dag_run.conf` (or an equally clear test interface). Off by default; validate allowed values and refuse unknown options. Suggested modes:

| Mode | Injected behavior | Required outcome |
|---|---|---|
| `none` | Valid deterministic data | Successful DAG; tables populated |
| `extract_error` | Raise a clearly labeled exception before extracting | DAG failure; no publication |
| `transform_error` | Raise a clearly labeled exception during transformation | DAG failure; no publication |
| `invalid_data` | Add a duplicate, null, negative value, or unknown customer in this batch's staging data | Validation task fails; published tables unchanged |
| `publish_error` | Raise a test exception within the warehouse publication transaction before commit | Rollback; previously published snapshot unchanged |

Implement at least `none`, `extract_error`, and `invalid_data`; `publish_error` is strongly recommended to prove transaction safety. Use obvious `SANDBOX_INJECTED_FAILURE` markers in task logs. Do not implement real outages by killing unrelated containers or changing credentials. The project deliberately does **not** auto-retry failed tasks or remediate incidents using AI.

## 8. Docker setup and developer experience

Supply an actual Compose file that defines PostgreSQL, Airflow initialization, and the minimum Airflow runtime services required by the pinned version. Implement dependable startup: PostgreSQL health check, explicit creation of all three databases, schema initialization, Airflow metadata migrations, and local admin access where supported. Explain that PostgreSQL `/docker-entrypoint-initdb.d` scripts run only on an **empty** data volume; provide a safe, non-destructive procedure for a partially initialized local environment. Never prescribe `docker compose down -v` as a routine troubleshooting step.

Use a project-specific Compose name and avoid fixed `container_name` entries. Document and check host ports to avoid collisions with the user's preinstalled Airflow/Airbyte. Set sensible defaults in `.env.example` and document overrides. Mount only necessary source folders and use volumes for database/metadata durability and task logs. Add `.dockerignore` if needed. Do not assume the user has PostgreSQL, Airflow, or any Python provider installed on the Windows host.

README must include copy-paste **PowerShell** instructions for:

1. Prerequisite checks (`docker --version`, `docker compose version`, `docker info`).
2. Copying `.env.example` to `.env`, generating safe secrets, and configuring ports.
3. `docker compose config --quiet` and image build.
4. Starting PostgreSQL and initializing schema/metadata without race conditions.
5. Starting Airflow, creating/logging into the local admin user as appropriate.
6. Seeding synthetic source records, then verifying source row counts.
7. Listing/import-checking the DAG and triggering a normal run using commands valid for the pinned Airflow version **or** precise UI instructions.
8. Verifying Airflow task results, source counts, warehouse counts, duplicate count, and aggregate revenue with copy-paste SQL.
9. Triggering each supported fault mode and checking that published tables remain unchanged.
10. Running tests, reading logs, stopping (`docker compose down` without `-v`), and restarting while retaining data.

Prefer robust scripts over manual database edits. If a CLI command differs between Airflow major versions, provide **only** the command correct for the pinned image.

## 9. Testing plan

Use `pytest` with clear separation between pure unit tests and Docker-backed integration tests. Tests must be runnable via documented commands, and an unavailable Docker database should produce an explicit skip or actionable error rather than misleading success.

**Unit tests:** deterministic seed and transforms; decimal money calculations; null/duplicate/status/foreign-key validation; failure-mode parsing; pure business logic.

**Integration tests:** source seeding; source/warehouse connectivity; staging for one batch; successful publication; rerun idempotency; validation failure leaves published snapshot intact; transaction rollback on injected publish failure; DAG parse/import test against the selected Airflow image.

**End-to-end demonstration:** start from a fresh, empty project-specific environment; seed; run green DAG; record counts and totals; rerun and compare results; trigger bad-data failure; verify previous published snapshot unchanged; trigger operational failure; verify clear task logs. Save example non-sensitive verification output in README or `docs/RUNBOOK.md` only after execution.

## 10. Implementation phases and acceptance gates

### Phase 0 — Freeze the contract and versions
- Choose and record compatible Airflow/Python/provider/PostgreSQL image versions; confirm setup syntax from official docs.
- Specify schema, transformation rules, success/failure modes, ports, credentials, and expected counts.
- **Gate:** README states pinned versions and `docs/DATA_CONTRACT.md` describes every column and rule.

### Phase 1 — Infrastructure
- Create Compose, Dockerfile only if necessary, `.env.example`, init scripts, health checks, and schema SQL.
- Make first-run initialization repeatable and explain behavior with existing volumes.
- **Gate:** Compose validates; PostgreSQL and Airflow start; DAG import check produces no error.

### Phase 2 — Source data and ETL modules
- Implement deterministic seed script and database-access utilities.
- Implement extract, transform, and batch-scoped staging with logging and parameterized SQL.
- **Gate:** source tables contain expected records; staging has expected batch counts; unit tests pass.

### Phase 3 — Validation and atomic publication
- Implement all critical batch checks and atomic warehouse publish.
- Implement consistent dimension/fact relationships and repeat-safe batch handling.
- **Gate:** first publication succeeds; identical rerun has unchanged counts/totals; failed validation preserves prior published tables.

### Phase 4 — Airflow DAG and observability
- Wire existing ETL modules into a small, readable DAG; add task-level logs and run summary.
- Configure manual scheduling, error propagation, and no automatic retries by default.
- **Gate:** DAG displays and executes successfully in Airflow; a deliberate error creates a visible failed task.

### Phase 5 — Fault injection and tests
- Implement named safe failure modes, automated tests, and copy-paste verification queries.
- **Gate:** green run, operational failure, bad-data failure, idempotent rerun, and publication rollback behave as specified.

### Phase 6 — Documentation and handoff
- Finish Windows README, runbook, data contract, failure-scenario instructions, and troubleshooting table.
- Remove unused services/placeholders and verify clean-start process wherever execution is possible.
- **Gate:** a second developer can follow the README without needing undocumented assumptions; report actual tests run and any remaining blockers.

## 11. Roles for a two-person team (optional)

- **Developer A — Infrastructure/data:** Compose, PostgreSQL schemas, seed script, database verification, Airflow startup.
- **Developer B — ETL/testing:** transformation and validation code, DAG wiring, failure switches, automated tests.
- **Together:** decide data contract, review retry safety and atomic publication, run end-to-end checks, own README/runbook.

## 12. Deliverables and final response expected from the coding AI

Produce the full project repository with meaningful code and all documentation in Section 3. In your final response, provide:

1. Actual file tree and key design decisions, including pinned versions.
2. Exact copy-paste PowerShell commands from clean checkout through first successful run.
3. How to trigger `none`, `extract_error`, and `invalid_data` (and `publish_error` if implemented).
4. Exact SQL commands and expected **types** of output to verify successful publication and unchanged state after failure; do not invent measured values.
5. Test commands, actual test results only if tests were run, and any environment limitations.
6. A clear statement that **no AI/agent component was built** and the ETL sandbox is ready for a separate future monitoring project once verified.

**Priority order:** working ETL and safety invariants > reproducible Docker startup > tests > documentation > optional conveniences. Keep the architecture deliberately simple.
