# Phase 2 Project Plan — Multilayer ETL Sandbox and Parent–Child Pipelines

**Project:** Standalone E-commerce ETL Sandbox  
**Deliverable:** A working, locally runnable extension of the existing Phase 1 repository (`sandbox_env/`)  
**Status:** Implementation specification; Phase 2 has **not** been implemented or tested by this document  
**Scope:** Data engineering and orchestration only. **No AI agents, LangChain, LangGraph, Ollama, automatic remediation, dashboards, Kafka, Airbyte, or dbt.**

## 0. Starting point and non-negotiable compatibility

Use `PROJECT_OVERVIEW.md` in the repository as the Phase 1 source of truth. Phase 1 already provides:

- Airflow **2.10.5** on Python **3.12**, PostgreSQL **16.4**, Docker Compose and LocalExecutor.
- Three databases in one PostgreSQL container: `airflow_db` (metadata only), `source_db` (synthetic `source.customers` and `source.orders`), and `warehouse_db` (the `staging`, `warehouse`, and `ops` schemas).
- The functioning `sandbox_ecommerce_etl` DAG: `check_source → extract → transform → stage → validate → publish → summarize`.
- Published `warehouse.dim_customers` and `warehouse.fact_orders`, batch-scoped staging, Decimal/NUMERIC money handling, atomic publication, idempotent reruns, fault injection and audit records.
- A deterministic seed of 100 customers and 1,000 orders by default, plus a documented set of normal and failure modes.

**Preserve all Phase 1 behavior, data contracts and tests.** Phase 2 is additive: do not replace the base DAG, rename its public tables, use `airflow_db` for business data, download external datasets, or weaken its validation-before-publication rule. Update Phase 1 code only where necessary to add a durable, batch-specific handoff, and document every change.

The Phase 1 `PROJECT_OVERVIEW.md` reports its own validation status. Do **not** treat that as evidence that the new Phase 2 code passes: rerun all tests after implementation.

## 1. Phase 2 goal

Extend the single-pipeline sandbox into a **multilayer, directed acyclic data platform** in which:

1. An upstream pipeline publishes a versioned, validated output that another pipeline uses as its *only* input for the corresponding run.
2. A parent Airflow DAG triggers a child DAG, passes a run identifier, waits for the child to finish and propagates its failure.
3. Two downstream branches can execute independently after a shared upstream dependency; a final publishing DAG waits for both branches.
4. Every output is traceable to its parent run and input batch; failed or partial child outputs cannot be mistaken for successful published data.
5. Operational failures can be injected at multiple layers without silently publishing invalid analytics.

**Definition of “child pipeline”:** a separate Airflow DAG orchestrated by a parent DAG through an explicit trigger-and-wait contract. It is **not** merely a TaskGroup or a task inside the parent, and it is not a claim that Airflow natively owns nested DAGs.

## 2. Target architecture

```text
source_db: source.customers + source.orders
                     |
                     v
       [sandbox_platform_orchestrator]  (top-level parent)
                     |
          trigger + wait for success
                     v
       [sandbox_ecommerce_etl]          (existing Phase 1 DAG)
                     |
       atomic base snapshot + manifest
                     v
   warehouse_db: layer1.*  (immutable, run-keyed inputs)
                     |
          +----------+--------------------+
          |                               |
          v                               v
 [sandbox_orders_enrichment]      [sandbox_customer_metrics]
   (branch parent DAG)               (independent branch)
          |                               |
   layer2.enriched_orders             layer2.customer_metrics
          |
    trigger + wait
          v
 [sandbox_daily_revenue]                 (child of orders enrichment)
          |
   layer2.daily_revenue
          |                               |
          +---------------+---------------+
                          |
                  trigger + wait
                          v
              [sandbox_analytics_publish]
                          |
        validate cross-branch reconciliation
                          |
           atomic analytics publication
                          v
     analytics.daily_revenue + analytics.customer_summary
                          |
                 ops run manifests / audit
```

### DAG responsibilities and direct dependencies

| DAG ID | Role | Reads | Writes | Triggered by |
| --- | --- | --- | --- | --- |
| `sandbox_platform_orchestrator` | Root orchestration DAG | No business table | Orchestration audit | Manual trigger initially |
| `sandbox_ecommerce_etl` | Existing, backward-compatible base DAG | `source_db.source.*` | Existing `warehouse.*` plus versioned `layer1.*` and base manifest | Root; can also run on its own |
| `sandbox_orders_enrichment` | Downstream pipeline **and child-parent** | Validated `layer1` orders and customers for one run | `layer2.enriched_orders` | Root after base success |
| `sandbox_daily_revenue` | **Child** of order enrichment | Validated `layer2.enriched_orders` | `layer2.daily_revenue` | Order enrichment, only after its own output succeeds |
| `sandbox_customer_metrics` | Independent sibling branch | Validated `layer1` orders and customers for one run | `layer2.customer_metrics` | Root after base success |
| `sandbox_analytics_publish` | Final convergence / data quality / publication | Both complete Layer 2 branches for one run | `analytics.daily_revenue`, `analytics.customer_summary`, final audit | Root after both branches succeed |

The root DAG triggers and waits for the base DAG; **then** launches order enrichment and customer metrics on separate branches. The orders-enrichment DAG produces its own data, triggers and waits for daily revenue, and does not report success before the child succeeds. The root waits for both branch DAGs before triggering analytics publication. Do not connect the root directly to daily revenue in a way that bypasses the orders-enrichment parent–child relationship.

Use `TriggerDagRunOperator` (or the equivalent supported, verified Airflow 2.10.5 API) with `wait_for_completion=True`, explicit allowed/failed states, a bounded wait and a deterministic target run ID. Make the root's branch tasks independent so Airflow may schedule them in parallel. Avoid `SubDagOperator` and avoid an `ExternalTaskSensor` design that assumes manually triggered DAGs happen to have identical logical dates. Verify exact APIs against the pinned installed Airflow version before implementation.

## 3. Layer-by-layer data contract

**Layer 0 — source, unchanged:** `source_db.source.customers`, `source_db.source.orders`. Continue to use deterministic synthetic data; no new external dataset.

**Layer 1 — validated base snapshot:** keep Phase 1 `warehouse.dim_customers` and `warehouse.fact_orders` as its existing current snapshot. In the **same successful publication transaction**, additionally persist immutable, run-keyed copies in `layer1.customers_snapshot` and `layer1.orders_snapshot`, keyed by `(platform_run_id, customer_id)` / `(platform_run_id, order_id)`. Write the matching ready manifest in that transaction. This ensures later pipelines never read whichever Phase 1 snapshot happens to be current after another run. Existing standalone Phase 1 runs also get a unique platform ID derived from their own DAG run ID when no orchestration ID is provided.

**Layer 2A — enrichment:** `layer2.enriched_orders`, keyed by `(platform_run_id, order_id)`. Read *only* matching Layer 1 rows. Preserve order ID, customer ID, UTC order date, status and exact numeric order total; join to the matching customer snapshot to add customer country. Define missing customer lookup as a critical quality failure, not a silently dropped row. Publish this layer only after row-count, uniqueness and order-total checks pass.

**Layer 2B — daily revenue (child):** `layer2.daily_revenue`, keyed by `(platform_run_id, order_date)`. Aggregate enriched orders using **the same Phase 1 revenue definition: exclude `cancelled` orders**. Define the date as the UTC date of `order_timestamp`; store numeric `revenue_total` and integer `order_count`. Document whether order_count includes only non-cancelled orders (recommended: yes), and keep the rule consistent everywhere.

**Layer 2C — customer metrics (sibling):** `layer2.customer_metrics`, keyed by `(platform_run_id, customer_id)`. Aggregate matching Layer 1 customers and orders into each customer's non-cancelled order count and revenue total; retain customers with zero eligible orders using a left join. Preserve `NUMERIC` calculations and deterministic ordering in verification outputs.

**Layer 3 — analytics publication:** `analytics.daily_revenue` and `analytics.customer_summary` are consumer-facing current snapshots. The final DAG validates that both matching Layer 2 outputs are ready and that Layer 2 daily revenue sums equal the customer-metrics revenue sum and match the non-cancelled Layer 1 fact total. Then replace **both** analytics tables and write the final success manifest **inside one PostgreSQL transaction**. A failed final run leaves the previous successful analytics snapshot untouched.

### Explicit database additions (all in `warehouse_db`)

- Schemas `layer1`, `layer2`, and `analytics`.
- Versioned tables `layer1.customers_snapshot`, `layer1.orders_snapshot`, `layer2.enriched_orders`, `layer2.daily_revenue`, `layer2.customer_metrics`.
- Final tables `analytics.daily_revenue`, `analytics.customer_summary`.
- `ops.pipeline_runs` (or an additive, clearly documented extension of `ops.etl_runs`) with `platform_run_id`, `dag_id`, `airflow_run_id`, `parent_dag_id`, `parent_run_id`, `input_platform_run_id`, `status`, start/end timestamps, input/output counts, error summary and publication status. Use a documented uniqueness constraint, such as `(platform_run_id, dag_id)`, and a clearly defined status lifecycle.
- `ops.platform_manifests` keyed by `platform_run_id`, linking the base snapshot and final analytics publication state. Every downstream read must check a successful upstream manifest **and** use the same platform ID in all queries.

Choose precise data types and foreign keys based on the existing schema; retain decimal-safe calculations. Use migration scripts that are idempotent and do not destroy Phase 1 data. Do not use `CREATE DATABASE` scripts on existing initialized volumes as a substitute for schema migrations.

## 4. Parent–child execution contract

### Run identity

- Root determines a stable `platform_run_id` from its run identity and passes it explicitly through DAG-run `conf` to every triggered DAG.
- Each DAG stores its **own** Airflow run ID and its received `platform_run_id`. The base DAG derives a compatible platform ID when independently triggered.
- Parent passes small JSON metadata only: platform ID, expected upstream DAG ID and run ID, and optionally an approved test failure mode. No credentials, full datasets or large XCom payloads.
- The child's input contract includes a successful **manifest and exact run-keyed rows**. Never select `MAX(batch_id)`, “latest successful run” or the current unversioned snapshot as a shortcut.

### Triggering, waiting, failure propagation

- A parent first writes its pending/audit record, triggers its named child with a stable child DAG-run ID, and waits until the child reaches success or failure. Reconcile an existing child run on retries; do **not** create a second child run or reset a running/successful child automatically.
- `orders_enrichment` publishes its enriched output before triggering `daily_revenue`; if the child fails, the parent DAG fails, and the root's order branch fails.
- If either branch fails, the root must **not** trigger `sandbox_analytics_publish`.
- All child DAGs have `schedule=None` initially. The root is manual initially. Existing standalone base DAG behavior should continue unchanged.
- Set `max_active_runs=1` on the root for the initial local sandbox. Also prevent accidental concurrent writes to the two unversioned final analytics tables with an advisory lock or equivalent transaction lock. Reject conflicting publication attempts safely.
- Implement bounded waits/timeouts; timeouts mark orchestration unsuccessful and produce actionable logs. No indefinitely waiting sensors.

### Retry and recovery policy for the sandbox

- Keep Phase 1's default zero automatic retries. New DAGs also default to zero automatic retries so injected failures are observable.
- Repeat a pipeline for an existing platform ID only via an explicit, documented manual recovery procedure. Tasks must replace or upsert **only that run's** candidate rows; never delete another run's data.
- A final publication retry must be transaction-safe and idempotent. Don't assume that a generic retry is safe because a SQL table has a primary key.
- Preserve historical versioned outputs needed for reproducibility; retention/purge is a future deliberate operation, not an implicit task cleanup.

## 5. Failure modes and observability

Retain all Phase 1 `failure_mode` values: `none`, `extract_error`, `transform_error`, `invalid_data`, `publish_error`. Add **namespaced, opt-in** Phase 2 failure injection, passed only to the targeted DAG; a failure parameter for one DAG must not accidentally break its siblings.

Minimum new scenarios:

| Test case | Injection point | Expected behavior |
| --- | --- | --- |
| Normal multilayer run | None | All DAGs succeed; final analytics published once |
| Base pipeline fails | Existing base failure mode | No downstream branch starts; prior analytics unchanged |
| Enrichment fails | Before Layer 2 enrichment publication | Child daily revenue does not start; final DAG blocked |
| Child daily-revenue fails | During its validation or transaction | Orders-enrichment parent and root fail; customer branch may finish; final blocked |
| Customer-metrics fails | During its validation | Other branch may finish; final blocked |
| Revenue reconciliation fails | Final validation | No analytics table replaced; previous snapshot preserved |
| Publication fails mid-transaction | Final publication | Both final tables and success manifest roll back together |
| Child trigger attempted twice | Parent retried | No duplicate child DAG runs or duplicate rows |
| Different runs overlap / manual base rerun | Separate platform IDs | No cross-batch mixing; only deliberate successful final publication updates current analytics |

Each pipeline must log its DAG/run IDs, platform ID, input manifest, row counts, start/end, status, and concise failure reason. A queryable `ops` audit record is mandatory. Airflow task logs and UI are sufficient for this phase; no dashboard or notification service is needed.

## 6. Implementation tasks in order

### P2.0 — Baseline protection and specification

- [ ] Record the current Git commit or copy of the Phase 1 source before changes.
- [ ] Rerun existing compile, unit, DAG import, Docker and end-to-end checks; record actual results rather than assuming the overview's validation still applies.
- [ ] Inspect the exact Phase 1 database DDL, `publish.py`, DAG interfaces, Docker mounts and run-ID handling.
- [ ] Document exact schemas, money/revenue definition, time zone, state transitions, output grains, naming and child failure semantics in `docs/PIPELINE_CONTRACTS.md`.

**Exit:** Phase 1 baseline recorded and compatibility constraints agreed.

### P2.1 — Add batch identity and durable handoff

- [ ] Create additive SQL migrations for `layer1`, `layer2`, `analytics` and `ops` manifests.
- [ ] Extend the base publish transaction to write versioned Layer 1 snapshots and a successful manifest atomically, while keeping existing Phase 1 public tables and audit behavior.
- [ ] Support orchestrated `platform_run_id` from `dag_run.conf` and standalone fallback.
- [ ] Test that a subsequent successful base run does not change an older run's Layer 1 input rows.

**Exit:** A downstream process can query one immutable validated base batch with no “latest” lookup.

### P2.2 — Implement independent transformation DAGs

- [ ] Build reusable Python/SQL modules for enrichment and customer metrics, with strict input-manifest checks.
- [ ] Implement `sandbox_orders_enrichment` and `sandbox_customer_metrics` as separate DAGs.
- [ ] Add batch-local staging/validation/transactional publish and per-DAG audit records.
- [ ] Test each DAG individually with a valid platform ID; reject absent, wrong or failed upstream manifests.

**Exit:** Two independently runnable Layer 2 pipelines consume only the specified Layer 1 batch.

### P2.3 — Implement a real child pipeline

- [ ] Build `sandbox_daily_revenue` from the validated enriched-orders output.
- [ ] Add a trigger-and-wait step to `sandbox_orders_enrichment`, **after** its output is ready.
- [ ] Propagate `platform_run_id`, parent IDs and child failures; enforce bounded waiting and stable child run IDs.
- [ ] Test a normal child run, failed child, duplicate trigger and parent restart/retry behavior.

**Exit:** The parent cannot succeed unless its distinct child DAG succeeds.

### P2.4 — Implement root fan-out and fan-in

- [ ] Create `sandbox_platform_orchestrator`.
- [ ] Trigger/wait for existing base DAG; after its success launch order-enrichment parent and customer-metrics sibling independently.
- [ ] Wait for **both** branches to succeed, then trigger/wait for `sandbox_analytics_publish`.
- [ ] Implement a deterministic initial execution model: manual root, no independent schedules on new child DAGs, bounded waiting, and root `max_active_runs=1`.

**Exit:** Airflow Graph view visibly shows the dependency split/join and the linked DAG runs demonstrate the nested child relationship.

### P2.5 — Final quality gate and atomic analytics publication

- [ ] Implement `sandbox_analytics_publish` with exact-platform input checks, counts, uniqueness, foreign keys, UTC date grouping and cross-branch revenue reconciliation.
- [ ] Replace both final analytics snapshots and commit success metadata in one transaction; rollback everything on injected failure.
- [ ] Ensure the previous final snapshots remain queryable on **any** failed run.
- [ ] Add SQL verification for base vs enrichment vs daily/customer totals, run lineage and no mixed platform IDs.

**Exit:** A normal run produces consistent consumer-facing analytics; an invalid run publishes nothing.

### P2.6 — Test, document and hand over

- [ ] Add Python unit tests, Docker-backed integration tests and DAG import tests for all six DAGs.
- [ ] Test the complete failure matrix in Section 5, including repeated runs, parent–child failure propagation and transaction rollback.
- [ ] Extend README and RUNBOOK with Windows PowerShell commands, seed/run/inspect/fail/recover workflow, DAG run ID discovery and cleanup rules.
- [ ] Update `PROJECT_OVERVIEW.md` **only after** verifying the implemented system, including accurate results and explicit remaining limitations.

**Exit:** A second developer can reproduce the complete multilayer demo on a clean Docker environment without guesswork.

## 7. Proposed repository additions

Retain every Phase 1 file. Add or adapt these files, renaming only if an existing repository convention calls for it:

```text
sandbox_env/
├── PROJECT_OVERVIEW.md              # update after implementation passes
├── PHASE_2_MULTILAYER_PIPELINE_PLAN.md
├── dags/
│   ├── sandbox_ecommerce_etl.py      # preserve; add atomic Layer 1 handoff
│   ├── sandbox_platform_orchestrator.py
│   ├── sandbox_orders_enrichment.py
│   ├── sandbox_daily_revenue.py
│   ├── sandbox_customer_metrics.py
│   └── sandbox_analytics_publish.py
├── etl/
│   ├── ...                           # preserve existing modules
│   ├── run_context.py
│   ├── manifests.py
│   ├── enrichment.py
│   ├── daily_revenue.py
│   ├── customer_metrics.py
│   └── analytics_publish.py
├── database/
│   ├── migrations/
│   │   └── 02_multilayer.sql
│   └── verify_multilayer.sql
├── tests/
│   ├── ...                           # preserve existing tests
│   ├── test_run_context.py
│   ├── test_layer_contracts.py
│   ├── test_child_orchestration.py
│   ├── test_revenue_reconciliation.py
│   └── test_multilayer_integration.py
└── docs/
    ├── PIPELINE_CONTRACTS.md
    ├── MULTILAYER_ARCHITECTURE.md
    └── PHASE_2_RUNBOOK.md
```

All DAGs should call reusable code in `etl/` instead of duplicating SQL and orchestration logic. Do not create large cross-DAG XCom payloads. Use Docker-mounted files only where a pipeline actually needs them; run-keyed PostgreSQL tables and small manifests are the standard cross-DAG handoff.

## 8. Testing and acceptance criteria

All of the following are required for Phase 2 completion:

- [ ] Existing Phase 1 tests and `sandbox_ecommerce_etl` still succeed unchanged from a user's perspective.
- [ ] All six DAGs parse, register and run on the pinned Airflow 2.10.5 environment.
- [ ] One root trigger runs the full dependency graph in the intended order.
- [ ] An enrichment DAG's own output is the sole input to its daily-revenue child.
- [ ] The customer-metrics branch runs independently of the daily-revenue child after the shared base succeeds.
- [ ] Root/final publishing waits for both branches, and failures stop downstream work.
- [ ] All inter-DAG reads are keyed to the exact `platform_run_id`, with verified successful input manifests.
- [ ] Full rerun yields identical counts and revenue, no duplicates and no accidental second child DAG run.
- [ ] Non-cancelled revenue reconciles across base, enriched, daily, customer and final outputs.
- [ ] Every critical data-quality or injected failure preserves the last successful published analytics snapshot.
- [ ] Final two analytics tables and final success audit commit/rollback together.
- [ ] Parent run, child run, input/output lineage and status are reconstructable from Airflow and `ops` tables.
- [ ] Validation commands and **actual measured** test results are recorded in the README or overview; do not invent passing test counts.

### Minimum demo script

1. Start the existing Compose stack; seed the unchanged synthetic dataset.
2. Trigger `sandbox_platform_orchestrator` with no fault injection and record its platform ID.
3. In Airflow, show base → two independent branches, with the orders branch triggering its own daily-revenue child → final publish.
4. Query all layers for that platform ID and show count/revenue reconciliations and the final manifest.
5. Trigger a **new** root run injecting a daily-revenue child failure. Show child, parent and root failure; show final DAG did not run and previous analytics remained unchanged.
6. Trigger another new root run injecting final publication failure. Show both final tables and success manifest were rolled back together.
7. Trigger a clean run again and verify successful recovery via a new complete, consistent publication. This is **manual test orchestration**, not automated remediation.

## 9. Two-person work split

**Developer A — Database and transformation:** additive migrations, Layer 1 snapshot handoff, enrichment/customer/daily transformations, SQL validation, atomic final publication, database tests.

**Developer B — Airflow and operational behavior:** root/parent/child DAGs, trigger/conf/run-ID contract, audit logging, injected failures, DAG import and end-to-end orchestration tests, runbook.

**Joint responsibilities:** agree on contracts before coding; review publication transactions and retry semantics; execute both success and failure demonstrations; update the overview only with observed results.

## 10. Key risks and mitigations

| Risk | Mitigation |
| --- | --- |
| A new base run overwrites an older current warehouse snapshot while children read it | Read only immutable Layer 1 rows keyed by platform ID, captured atomically in the base publish transaction |
| A branch reads another run's data | Require platform ID in every query and enforce a successful upstream manifest |
| Parent appears successful while a child failed | Trigger and **wait** for the child; propagate failure and apply bounded timeout |
| Duplicate child DAGs on retry | Stable child DAG-run IDs plus existence/reconciliation checks; no automatic reset |
| Final table A changes but table B fails | Single transaction for both tables and final audit |
| Daily totals and customer totals use inconsistent revenue definitions | Explicitly exclude cancelled orders in every aggregate and assert reconciliation |
| Final publication collides with a manual root/standalone run | Run-keyed intermediate tables plus a transaction-level publication lock |
| DAG depends on exact matching logical dates across manual runs | Explicitly propagate platform/run IDs through DAG-run conf and manifests |
| Phase 2 breaks Phase 1 | Baseline capture, additive migrations and full regression tests |
| Scope becomes an unrelated platform rewrite | No new ingestion stack, external datasets, streaming, AI, dashboards or cloud |

## 11. Implementation request to give a coding AI

> Extend the **existing** Phase 1 repository according to `PROJECT_OVERVIEW.md` and this Phase 2 plan. Inspect its real code before editing; do not assume the overview is executable code. Implement the six-DAG architecture, durable run-keyed handoffs, genuine parent–child trigger/wait behavior, parallel branches, a transactional final analytics quality gate, audit metadata and failure injection. Keep Phase 1 functional and honor the pinned dependency versions. Produce complete code for every changed or new file, additive migrations, meaningful tests, an exact Windows/PowerShell runbook, and actual test results. Do not claim success without running the relevant checks. If any requirement conflicts with existing code, document the conflict and choose the smallest safe compatible change. Do not add Agentic AI components.

---

**Phase 2 is complete only when the independent downstream branches and nested child DAG can be demonstrated end to end, downstream data is batch-specific and validated, and failed runs cannot corrupt the previously published analytics snapshot.**
