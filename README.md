# Comparative Study: Cloud Platforms for Big Data Processing (AWS vs Azure vs GCP) — SDG 9

Evaluate managed Spark services across AWS EMR, Azure Synapse/HDInsight, and GCP Dataproc using a reproducible PySpark workload. Compare performance, cost, elasticity, manageability, and sustainability signals aligned to SDG 9 (Industry, Innovation & Infrastructure).

## Objectives
- Performance: end-to-end runtime, shuffle size, rows processed.
- Cost: estimated from instance pricing and runtime.
- Elasticity: cluster spin-up time, autoscaling behavior (qualitative).
- Manageability: setup steps, observability, developer experience.
- Sustainability: region/energy proxy and use of spot/preemptible capacity (signals only).

## Repository Structure
- docs/: background, methodology, provider overviews, interpretation guide
- scripts/: dataset generator and PySpark job
- notebooks/: data prep, benchmark job, results analysis (placeholders)
- cloud/: provider runbooks (AWS, Azure, GCP)
- docker/: Dockerfile and docker-compose for local Spark
- results/: templates and sample outputs
- .github/workflows/: CI workflow

## Quickstart (Local Baseline)
Prereqs: Docker Desktop (recommended) OR Python 3.11 + Java + Spark.

- Generate synthetic dataset (CSV):
  - Windows PowerShell:
    ```powershell
    python scripts/generate_dataset.py --rows 2000000 --out data
    ```
  - Linux/macOS:
    ```bash
    python3 scripts/generate_dataset.py --rows 2000000 --out data
    ```

- Run the PySpark job via Docker (no local Spark needed):
  - Windows PowerShell:
    ```powershell
    ./scripts/run_local.ps1 -InputDir "$(pwd)\data" -OutputDir "$(pwd)\results"
    ```
  - Linux/macOS:
    ```bash
    ./scripts/run_local.sh --input ./data --out ./results
    ```

Outputs are JSON metrics in `results/` and transformed data in `results/output/`.

## Cloud Runs
Follow step-by-step runbooks in:
- cloud/aws/emr_runbook.md
- cloud/azure/synapse_runbook.md
- cloud/gcp/dataproc_runbook.md

Each runbook includes minimal roles, region guidance, cost guardrails, and cleanup commands.

## Comparison & Analysis
- Record runs in `results/templates/metrics.csv`.
- Use `notebooks/30_results_analysis.ipynb` to compare providers (table + radar chart).
- See `docs/04_interpretation_guide.md` for trade-off guidance.

## SDG 9 Alignment
- Efficient, scalable infrastructure for data-driven innovation.
- Emphasis on cost efficiency, reliability, and sustainability signals.

## License
MIT. See LICENSE.
# cloud-bigdata-comparison-sdg9-.
