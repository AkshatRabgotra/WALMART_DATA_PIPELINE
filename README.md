# Walmart End-to-End Data Pipeline

An end-to-end modern data engineering pipeline built to ingest, validate, model, and transform Walmart retail data. This pipeline leverages **Apache Airflow** for orchestration, **dbt (data build tool)** for dimensional modeling and data quality testing, and containerized deployment with **Docker Compose**.

---

## 🏛️ Architecture Diagram

![Walmart Data Pipeline Architecture](https://github.com/AkshatRabgotra/WALMART_DATA_PIPELINE/blob/main/arch.png?raw=true)

---

## 🚀 Key Highlights & Tech Stack

- **Orchestration:** [Apache Airflow](https://airflow.apache.org/) for DAG-driven pipeline execution, task dependency tracking, and automated retries.
- **Data Transformation & Modeling:** [dbt](https://www.getdbt.com/) for staging, ephemeral CTEs, SCD Type-2 snapshots, fact and dimension builds, and data quality tests.
- **Storage & Warehouse Layer:** Relational databases and data warehouses, including PostgreSQL, Snowflake, Amazon Redshift, and BigQuery-compatible architectures.
- **Quality & Freshness Testing:** dbt source freshness checks and schema tests for uniqueness, not-null constraints, and referential integrity.
- **Containerization:** [Docker](https://www.docker.com/) and Docker Compose for reliable multi-service deployment.
- **Languages:** Python 3, SQL, and Jinja.

---

## 📐 Data Modeling Architecture

The pipeline follows a multi-tier medallion architecture combined with dimensional modeling.

```text
Sources (CSV / OLTP / CDC)
          |
          v
   Source Freshness
          |
          v
    Silver Technical
 (Standardization, Casting,
    Constraints & Tests)
          |
          v
     Silver Business
 (OBT - One Big Table,
   Enrichment & Tests)
          |
          v
     Gold Ephemeral
 (Reusable Business Logic
       & CTE Models)
          |
          +-------------------+
          |                   |
          v                   v
   Gold Dimensions        Gold Facts
  (SCD Type-2 History)    (fact_orders)
```

### 1. Sources

Raw retail datasets containing:

- `customers.csv`
- `employees.csv`
- `stores.csv`
- `products.csv`
- `orders.csv`
- `order_items.csv`

### 2. Silver Technical (`silver_t`)

Cleansed representations of source data with standardized naming, data type casting, basic constraints, and schema tests.

Example models: `customers_t`, `products_t`, `orders_t`, and other technical staging models.

### 3. Silver Business (`silver_b`)

Creates a denormalized and enriched analytical foundation through the `obt_b` (One Big Table) model.

### 4. Gold Tier (`gold`)

- **Ephemeral Models:** Reusable transformation logic for preparing dimension and fact candidates, such as `eph_customers`, `eph_products`, and `eph_orders`.
- **Snapshots and Dimensions:** SCD Type-2 snapshots preserve historical changes in customer, product, store, employee, and order attributes.
- **Fact Tables:** Analytical fact tables, including `fact_orders`, support reporting and business metrics.

---

## 📂 Repository Structure

```text
walmart_data_pipeline/
├── assets/
│   └── architecture_diagram.png
├── airflow_dbt_project/
│   ├── dags/
│   │   └── orchestrate.py
│   ├── walmart_project/
│   │   ├── analyses/
│   │   ├── macros/
│   │   │   └── custom_schema.sql
│   │   ├── models/
│   │   │   ├── source/
│   │   │   │   └── sources.yml
│   │   │   ├── silver_t/
│   │   │   ├── silver_b/
│   │   │   └── gold/
│   │   │       ├── ephemeral/
│   │   │       └── fact/
│   │   ├── snapshots/
│   │   ├── tests/
│   │   ├── dbt_project.yml
│   │   └── profiles.yml
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   ├── requirements.txt
│   └── .env
└── walmart_dataset/
    ├── data/
    └── ddl/
        └── walmart_schema.sql
```

---

## 🔄 Airflow Orchestration Workflow

The primary Airflow DAG, `orchestrate`, coordinates the pipeline through the following sequence:

```text
clean_target
     |
     v
ingest_cdc
     |
     v
source_freshness
     |
     v
silver_technical
     |
     v
silver_technical_tests
     |
     v
silver_business
     |
     v
silver_business_tests
     |
     v
gold_ephemeral
     |
     v
gold_dimensions
     |
     v
gold_facts
```

### Workflow Overview

1. **Clean Target:** Prepares the target environment for pipeline execution.
2. **Ingest CDC:** Loads source data and handles change data capture processing.
3. **Source Freshness:** Validates source data freshness before transformations.
4. **Silver Technical:** Performs initial standardization, casting, and cleaning.
5. **Silver Technical Tests:** Validates technical models using dbt tests.
6. **Silver Business:** Builds enriched, business-ready analytical models.
7. **Silver Business Tests:** Runs quality checks on business models.
8. **Gold Ephemeral:** Prepares reusable transformation logic.
9. **Gold Dimensions:** Builds dimensional models and processes SCD Type-2 snapshots.
10. **Gold Facts:** Builds fact tables for analytical reporting.

---

## 🛠️ Getting Started

### Prerequisites

Install the following tools before running the pipeline:

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [Git](https://git-scm.com/)
- [Python 3.10+](https://www.python.org/) (optional for local CLI development)

### 1. Clone the Repository

Replace `<your-username>` with your GitHub username.

```bash
git clone https://github.com/<your-username>/walmart_data_pipeline.git
cd walmart_data_pipeline
```

### 2. Add the Architecture Diagram

Place your architecture diagram in the `assets/` directory.

```bash
mkdir -p assets
cp /path/to/your/image.png assets/architecture_diagram.png
```

Ensure that `architecture_diagram.png` exists at the specified location so GitHub can render it in the README.

### 3. Configure Environment Variables

Navigate to the Airflow project directory and create the environment file.

```bash
cd airflow_dbt_project
echo "AIRFLOW_UID=50000" > .env
```

Configure any additional database credentials, connection settings, and environment variables required by your Docker Compose and dbt configurations.

**Security note:** Do not commit passwords, API keys, or other secrets to GitHub. Keep sensitive configuration in your local `.env` file and add it to `.gitignore`.

### 4. Start Services Using Docker Compose

From the `airflow_dbt_project/` directory, run:

```bash
docker-compose up --build -d
```

Check the running containers:

```bash
docker-compose ps
```

View service logs if needed:

```bash
docker-compose logs -f
```

### 5. Access the Airflow UI

- **URL:** [http://localhost:8080](http://localhost:8080)
- **Credentials:** Use the credentials configured for your Airflow deployment.

If your configuration uses the default local development credentials, try `airflow` / `airflow`. Change default credentials before exposing the service beyond your local development environment.

Locate the `orchestrate` DAG in the Airflow interface and trigger it manually, or enable its configured schedule.

---

## 🧪 Running dbt Locally (Optional)

To run dbt commands directly, navigate to the dbt project directory:

```bash
cd airflow_dbt_project/walmart_project
```

### Verify the Database Connection

```bash
dbt debug
```

### Check Source Freshness

```bash
dbt source freshness
```

### Run Silver Technical Models and Tests

```bash
dbt run --select silver_t
dbt test --select silver_t
```

### Run Silver Business Models and Tests

```bash
dbt run --select silver_b
dbt test --select silver_b
```

### Build Snapshots and Gold Models

```bash
dbt snapshot
dbt run --select gold
dbt test --select gold
```

> **Note:** Run these commands only after configuring a valid dbt profile and database connection. Ensure your dbt project selectors, model configurations, and snapshot definitions match the actual repository implementation.

---

## 📊 Expected Outcomes

- Automated orchestration of data engineering workflows using Apache Airflow.
- Structured, layered transformations using dbt.
- Historical tracking of selected dimension attributes through SCD Type-2 snapshots.
- Data quality validation through freshness checks and schema tests.
- Reusable SQL transformation logic using dbt ephemeral models.
- Analytics-ready fact and dimension models for downstream reporting.
- Containerized local development and deployment using Docker Compose.

---

## 🔮 Future Enhancements

- Integrate a cloud data warehouse such as Snowflake, Amazon Redshift, or BigQuery.
- Add incremental dbt models for efficient processing of large datasets.
- Introduce automated CI/CD checks for dbt models and Airflow DAGs.
- Add data observability, alerting, and pipeline failure notifications.
- Integrate Power BI or Tableau for business intelligence dashboards.
- Add documentation generation and lineage visualization using `dbt docs`.

---

## 👨‍💻 Technologies Used

`Python` · `SQL` · `Apache Airflow` · `dbt` · `PostgreSQL` · `Docker` · `Docker Compose` · `Jinja`

---

**Built to demonstrate end-to-end data engineering practices, from raw retail data ingestion to tested, analytics-ready dimensional models.**

