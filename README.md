# NYC 311 Complaints Data Pipeline

An end-to-end data engineering pipeline that ingests NYC 311 service requests and US Census median income data, transforms it with Spark on Google Cloud Dataproc, and serves analytics-ready tables in BigQuery — all orchestrated by Apache Airflow.

## Architecture

```
NYC 311 API  ──┐
               ├──► GCS (bronze) ──► Dataproc Spark ──► GCS (silver) ──► BigQuery (dbt) ──► Mart Tables
Census API  ───┘
```

| Layer | Tool | Output |
|---|---|---|
| Ingestion | Python (Docker) | Raw NDJSON + Parquet in GCS `bronze/` |
| Transform | PySpark on Dataproc | Clean Parquet in GCS `silver/` |
| Modeling | dbt (Docker) | External tables → staging → mart in BigQuery |
| Orchestration | Apache Airflow | 3 daily DAGs chained in sequence |

### DAG Schedule

| DAG | Schedule (UTC) | Purpose |
|---|---|---|
| `ingestion_dag` | 08:30 | Fetch new complaints + income data → GCS bronze |
| `spark_transform_dag` | 12:30 | Transform bronze → silver, create/delete Dataproc cluster |
| `dbt_dag` | 14:30 | Build staging + mart models in BigQuery |

---

## Prerequisites

Install these before starting:

- [Docker + Docker Compose](https://docs.docker.com/get-docker/)
- [Terraform](https://developer.hashicorp.com/terraform/install)
- [Google Cloud CLI (`gcloud`)](https://cloud.google.com/sdk/docs/install)
- A [GCP account](https://cloud.google.com/) with billing enabled
- A [Socrata app token](https://data.cityofnewyork.us/profile/edit/developer_settings) (free, required for higher API rate limits)

---

## Setup

### 1. Clone the repo

```bash
git clone <your-repo-url>
cd project
```

### 2. GCP — Bootstrap (manual, one-time)

Terraform needs a service account to provision resources. Create one manually first.

```bash
# Authenticate and set your project
gcloud auth login
gcloud config set project YOUR_PROJECT_ID

# Enable the two APIs Terraform itself needs
gcloud services enable cloudresourcemanager.googleapis.com iam.googleapis.com

# Create a Terraform runner service account
gcloud iam service-accounts create terraform-runner --display-name="Terraform Runner"

SA="terraform-runner@YOUR_PROJECT_ID.iam.gserviceaccount.com"

# Grant it permissions to provision all pipeline resources
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:$SA" --role="roles/editor"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:$SA" --role="roles/iam.securityAdmin"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:$SA" --role="roles/serviceusage.serviceUsageAdmin"

# Download the key — this SA is also used by the pipeline (ingestion, Dataproc, dbt)
# Place it in the credentials/ folder at the project root
mkdir -p credentials
gcloud iam service-accounts keys create credentials/g_creds.json --iam-account=$SA
```

> The same service account is used for both Terraform provisioning and running the pipeline.
> Terraform (`iam.tf`) grants it `storage.admin`, `bigquery.admin`, and `dataproc.editor`.

### 3. Terraform — Provision GCP Infrastructure

```bash
cd terraform/
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` with your values:

```hcl
project_id            = "your-gcp-project-id"
region                = "us-central1"
location              = "US"
bucket_name           = "your-gcs-bucket-name"
credentials           = "/path/to/terraform-key.json"
service_account_email = "terraform-runner@your-project-id.iam.gserviceaccount.com"
services = [
  "iam.googleapis.com",
  "cloudresourcemanager.googleapis.com",
  "storage.googleapis.com",
  "bigquery.googleapis.com",
  "bigquerystorage.googleapis.com",
  "dataproc.googleapis.com",
  "compute.googleapis.com",
]
```

Apply:

```bash
terraform init
terraform plan    # review what will be created
terraform apply
```

This enables all required GCP APIs, creates the GCS bucket, and grants the service account the permissions it needs to run the pipeline.

### 4. Networking — Enable Private Google Access

Dataproc cluster VMs need this to reach GCS and BigQuery without a public IP.

```bash
gcloud compute networks subnets update default \
  --region=us-central1 \
  --enable-private-ip-google-access
```

### 5. Build Docker Images

```bash
# Ingestion image
cd ingestion/
docker build -t ingestion .
cd ..

# dbt image
cd dbt/
docker build -t dbt .
cd ..
```

### 6. Configure Airflow

```bash
cd airflow/
cp .env.example .env
```

Edit `.env` with your values. Generate the required keys:

```bash
# Generate FERNET_KEY
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Generate AIRFLOW__API__SECRET_KEY
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

Key variables to set in `.env`:

| Variable | Description |
|---|---|
| `FERNET_KEY` | Generated above |
| `AIRFLOW__API__SECRET_KEY` | Generated above |
| `CREDENTIALS_HOST_DIR` | Absolute path to your `credentials/` folder |
| `GCS_BUCKET_NAME` | Your GCS bucket name |
| `SOCRATA_APP_TOKEN` | Your Socrata developer token |
| `DATAPROC_PROJECT_ID` | Your GCP project ID |
| `DATAPROC_REGION` | `us-central1` (or your chosen region) |

### 7. Start Airflow

```bash
cd airflow/

# First-time initialisation
docker compose up airflow-init

# Start all services
docker compose up -d
```

Airflow UI will be available at `http://localhost:8080` (default credentials: `airflow` / `airflow`).

---

## Running the Pipeline

The three DAGs run automatically on their daily schedule. To trigger a full run manually:

1. Open `http://localhost:8080`
2. Enable and trigger `ingestion_dag` — waits for it to complete
3. Enable and trigger `spark_transform_dag` — waits for it to complete
4. Enable and trigger `dbt_dag`

> **First run note:** dbt will create BigQuery datasets automatically on first run. The `stg_complaints` model only loads records with `created_date >= 2025-01-01`.

> **Spark script:** `spark_transform_dag` automatically uploads `transforms.py` to GCS before each run — no manual upload needed.

---

## Project Structure

```
project/
├── airflow/                    # Airflow Docker Compose setup
│   ├── dags/
│   │   ├── ingestion_dag.py
│   │   ├── spark_transform_dag.py
│   │   └── dbt_dag.py
│   ├── docker-compose.yaml
│   └── .env.example
├── ingestion/                  # Ingestion Docker image
│   ├── main.py                 # Fetches from Socrata + Census APIs
│   └── Dockerfile
├── spark-tranformation/        # PySpark transformation scripts
│   └── transforms.py           # Transformation logic + entrypoint (submitted directly to Dataproc)
├── dbt/                        # dbt project
│   ├── nyc_complaints/
│   │   ├── models/
│   │   │   ├── staging/        # stg_complaints, stg_median_income
│   │   │   └── mart/           # fct_agency_efficiency, fct_complaint_hotspots, fct_income_resolution
│   │   └── profiles.yml
│   └── Dockerfile
├── terraform/                  # Infrastructure as Code
│   ├── main.tf
│   ├── apis.tf
│   ├── iam.tf
│   ├── variables.tf
│   └── terraform.tfvars.example
└── credentials/                # Gitignored — place g_creds.json here
```

---

## Data Sources

- **NYC 311 Service Requests** — [Socrata Open Data API](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9)
- **US Census Median Household Income by ZIP** — [ACS 5-Year Estimates (2022), Table S1901](https://api.census.gov/data/2022/acs/acs5/subject)
