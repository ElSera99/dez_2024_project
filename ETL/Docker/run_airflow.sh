# Create needed folders
mkdir -p ./dags ./logs ./plugins ./config

# Create environmet variables for usage
echo -e "AIRFLOW_UID=$(id -u)" > .env

# First configuration for Airflow
docker compose up airflow-init

# Run airflow
docker compose up -d