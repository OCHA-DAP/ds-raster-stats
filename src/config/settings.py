import os

import yaml
from dotenv import load_dotenv

load_dotenv()

UPSAMPLED_RESOLUTION = 0.05
LOG_LEVEL = "INFO"
AZURE_DB_PW_DEV = os.getenv("AZURE_DB_PW_DEV")
AZURE_DB_PW_PROD = os.getenv("AZURE_DB_PW_PROD")
DATABASES = {
    "local": "sqlite:///chd-rasterstats-local.db",
    "dev": f"postgresql+psycopg2://chdadmin:{AZURE_DB_PW_DEV}@chd-rasterstats-dev.postgres.database.azure.com/postgres",  # noqa
    "prod": f"postgresql+psycopg2://chdadmin:{AZURE_DB_PW_PROD}@chd-rasterstats-prod.postgres.database.azure.com/postgres",  # noqa
}


def load_pipeline_config(pipeline_name):
    config_path = os.path.join(os.path.dirname(__file__), f"{pipeline_name}.yml")
    with open(config_path, "r") as config_file:
        config = yaml.safe_load(config_file)
    return config


def parse_pipeline_config(config, test):
    if test:
        start_date = config["test"]["start_date"]
        end_date = config["test"]["end_date"]
    else:
        start_date = config["start_date"]
        end_date = config["end_date"]
    forecast = config["forecast"]
    return start_date, end_date, forecast
