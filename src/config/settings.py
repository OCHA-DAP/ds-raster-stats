import os
from datetime import date, timedelta

import yaml
from dotenv import load_dotenv

from src.utils.general_utils import get_most_recent_date

load_dotenv()


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


def parse_pipeline_config(dataset, test, update, mode):
    config = load_pipeline_config(dataset)
    if test:
        start_date = config["test"]["start_date"]
        end_date = config["test"]["end_date"]
        sel_iso3s = config["test"]["iso3s"]
    else:
        start_date = config["start_date"]
        end_date = config["end_date"]
        sel_iso3s = None
    forecast = config["forecast"]
    if not end_date:
        end_date = date.today() - timedelta(days=1)
    if update:
        last_update = get_most_recent_date(mode, config["blob_prefix"], dataset)
        start_date = last_update
        end_date = last_update
    return start_date, end_date, forecast, sel_iso3s
