import os
from datetime import date, timedelta

import yaml
from dotenv import load_dotenv
from sqlalchemy import VARCHAR, Integer

from src.utils.general_utils import get_most_recent_date

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


# TODO shift this to some utils?
def parse_extra_dims(extra_dims):
    parsed_extra_dims = {}
    for extra_dim in extra_dims:
        dim = next(iter(extra_dim))
        if extra_dim[dim] == "str":
            parsed_extra_dims[dim] = VARCHAR
        else:
            parsed_extra_dims[dim] = Integer

    return parsed_extra_dims


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
    extra_dims = parse_extra_dims(config.get("extra_dims"))
    if not end_date:
        end_date = date.today() - timedelta(days=1)
    if update:
        last_update = get_most_recent_date(mode, config["blob_prefix"])
        start_date = last_update
        end_date = last_update
    return start_date, end_date, forecast, sel_iso3s, extra_dims
