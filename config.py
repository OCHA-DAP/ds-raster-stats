import os

from dotenv import load_dotenv

load_dotenv()

MAX_ADM = 2
LOG_LEVEL = "DEBUG"
AZURE_DB_PW = os.getenv("AZURE_DB_PW")

DATASETS = {
    "era5": {
        "blob_prefix": "era5/monthly/processed/daily_precip_reanalysis_v",
        "start_date": "1981-01-01",
        "end_date": "2024-07-30",  # TODO
        "coverage": "global",
        "forecast": False,
        "update_schedule": "0 0 6 * *",
        "dev_run": {
            "start_date": "1981-01-01",
            "end_date": "2024-01-01",  # TODO
            "iso3s": ["BRA"],
        },
    },
    "imerg": {
        "blob_prefix": "imerg/v7/late/processed/imerg-daily-late-",
        "start_date": "2000-01-01",  # TODO
        "end_date": "2024-07-30",  # TODO
        "coverage": "global",
        "forecast": False,
        "update_schedule": "0 20 * * *",
        "dev_run": {
            "start_date": "2020-01-01",
            "end_date": "2020-01-01",
            "iso3s": ["PHL"],
        },
    },
    "seas5": {
        "blob_prefix": "seas5/processed/precip_em_i",
        "start_date": "1981-01-01",
        "end_date": "2024-07-30",  # TODO
        "coverage": "global",
        "forecast": True,
        "update_schedule": "0 0 6 * *",
        "dev_run": {
            "start_date": "1981-01-01",
            "end_date": "2024-07-30",
            "iso3s": ["YEM"],
        },
    },
}

DATABASES = {
    "local": {
        "engine_url": "sqlite:///chd-rasterstats-local.db",
    },
    "dev": {
        "engine_url": f"postgresql+psycopg2://chdadmin:{AZURE_DB_PW}@chd-rasterstats-dev.postgres.database.azure.com/postgres",  # noqa
    },
}
