DATASETS = {
    "era5": {
        "blob_prefix": "era5/monthly/processed/daily_precip_reanalysis_v",
        "start_date": "1981-01-01",
        "end_date": None,
        "coverage": "global",
        "update_schedule": "0 0 6 * *",
        "dev_run": {
            "start_date": "2020-01-01",
            "end_date": "2020-05-01",
            "iso3s": ["ATG", "QAT", "JAM", "SEN", "YEM"],
        },
    },
    "imerg": {
        "blob_prefix": "imerg/v7/late/processed/imerg-daily-late-",
        "start_date": 2000,
        "end_date": None,
        "coverage": "global",
        "update_schedule": "0 20 * * *",
        "dev_run": {
            "start_date": "2020-01-01",
            "end_date": "2020-01-15",
            "iso3s": ["ATG", "QAT", "JAM", "SEN", "YEM"],
        },
    },
    "seas5": {
        "blob_prefix": "seas5/processed/precip_em_i",
        "start_date": "1981-01-01",
        "end_date": None,
        "coverage": "global",
        "update_schedule": "0 0 6 * *",
        "dev_run": {
            "start_date": "2020-01-01",
            "end_date": "2020-02-01",
            "iso3s": ["ATG", "QAT", "JAM", "SEN", "YEM"],
        },
    },
}
