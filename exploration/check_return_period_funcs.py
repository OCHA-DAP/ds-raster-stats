# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: ds-raster-stats
#     language: python
#     name: ds-raster-stats
# ---

# %%
# %matplotlib inline
# %load_ext autoreload
# %autoreload 2

# %%
from src.utils import cloud_utils
from src.utils import return_periods as rp
import pandas as pd
import numpy as np
from io import BytesIO
import pyarrow
from azure.storage.blob import BlobServiceClient
import re
from scipy.stats import beta
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

def to_snake_case(name):
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
    
sample_admin = "Awdal" # just used for plotting checking
# %% [markdown]
# Load data

# %%
pc = cloud_utils.get_container_client(mode="dev", container_name="projects")
blob_name = "ds-floodscan-ingest/df_aer_sfed_som_adm1_zstats.parquet"
blob_client = pc.get_blob_client(blob_name)
blob_data = blob_client.download_blob().readall()
df = pd.read_parquet(BytesIO(blob_data))
df.columns = [to_snake_case(col) for col in df.columns]

# %% [markdown]
# ## Pre Process Data
#
# So just take yearly max values

# %%
df["year"] = pd.to_datetime(df["date"]).dt.year
df_max = df.groupby(["adm1_en", "adm1_pcode", "year"]).max().reset_index()

# %% [markdown]
# ## Calculate LP3 Params
#
# Show the implementation by first just doing each method one by one on a single sample admin

# %%
rp.lp3_params(df_max[df_max["adm1_en"]==sample_admin].value,est_method = "usgs")


# %%
rp.lp3_params(df_max[df_max["adm1_en"]==sample_admin].value,est_method = "lmoments")

# %%
rp.lp3_params(df_max[df_max["adm1_en"]==sample_admin].value,est_method = "scipy")

# %% [markdown]
# We can also run them all at once with this wrapper

# %%
rp.lp3_params_all2(df_max[df_max["adm1_en"]==sample_admin].value)

# %% [markdown]
# Use wrapper function with `groupby` to run for each admin

# %%
df_params = df_max.groupby("adm1_en")["value"].apply(rp.lp3_params_all).reset_index().rename(columns={"level_1": "method"})

# %% [markdown]
# ## Calculate RPs
#
# Now we can loop through each admin and calculate RPs associated with each value

# %%
unique_adm1s = df_params_long["adm1_en"].unique()
df_list = []

for adm in unique_adm1s:
    df_params_adm_filt = df_params[df_params["adm1_en"] == adm]
    df_adm_filt = df[df["adm1_en"] == adm]
    
    est_methods = ["lmoments", "scipy", "usgs"]

    for method in est_methods:
        df_adm_filt[f"rp_{method}"] = rp.lp3_rp(
            x=df_adm_filt["value"],
            params=df_params_adm_filt.loc[df_params_adm_filt["method"] == method, "value"].iloc[0],
            est_method=method
        )
    
    df_list.append(df_adm_filt)

# Concatenate all the dataframes in the list
df_combined = pd.concat(df_list, ignore_index=True)


# %% [markdown]
# Plot all RPs from each method - can see that they are generally similar, but plot is not very useful.

# %%
df_combined_sample = df_combined[df_combined["adm1_en"]==sample_admin]

# Plot RP values against 'value' for different estimation methods
plt.figure(figsize=(10, 6))

# Plot for lmoments
plt.scatter(df_combined_sample["value"], df_combined_sample["rp_lmoments"], color='blue', label='RP Lmoments', alpha=0.5)

# Plot for scipy
plt.scatter(df_combined_sample["value"], df_combined_sample["rp_scipy"], color='green', label='RP Scipy', alpha=0.5)

# Plot for usgs
plt.scatter(df_combined_sample["value"], df_combined_sample["rp_usgs"], color='red', label='RP USGS', alpha=0.5)

plt.xlabel('Value')
plt.ylabel('Return Period (RP)')
plt.title('Return Periods (RP) vs Value')
plt.legend()
plt.xscale('log')
plt.yscale('log')
plt.grid(True, which="both", ls="--")
plt.show()


# %% [markdown]
# ## Plot RVs for Specified RPs
#
# for a better comparison we can calculate RVs for specified RPs `1-1000` for the sample admin

# %%

return_periods = np.arange(1, 10000)

params_sample_lmom = df_params.loc[(df_params["method"] == "lmoments") & (df_params["adm1_en"] == sample_admin), "value"].iloc[0]
params_sample_usgs = df_params.loc[(df_params["method"] == "usgs") & (df_params["adm1_en"] == sample_admin), "value"].iloc[0]
params_sample_scipy = df_params.loc[(df_params["method"] == "scipy") & (df_params["adm1_en"] == sample_admin), "value"].iloc[0]
# Calculate return values for each return period using lmoments method

# import inspect
# inspect.getsourcelines(rp.lp3_rv)
return_values_lmom = rp.lp3_rv(rp=return_periods, params=params_sample_lmom, est_method="lmoments")
return_values_usgs = rp.lp3_rv(rp=return_periods, params=params_sample_usgs, est_method="usgs")
return_values_scipy = rp.lp3_rv(rp=return_periods, params=params_sample_scipy, est_method="scipy")

# Plot the return periods against the return values
plt.figure(figsize=(10, 6))
plt.plot(return_periods, return_values_lmom * 100, label='LMoments', color='blue')
plt.plot(return_periods, return_values_usgs * 100, label='USGS', color='red')
plt.plot(return_periods, return_values_scipy * 100, label='SCIPY', color='green')
plt.xlabel('Return Period')
plt.ylabel('Return Value (%)')
plt.title('Return Values vs Return Periods')
plt.xscale('log')
plt.yscale('log')
plt.legend()
plt.grid(True, which="both", ls="--")
# Format y-axis to percentage
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0f}%'.format(y)))
plt.show()


