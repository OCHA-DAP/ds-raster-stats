import pandas as pd
import numpy as np
from scipy.stats import skew, norm, pearson3
from lmoments3 import distr, lmom_ratios


def lp3_params(x, est_method="lmoments"):
    x = np.asarray(x)
    x[x == 0] = np.min(x[x != 0])
    x_sorted = np.sort(x)
    x_log = np.log10(x_sorted)

    if est_method == "lmoments":
        lmoments = lmom_ratios(x_log, nmom=4)
        params = distr.pe3.lmom_fit(x_log, lmoments)  # , lmom_ratios=lmom/)
    elif est_method == "scipy":
        params = pearson3.fit(x_log, method="MM")
    elif est_method == "usgs":
        params = {
            "mu": np.mean(x_log),
            "sd": np.std(x_log, ddof=1),
            "g": skew(x_log),
        }
    else:
        raise ValueError(
            "Invalid est_method specified. Choose 'usgs','lmoments' or 'scipy'."
        )
    return params


def lp3_params_all(group):
    methods = ["usgs", "lmoments", "scipy"]
    params = {method: lp3_params(group["value"], method) for method in methods}
    return pd.Series(params)


def lp3_rp(x, params, est_method="lmoments"):
    x = np.asarray(x)
    x_sorted = np.sort(x)
    x_log = np.log(x_sorted)

    if est_method == "scipy":
        # Calculate the CDF for the log-transformed data
        p_lte = pearson3.cdf(x_log, *params)

    elif est_method == "lmoments":
        p_lte = distr.pe3.cdf(x_log, **params)

    elif est_method == "usgs":
        g = params["g"]
        mu = params["mu"]
        stdev = params["sd"]
        # Step 1: From quantiles_rp to y_fit_rp
        y_fit_rp = x_log

        # Step 2: From y_fit_rp to k_rp
        k_rp = (y_fit_rp - mu) / stdev

        # Step 3: From k_rp to q_rp_norm
        q_rp_norm = (g / 6) + ((k_rp * g / 2 + 1) ** (1 / 3) - 1) * 6 / g

        # Step 4: From q_rp_norm to rp_exceedance
        p_lte = norm.cdf(q_rp_norm, loc=0, scale=1)

    else:
        raise ValueError(
            "Invalid package specified. Choose 'distr' or 'scipy'."
        )

    # Calculate the return periods
    p_gte = 1 - p_lte
    rp = 1 / p_gte

    return rp


def lp3_rv(rp, params, est_method="usgs"):
    """
    Calculate return values for given return periods using the Log-Pearson Type III distribution.

    Parameters:
    rp (list or array-like): List of return periods.
    params (dict or tuple): Parameters for the distribution.
        - For 'usgs' method, a dictionary with keys 'g' (skewness), 'mu' (mean), and 'sd' (standard deviation).
        - For 'lmoments' method, a dictionary with parameters for the Pearson Type III distribution.
        - For 'scipy' method, a tuple with parameters for the Pearson Type III distribution.
    est_method (str, optional): Method to estimate the return values. Options are 'usgs', 'lmoments', or 'scipy'. Default is 'usgs'.

    Returns:
    numpy.ndarray: Return values corresponding to the given return periods.

    Raises:
    ValueError: If an invalid estimation method is provided.
    """
    est_method = est_method.lower()
    if est_method not in ["usgs", "lmoments", "scipy"]:
        raise ValueError(
            "Invalid method. Choose 'usgs' or 'lmoments' or 'scipy'."
        )
    rp_exceedance = [1 / rp for rp in rp]

    if est_method == "usgs":
        g = params["g"]
        mu = params["mu"]
        stdev = params["sd"]

        q_rp_norm = norm.ppf(
            1 - np.array(rp_exceedance), loc=0, scale=1
        )  # Normal quantiles
        k_rp = (2 / g) * (
            ((q_rp_norm - (g / 6)) * (g / 6) + 1) ** 3 - 1
        )  # Skewness adjustment
        y_fit_rp = mu + k_rp * stdev  # Fitted values for return periods
        ret = 10 ** (y_fit_rp)
        # return return_value_lp3_usgs(x, rp)
    elif est_method == "lmoments":
        value_log = distr.pe3.ppf(1 - np.array(rp_exceedance), **params)
        ret = 10 ** (value_log)
    elif est_method == "scipy":
        value_log = pearson3.ppf(1 - np.array(rp_exceedance), *params)
        ret = 10 ** (value_log)

    return ret
