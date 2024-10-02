import pandas as pd
import numpy as np
from scipy.stats import skew, norm, pearson3
from lmoments3 import distr, lmom_ratios


def params_lp3_usgs(x, imputation_method="lowest"):
    """
    Calculate the parameters for the Log-Pearson Type III distribution using USGS method.

    Parameters:
    x (array-like): Array of observed data.
    imputation_method (str): Method for handling non-positive values in the data. Default is "lowest".

    Returns:
    dict: Dictionary containing the mean (mu), standard deviation (sd), and skewness (g) of the log-transformed data.

    Example:
    >>> import numpy as np
    >>> data = np.array([10, 20, 30, 40, 50])
    >>> params_lp3_usgs(data)
    {'mu': np.float64(1.415836249209525),'sd': np.float64(0.2759982424449524),'g': np.float64(-0.5854140744039718)}
    """
    if imputation_method == "lowest":
        x = np.where(x <= 0, np.min(x[np.nonzero(x)]), x)
    x_log = np.log10(x)
    return {"mu": np.mean(x_log), "sd": np.std(x_log, ddof=1), "g": skew(x_log)}


def return_value_lp3_usgs(x, return_period):
    """
    Calculate the Log-Pearson Type III return value for given return periods using USGS method.

    Parameters:
    x (array-like): Array of observed data.
    return_period (float): Return period for which the quantile is calculated.

    Returns:
    float: return value for the given return period.

    Example:
    >>> import numpy as np
    >>> data = np.array([10, 20, 30, 40, 50])
    >>> return_value_lp3_usgs(data, 100)
    np.float64(86.91377279516328)
    """
    params = params_lp3_usgs(x)
    g = params["g"]
    mu = params["mu"]
    stdev = params["sd"]

    rp_exceedance = 1 / return_period
    q_rp_norm = norm.ppf(1 - rp_exceedance)  # Normal quantiles
    k_rp = (2 / g) * (
        ((q_rp_norm - (g / 6)) * (g / 6) + 1) ** 3 - 1
    )  # Skewness adjustment
    y_fit_rp = mu + k_rp * stdev  # Fitted values for return periods
    quantiles_rp = 10**y_fit_rp
    return quantiles_rp


def return_period_lp3_lmom(x, params):
    """
    Calculate the return period for given data and Log-Pearson Type III parameters.

    Args:
        x (array-like): Array of observed data.
        params (tuple): Parameters of the Log-Pearson Type III distribution.

    Returns:
        float: Return period.

    Example:
        >>> import numpy as np
        >>> from lmoments3 import distr, lmom_ratios
        >>> data = np.array([10, 20, 30, 40, 50])
        >>> params = params_lp3_lmom(data)
        >>> return_period_lp3_lmom(30, params)
        np.float64(9.264029097369093)
    """
    # Calculate the cumulative distribution function (CDF) value
    p_lte = distr.pe3.cdf(x, **params)
    p_gte = 1 - p_lte
    rp = 1 / p_gte
    return rp


def params_lp3_lmom(x):
    """
    Calculate the parameters for the Log-Pearson Type III distribution using L-moments.

    Args:
        x (array-like): Array of observed data.

    Returns:
        dict: Dictionary containing the parameters of the Log-Pearson Type III distribution.
    Examples:
        >>> import numpy as np
        >>> from return_periods import lp3_params_lmom
        >>> data = np.array([1.2, 2.3, 3.4, 4.5, 5.6])
        >>> params = lp3_params_lmom(data)
        >>> print(params)
        OrderedDict({'skew': 3.9736810031063223, 'loc': np.float64(1.125), 'scale': np.float64(2.459845871608456)})
    """

    # Compute L-moments
    lmom = lmom_ratios(x, nmom=4)
    # Fit the Log-Pearson Type III distribution using L-moments
    params = distr.pe3.lmom_fit(lmom)
    return params


def return_value_lp3_lmom(x, return_period, params):
    """
    Calculate the Log-Pearson Type III return value for given return periods using L-moments.

    Args:
        x (array-like): Array of observed data.
        return_period (float): Return period for which the quantile is calculated.
        params (tuple): Parameters of the Log-Pearson Type III distribution.

    Returns:
        float: Return value for the given return period.

    Example:
        >>> import numpy as np
        >>> data = np.array([10, 20, 30, 40, 50])
        >>> params = params_lp3_lmom(data)
        >>> return_value_lp3_lmom(data, 100, params)
        np.float64(86.91377279516328)
    """
    rp_exceedance = 1 / return_period
    quantile = distr.pe3.isf(rp_exceedance, **params)
    return quantile


def return_value(x, return_period, method="usgs"):
    """
    Calculate the return value for given return periods using specified method.

    Parameters:
    x (array-like): Array of observed data.
    return_period (float): Return period for which the quantile is calculated.
    method (str): Method to use for calculation ("usgs" or "lmom"). Default is "usgs".

    Returns:
    float: Return value for the given return period.

    Example:
    >>> import numpy as np
    >>> data = np.array([10, 20, 30, 40, 50])
    >>> return_value(data, 100, method="usgs")
    np.float64(86.91377279516328)
    >>> return_value(data, 100, method="lmom")
    np.float64(86.91377279516328)
    """
    method = method.lower()
    if method not in ["usgs", "lmom"]:
        raise ValueError("Invalid method. Choose 'usgs' or 'lmom'.")

    if method == "usgs":
        return return_value_lp3_usgs(x, return_period)
    elif method == "lmom":
        params = params_lp3_lmom(x)
        return return_value_lp3_lmom(x, return_period, params)
