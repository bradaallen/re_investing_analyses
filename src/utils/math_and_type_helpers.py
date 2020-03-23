import pandas as pd
import re
from fuzzywuzzy import process


def cagr(input_data, end, begin, timing="year", operation="column", prepend=""):
    """
    Calculate CAGR for a given time period and operation.

    Args:
        input_data (pd.DataFrame): dataframe with columns for calculating cagr
        end (int): ending period for cagr
        begin (int): beginning period for cagr
        timing (str): period between end and begin dates (year or month)
        operation (str): declare whether analysis is row-wise or column-wise
        prepend (str, optional): if information is required for column names
    Returns:
        cagr (float)
    """

    if (end - begin) < 0:
        raise ValueError(
            "The end period precedes the start period. Check order of inputs."
        )

    timeframe = ((end - begin) / 12) if timing == "month" else (end - begin)

    if operation == "apply":
        ending_pd = input_data.loc[end, prepend]
        beginning_pd = input_data.loc[begin, prepend]

    elif operation == "column":
        ending_pd = input_data[prepend + str(end)]
        beginning_pd = input_data[prepend + str(begin)]

    return (ending_pd / beginning_pd) ** (1 / timeframe) - 1.0


def string2float(x):
    """
    Converts string values (with commas) to float for analysis.
    If representing monetary amounts, $ and K (if present) are stripped.
    """
    x = str(x)
    x = re.sub(",", "", x)

    if "$" in x:
        x = x.strip("$")
    if "K" in x:
        x = x.strip("K")
        x = float(x) * 1000
    elif "k" in x:
        x = x.strip("k")
        x = float(x) * 1000
    return float(x)


def pct2float(x):
    """
    Converts string percentages to floats for analysis.
    """
    x = str(x)
    x = re.sub(",", "", x)
    return float(x.strip("%")) / 100


def get_match(cbsa_data, zillow_list):
    """
    Used in an apply function to match Zillow data to Census CBSA data.
    """
    best_match = process.extractOne(cbsa_data, zillow_list)

    return best_match[0]
