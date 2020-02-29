import requests
import csv
from io import StringIO
import pandas as pd
from src.trends.housing_trends import create_indexed_prices


def create_zillow_dataframe(endpoint):
    """
    Imports raw Zillow research data. This is used to pull in median rent and
    median price data from ~2000 to today.
    https://www.zillow.com/research/data/

    Args:
        endpoint (URL): URL associated with Zillow's download link at the above site
    Returns:
        cleaned_input_data (pd.DataFrame)
    """

    # Load the Zillow CSV data and process row-wise
    r = requests.get(endpoint)
    r.encoding = "ISO-8859-1"  # encoding is not sent by the Zillow server
    csvio = StringIO(r.text, newline="")
    data = []
    for row in csv.DictReader(csvio):
        data.append(row)

    # Convert to DataFrame and process numeric data
    zillow_df = pd.DataFrame(data)
    numeric_columns = zillow_df.columns[3:]
    for col in numeric_columns:
        zillow_df[col] = pd.to_numeric(zillow_df[col])

    return zillow_df


def format_zillow_for_ptr_ratio(input_data, new_column_name):
    """
    Reshapes raw Zillow data to allow for price and rent data to be joined.

    Args:
        input_data (pd.DataFrame): Raw Zillow data to be reformatted for PTR calculation
        new_column_name (str): Column name for the new stacked data

    Returns:
        output_data (pd.DataFrame)
    """

    # Reshape raw data to have price and rents to be joinable
    input_data.columns.name = "dates"
    output_data = (
        input_data.set_index(["RegionID", "RegionName"])
        .drop(["SizeRank"], axis=1)
        .stack()
        .rename(new_column_name)
        .to_frame()
        .reset_index()
    )
    output_data["dates"] = pd.to_datetime(output_data["dates"])

    return output_data


def generate_price_to_rent_ratios(raw_price_data, raw_rent_data):
    """
    Calculates PTR data for different MSAs using Zillow data
    https://www.zillow.com/research/data/

    Args:
        raw_price_data (pd.DataFrame): Raw Zillow price data as a df
        raw_rent_data (pd.DataFrame): Raw Zillow rent data as a df

    Returns:
        ptr_ratio_df (pd.DataFrame)
    """

    # Reshape input data to allow for join
    price_pivot_df = format_zillow_for_ptr_ratio(raw_price_data, "prices")
    rent_pivot_df = format_zillow_for_ptr_ratio(raw_rent_data, "rents")

    # Divide values by one another to get the price-to-rent ratio. Rent data is in months.
    join_keys = ["RegionID", "RegionName", "dates"]
    ptr_ratio_df = price_pivot_df.join(
        rent_pivot_df.set_index(join_keys), how="inner", on=join_keys
    )
    ptr_ratio_df["ratio"] = ptr_ratio_df["prices"] / (ptr_ratio_df["rents"] * 12)

    # Index data to allow for easy comparison of growths
    indexed_prices_df = create_indexed_prices(
        ptr_ratio_df, "dates", "RegionName", "prices", "indexed_prices"
    )
    indexed_rents_df = create_indexed_prices(
        ptr_ratio_df, "dates", "RegionName", "rents", "indexed_rents"
    )

    # Join indexed data back to master dataframe
    join_keys = ["dates", "RegionName"]
    ptr_ratio_df = ptr_ratio_df.join(
        indexed_prices_df.set_index(join_keys), on=join_keys
    ).join(indexed_rents_df.set_index(join_keys), on=join_keys)

    return ptr_ratio_df
