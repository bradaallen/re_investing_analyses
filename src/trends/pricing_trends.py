import requests
import csv
import os
import pandas as pd
from io import StringIO
from box import Box
from ..trends.housing_trends import create_indexed_prices


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


def generate_price_to_rent_ratios(raw_price_data, raw_rent_data, crosswalk_data):
    """
    Calculates PTR data for different MSAs using Zillow data
    https://www.zillow.com/research/data/

    Args:
        raw_price_data (pd.DataFrame): Raw Zillow price data as a df
        raw_rent_data (pd.DataFrame): Raw Zillow rent data as a df
        crosswalk_data (pd.DataFrame): Joins MSA_ID to Zillow rent data
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

    ptr_ratio_df.rename(columns={"RegionID": "ZILLOW_ID"}, inplace=True)
    ptr_ratio_df = ptr_ratio_df.merge(
        crosswalk_data[["ZILLOW_ID", "MSA_ID"]], on="ZILLOW_ID", how="left"
    )

    return ptr_ratio_df


def trulia_ptr_categories(input, moderate_threshold=16, high_threshold=21):
    """
    Defines whether markets are buyers or renters markets based on Trulia's
    price to rent ratio guidelines:
        * 1 to 15 indicates it is much better to buy than rent
        * 16 to 20 indicates it is typically better to rent than buy
        * 21 or more indicates it is much better to rent than buy

    Args:
        input (int or str): PTR ratio calculated from Zillow data
        moderate_threshold (int; default: 16): Trulia threshold defined above
        high_threshold (int; default: 21): Trulia threshold defined above
    Returns:
        result (str): Classification on purchase quality
    """

    if input < moderate_threshold:
        result = "Good to buy"
    elif (input >= moderate_threshold) & (input < high_threshold):
        result = "Moderate investment"
    elif input > high_threshold:
        result = "Good to rent"
    else:
        result = "Error"
    return result


def price_to_rent_categories(input_data):
    """
    Pulls most recent PTR ratio from Zillow data and classifies it per Trulia.

    Args:
        input_data (pd.DataFrame): PTR ratio calculated from Zillow data
    Returns:
        output_data (pd.DataFrame): Recent classification data, price, and PTR ratio
    """

    # Initialize dataframe and reset input data to do timeseries analysis
    msa_list = list(input_data.MSA_ID.dropna().unique())
    input_data = input_data.set_index("dates")

    output_data = pd.DataFrame(
        data=None, index=msa_list, columns=["current_ratio", "current_zillow_price"]
    )

    # Pull most recent PTR ratio and price data
    for item in msa_list:
        output_data.loc[item, "current_ratio"] = (
            input_data.loc[input_data.MSA_ID == item, "ratio"].last("1M").values[0]
        )
        output_data.loc[item, "current_zillow_price"] = (
            input_data.loc[input_data.MSA_ID == item, "prices"].last("1M").values[0]
        )

    # Set type and classify data
    output_data["current_ratio"] = output_data["current_ratio"].astype("float")
    output_data["rating"] = output_data["current_ratio"].apply(
        lambda x: trulia_ptr_categories(x)
    )

    # Ensure MSA_ID is a string (to join elsewhere)
    output_data.index = output_data.index.astype("int").astype("str")

    return output_data


def generate_pricing_output(crosswalk_home, crosswalk_name, params=Box):
    """
    Generates two dataframes for price-to-rent analysis.

    Args:
        crosswalk_home (filepath, str): Path to CSV data stored for analysis
        crosswalk_name (filename, str): Filename of crosswalk for analysis
        params (Box): Config parameters for analysis
    Returns:
        ptr_df (pd.DataFrame): detailed price to rent ratios by MSA, and chartable
        final_ptr_output_df (pd.DataFrame): classification of PTR ratios on quality of
        buying vs. renting
    """

    # Load and format crosswalk data
    crosswalk_df = pd.read_csv(os.path.join(crosswalk_home, crosswalk_name))
    crosswalk_df["ZILLOW_ID"] = (
        crosswalk_df["ZILLOW_ID"].fillna(0).astype("int").astype("str")
    )

    # Reshape price and rent data, create price-to-rent ratios and categories
    price_df = create_zillow_dataframe(params.endpoint_price)
    rent_df = create_zillow_dataframe(params.endpoint_rents)
    ptr_df = generate_price_to_rent_ratios(price_df, rent_df, crosswalk_df)
    final_ptr_output_df = price_to_rent_categories(ptr_df)

    return ptr_df, final_ptr_output_df
