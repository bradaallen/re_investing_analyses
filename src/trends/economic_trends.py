import re
import requests
import json
import pandas as pd
import numpy as np
from io import StringIO
from src.utils.math_and_type_helpers import pct2float, string2float
from src.utils.io_helpers import curl_census_link


def housing_shortage_categories(input, moderate_threshold=1.5, high_threshold=2.5):
    """
    Defines whether markets are buyers or renters markets based on CityLab and
    Apartment List's Rentonomics general guidelines: 2 jobs for every new 1 unit.

    Args:
        input (int or str): PTR ratio calculated from Zillow data
        moderate_threshold (int; default: 1.5): Below is risk of oversupply
        high_threshold (int; default: 2.5): Risk of undersupply
    Returns:
        result (str): Classification on purchase quality
    """

    if input < moderate_threshold:
        result = "Risk of Oversupply"
    elif (input >= moderate_threshold) & (input < high_threshold):
        result = "Balanced Market"
    elif input > high_threshold:
        result = "Risk of Undersupply"
    else:
        result = "Error"

    return result


def clean_apartment_list_data(input_data, crosswalk_data):
    """
    Cleans permit-to-job growth data aggregated by Apartment List - link can be found
    in the article below.
    https://www.apartmentlist.com/rentonomics/building-permits-vs-job-growth-2019/

    Args:
        input_data (pd.DataFrame): Raw Apartment List data loaded into pandas
        crosswalk_data (pd.DataFrame): Mapping table to add MSA data (allows for joins
        with other analyses). Built myself, saved with the codebase.
    Returns:
        cleaned_input_data (pd.DataFrame)
    """

    input_data.columns = [
        col.strip().lower().replace(" ", "_") for col in list(input_data.columns)
    ]

    float_list = [
        "total_jobs_added_(2008-2018)",
        "jobs_per_1,000_residents_(2008-2018)",
        "total_building_permits_\n(2008_-_2018)",
        "permits_per_1,000_residents_(2008-2018)",
        "jobs_per_permit_(2008-2018)",
        "multi-family_permit_share_(1990-2005)",
        "average_size_of_multi-family_properties_(1990-2005)",
        "average_size_of_multi-family_properties_(2006-2018)",
    ]
    pct_list = [
        "multi-family_permit_share_(1990-2005)",
        "multi-family_permit_share_(2006-2018)",
    ]

    for item in pct_list:
        input_data[item] = input_data[item].replace("NA", np.nan)
        input_data[item] = input_data[item].apply(lambda x: pct2float(x))

    for item in float_list:
        input_data[item] = input_data[item].replace("NA", np.nan)
        input_data[item] = input_data[item].apply(lambda x: string2float(x))

    tmp_crosswalk = (
        crosswalk_data.loc[
            crosswalk_data["APARTMENT_LIST_DATA"].notnull(),
            ["APARTMENT_LIST_DATA", "MSA_ID"],
        ]
        .set_index("APARTMENT_LIST_DATA")
        .astype("str")
    )
    cleaned_input_data = input_data.join(tmp_crosswalk, on="location")

    cleaned_input_data["shortage_categories"] = cleaned_input_data[
        "jobs_per_permit_(2008-2018)"
    ].apply(lambda x: housing_shortage_categories(x))

    return cleaned_input_data


def HOLD_generate_permitting_table(year_date, cert_filepath):
    """
    Cleans permitting data from the Census Building Permits Survey. Joins multiple
    years together (using the year_date list) to allow for longitudinal analysis.

    Overview: https://www.census.gov/construction/bps/
    Sample link: https://www.census.gov/construction/bps/txt/t3yu201812.txt

    Args:
        year_date (list): List of values (ints in YYYYMM format) for querying
        Census website. Format follows the structure in the "link" variable
        cert_filepath (.pem file): Certification for pinging Census website
    Returns:
        cleaned_df (pd.DataFrame): concatenated DFs formatted for analysis
    """

    link = f"https://www.census.gov/construction/bps/txt/t3yu{year_date}.txt"

    # We want to only read the numerical values at the Census link. This
    # read_flag prevents us from stripping erroneous data from the site.
    read_flag = False
    cleaned_text = []

    # Uses pycurl to download the .txt file from the Census
    fh = curl_census_link(link, cert_filepath)
    for line in fh.splitlines():

        # some lines have no strings, adding padding for .isdigit() check
        line += " "
        if line[0].isdigit():
            cleaned_text.append(line)
            last_line = line
            read_flag = True

        # some whitespace and text at the bottom needs to be removed
        elif read_flag and len(line) > 5:
            del cleaned_text[-1]
            cleaned_text.append(last_line + line)
        else:
            read_flag = False

    msa_text = []
    permits_text = []

    # Split the MSA data from the building permits statistics
    for item in cleaned_text:

        # CA MSAs have less space for numbers, identify them specifically
        if "Los Angeles" in item or "San Francisco" in item:
            split_list = re.split("CA", item, maxsplit=1)
            msa_text.append(split_list[0] + "CA")
            permits_text.append(split_list[1])
        else:
            split_list = re.split(r"\s{4,}", item, maxsplit=1)
            msa_text.append(split_list[0])
            permits_text.append(split_list[1])

    msa_split = []
    for item in msa_text:
        tmp_list = re.split(r"\s", item, maxsplit=2)
        msa_split.append(tmp_list)

    msa_df = pd.DataFrame(msa_split, columns=["csa", "cbsa", "msa_name"])

    permits_df = pd.read_csv(
        StringIO("\n".join(permits_text)), delim_whitespace=True, header=None
    )

    permits_df.columns = [
        "total",
        "one_unit",
        "two_units",
        "three_four_units",
        "five_plus_units",
        "structures_with_five_plus",
    ]

    # Format year and set to datetime for analysis
    permits_df["year"] = year_date[:4]
    permits_df["dates"] = pd.to_datetime(
        "12-31-" + permits_df["year"], infer_datetime_format=True
    )

    cleaned_df = msa_df.merge(permits_df, left_index=True, right_index=True)

    return cleaned_df


def HOLD_format_bls_cbsa_codes(filepath):
    """
    The BLS has an API for "State and Area Employment, Hours, and Earnings."
    To query this API, one needs the numerical State & CBSA codes concatenated.
    This function helps prepare that code.

    It uses my own crosswalk developed for this analysis; a similar reference
    can be found at the NBER site: http://data.nber.org/data/cbsa-fips-county-crosswalk.html

    Args:
        filepath (str): Path for .csv file of crosswalk
    Returns:
        formatted_crosswalk_df (pd.DataFrame): Crosswalk with formatted 'MSA_STATE_ID' field
    """

    # Crosswalk loads STATE_MSA data as int, dropping the leading 0 from the code
    # This function adds the 0 back, if necessary
    def clean_state(x):
        if len(x) == 6:
            x = "0" + x
        return x

    formatted_crosswalk_df = pd.read_csv(filepath)

    # We want to provide the BLS with valid crosswalk data, particularly as the API has daily
    # limits. '99999' is a personal code for extraneous markets to the analysis.
    formatted_crosswalk_df = formatted_crosswalk_df.loc[
        (formatted_crosswalk_df.MSA_ID != 99999)
        & (formatted_crosswalk_df.MSA_STATE_ID.notnull()),
    ]
    formatted_crosswalk_df["MSA_STATE_ID"] = (
        formatted_crosswalk_df["MSA_STATE_ID"].astype("int").astype("str")
    )
    formatted_crosswalk_df["MSA_STATE_ID"] = formatted_crosswalk_df[
        "MSA_STATE_ID"
    ].apply(lambda x: clean_state(x))

    return formatted_crosswalk_df


def HOLD_bls_employee_headcount_by_msa(series_id_list, start_year, end_year):
    """
    Function for one query to the BLS website. The focus is on the survey, "State
    and Area Employment, Hours, and Earnings," and all employees across all industries.
    Subgroups are available, if desired. Examples from: https://www.bls.gov/help/hlpforma.htm#SM

    Example series ID: 'SMU19197802023800001'
    Positions       Value           Field Name
        1-2             SM              Prefix
        3               U               Seasonal Adjustment Code
        4-5             19              State Code <-- look for 6 digit MSA
        6-10            19780           Area Code <-- look for 6 digit MSA
        11-18           20238000        SuperSector and Industry Code <-- the 2 digit ending in 00001 is what I want
        19-20           01             	Data Type Code (all employees, in thousands)

    Note: There is a daily limit of 500 queries, 50 series per query, and a set time frame of up
    to 20 years. The resulting numerical output is "All Employees, In Thousands."

    Args:
        series_id_list (list): list of series_ids (as strings). Currently generated via crosswalk
        start_year (int): starting year for query - will apply to all strings
        end_year (int): ending year for query - will apply to all strings
    Returns:
        formatted_crosswalk_df (pd.DataFrame): Crosswalk with formatted 'MSA_STATE_ID' field
    """

    # Query API in link below. Flexible for all series IDs, start years, and end years.
    headers = {"Content-type": "application/json"}
    data = json.dumps(
        {
            "seriesid": series_id_list,
            "startyear": str(start_year),
            "endyear": str(end_year),
        }
    )
    p = requests.post(
        "https://api.bls.gov/publicAPI/v2/timeseries/data/", data=data, headers=headers
    )
    json_data = json.loads(p.text)

    joined_bls_output = pd.DataFrame(data=None)

    # Query output is converted from JSON string to pandas dataframe. It is easy to hit the BLS limit.
    # When that occurs, the message is returned as a list and the error is dealt with.
    try:
        for i in range(len(json_data["Results"]["series"])):
            tmp_df = pd.DataFrame.from_dict(json_data["Results"]["series"][i]["data"])
            tmp_df["series_name"] = json_data["Results"]["series"][i]["seriesID"]
            joined_bls_output = joined_bls_output.append(tmp_df)
    except KeyError:
        return json_data["message"]

    return joined_bls_output


def HOLD_format_bls_employment_data(input_data, crosswalk_data):
    """
    Takes output from BLS query (func "bls_employee_headcount_by_msa"), adds MSA IDs,
    and formats the values for analysis.

    Args:
        input_data (pd.DataFrame): BLS output (eg, joined_bls_output (df) from "bls_employee_headcount_by_msa")
        crosswalk_data (pd.DataFrame): .CSV crosswalk data as a pandas DF
    Returns:
        cleaned_df: Formatted final output
    """

    # Take BLS Series IDs and pull out the State & CBSA code
    def trim_for_state_cbsa(x):
        return x[3:10]

    def trim_for_cbsa(x):
        return x[5:10]

    # Strip series_name column for state & CBSA codes
    input_data["MSA_STATE_ID"] = input_data["series_name"].apply(
        lambda x: trim_for_state_cbsa(x)
    )
    input_data["MSA_ID"] = input_data["series_name"].apply(lambda x: trim_for_cbsa(x))

    # Rename columns, set types, set employment to thousands
    input_data.rename(columns={"value": "employment"}, inplace=True)
    input_data["year"] = input_data["year"].astype("int")
    input_data["employment"] = input_data["employment"].astype("float").multiply(1000)

    # Join MSA name to final dataframe
    cleaned_df = input_data.merge(
        crosswalk_data[["MSA_STATE_ID", "NAME_2018"]], on="MSA_STATE_ID"
    )

    # Format columns 'periodName' and 'year' to datetime
    cleaned_df["dates"] = pd.to_datetime(
        cleaned_df["periodName"] + "-" + cleaned_df["year"].astype("str"),
        infer_datetime_format=True,
    )

    return cleaned_df


def HOLD_create_job_shortage_ratio(permitting_input, bls_input):
    """
    XXX
    """

    # Generate pivots of both data
    cleaned_permitting_output_pivot = permitting_input.pivot(
        index="dates", columns="MSA_ID", values="total"
    )
    cleaned_bls_output_pivot = bls_input.pivot(
        index="dates", columns="MSA_ID", values="employment"
    )

    permit_list = list(cleaned_permitting_output_pivot.columns)
    bls_list = list(cleaned_bls_output_pivot.columns)

    final_list = list(set(permit_list) | set(bls_list))
    tmp_df = pd.DataFrame(data=None, index=final_list)
    final_df = pd.DataFrame(data=None, index=final_list)

    timeframe_list = ["3Y", "2Y", "1Y"]
    for period in timeframe_list:
        permitting_series = pd.Series(
            cleaned_permitting_output_pivot.last(period).sum(), name=period + "_permits"
        )
        growth_series = pd.Series(
            cleaned_bls_output_pivot.last(period).iloc[-1]
            - cleaned_bls_output_pivot.last(period).iloc[0],
            name=period + "_growth",
        )
        tmp_df = tmp_df.join([growth_series, permitting_series])

    for period in timeframe_list:
        final_df[period + "_ratio"] = (
            tmp_df[period + "_growth"] / tmp_df[period + "_permits"]
        )

    return final_df
