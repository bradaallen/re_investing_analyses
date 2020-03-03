import requests
import json
import pandas as pd
import numpy as np
from box import Box
from src.utils.math_and_type_helpers import cagr


def generate_acs_employment_data(industry_dict, survey_year):
    """
    Takes survey data from the American Community Survey 5-survey_year Data (2009-2018)
    Table C24050: INDUSTRY BY OCCUPATION FOR THE CIVILIAN EMPLOYED POPULATION 16 YEARS AND OVER

    Detailed descriptions of variables can be found at this link:
    https://www2.census.gov/programs-surveys/acs/tech_docs/subject_definitions/2018_ACSSubjectDefinitions.pdf?#

    Results will be compared to the 2013 survey. According to the Census, this data is comparable (other
    surveys within 2013-2018 are not comparable. In this time, the NAICS classifications changed,
    but since we only use 2-digit values, we should be fine.

    Args:
        industry_dict (dict): mapping column labels to easier text for analysis
        survey_year (int): year in which survey was conducted by the Census
    Returns:
        base_df (pd.DataFrame)
    """

    # Components of the API endpoint from the Census
    endpoint = f"https://api.census.gov/data/{str(survey_year)}/acs/acs5?get=NAME,group(C24050)&"
    geography = (
        f"for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*"
    )

    # Make API call and format column names
    response = requests.get(endpoint + geography)
    json_data = json.loads(response.text)
    ingested_df = pd.DataFrame(columns=json_data[0], data=json_data[1:])
    ingested_df.rename(
        columns={
            "metropolitan statistical area/micropolitan statistical area": "MSA_ID"
        },
        inplace=True,
    )
    ingested_df.rename(columns=industry_dict, inplace=True)

    # Drop columns unnecessary for analysis; set dtypes and final formatting
    base_df = ingested_df[["NAME", "MSA_ID"] + list(industry_dict.values())]
    for item in list(industry_dict.values()):
        base_df[item] = base_df[item].astype("float")

    base_df = base_df.loc[:, ~base_df.columns.duplicated()]
    base_df["total_workforce"] = base_df.sum(axis=1)

    base_df.set_index("MSA_ID", inplace=True)
    print(
        f"The total workforce in {survey_year} was {base_df['total_workforce'].sum()}."
    )

    return base_df


def add_population_feature(year):
    """
    Generate a population feature by MSA for a given Census ACS year

    Args:
        year (int): year in which survey was conducted by the Census
    Returns:
        population_df (pd.DataFrame) 2-col total population by MSA
    """

    # Components of the API endpoint from the Census
    endpoint = (
        f"https://api.census.gov/data/{str(year)}/acs/acs5?get=NAME,group(B01001)"
    )
    geography = (
        f"&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*"
    )

    # Make API call and format column names
    response = requests.get(endpoint + geography)
    json_data = json.loads(response.text)
    population_df = pd.DataFrame(columns=json_data[0], data=json_data[1:])
    population_df.rename(
        columns={
            "metropolitan statistical area/micropolitan statistical area": "MSA_ID",
            "B01001_001E": "pop",
        },
        inplace=True,
    )

    # Drop columns unnecessary for analysis; set dtypes and final formatting
    population_df = population_df[["pop", "MSA_ID"]].set_index("MSA_ID")
    population_df["pop"] = population_df["pop"].astype("float")

    return population_df


def add_attainment_feature(year):
    """
    Generate a count of the number of individuals with an MS degree or higher,
    by MSA for a given Census ACS year

    Args:
        year (int): year in which survey was conducted by the Census
    Returns:
        attainment_df (pd.DataFrame) 2-col advanced degree count by MSA
    """

    # Components of the API endpoint from the Census
    endpoint = (
        f"https://api.census.gov/data/{str(year)}/acs/acs5?get=NAME,group(B15003)"
    )
    geography = (
        f"&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*"
    )

    # Make API call and format column names
    response = requests.get(endpoint + geography)
    json_data = json.loads(response.text)
    attainment_df = pd.DataFrame(columns=json_data[0], data=json_data[1:])
    attainment_df.rename(
        columns={
            "metropolitan statistical area/micropolitan statistical area": "MSA_ID",
            "B15003_023E": "MS",
            "B15003_024E": "Prof",
            "B15003_025E": "PHD",
        },
        inplace=True,
    )

    # Drop columns unnecessary for analysis; set dtypes and final formatting
    attainment_df = attainment_df[["MSA_ID", "MS", "Prof", "PHD"]].set_index("MSA_ID")
    attainment_df[["MS", "Prof", "PHD"]] = attainment_df[["MS", "Prof", "PHD"]].astype(
        "float"
    )
    attainment_df["deg_count"] = (
        attainment_df["MS"] + attainment_df["Prof"] + attainment_df["PHD"]
    )

    return attainment_df["deg_count"].to_frame()


def add_mobility_feature(year):
    """
    Generate a count of the number of individuals with an MS degree or higher,
    by MSA - that have newly moved to that MSA - for a given Census ACS year

    Args:
        year (int): year in which survey was conducted by the Census
    Returns:
        mobility_df (pd.DataFrame) 2-col advanced degree and moved count by MSA
    """

    # Components of the API endpoint from the Census
    endpoint = (
        f"https://api.census.gov/data/{str(year)}/acs/acs5?get=NAME,group(B07009)"
    )
    geography = (
        f"&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*"
    )

    # Make API call and format column names
    response = requests.get(endpoint + geography)
    json_data = json.loads(response.text)
    mobility_df = pd.DataFrame(columns=json_data[0], data=json_data[1:])
    mobility_df.rename(
        columns={
            "metropolitan statistical area/micropolitan statistical area": "MSA_ID",
            "B07009_024E": "move_same_state",
            "B07009_030E": "move_different_state",
        },
        inplace=True,
    )

    # Drop columns unnecessary for analysis; set dtypes and final formatting
    mobility_df = mobility_df[
        ["MSA_ID", "move_same_state", "move_different_state"]
    ].set_index("MSA_ID")
    mobility_df[["move_same_state", "move_different_state"]] = mobility_df[
        ["move_same_state", "move_different_state"]
    ].astype("float")
    mobility_df["total_move"] = (
        mobility_df["move_same_state"] + mobility_df["move_different_state"]
    )

    return mobility_df["total_move"]


def add_gdp_cagr_feature(start_year, end_year, api_key):
    """
    Calculate an MSA's GDP CAGR between two years using the BEA's CAGDP table

    A user guide for the BEA interface can be found here:
    https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf

    Args:
        year_list (list): Years for calculating CAGR amount - input needs to
                            be ['end_year', 'beginning_year']
        api_key (string): Uniquely generated BEA API key
    Returns:
        bea_pvt_df (pd.DataFrame)
    """

    # Components of the API endpoint from the Census
    endpoint = f"https://apps.bea.gov/api/data/?UserID={api_key}&method=GetData"
    table = f"&datasetname=Regional&TableName=CAGDP2&LineCode=2"
    criteria = f"&Year={end_year},{start_year}&GeoFips=MSA&ResultFormat=json"

    # Make API call and format column names
    response = requests.get(endpoint + table + criteria)
    json_data = json.loads(response.text)
    bea_df = pd.DataFrame.from_dict(json_data["BEAAPI"]["Results"]["Data"])
    bea_df.rename(columns={"GeoFips": "MSA_ID"}, inplace=True)

    # Pivot table and perform CAGR calculation
    bea_df["DataValue"] = bea_df["DataValue"].str.replace(",", "").astype("float")
    bea_pvt_df = bea_df.pivot(index="MSA_ID", columns="TimePeriod", values="DataValue")
    bea_pvt_df["gdp_growth_cagr"] = cagr(
        bea_pvt_df, end_year, start_year, timing="year", operation="column", prepend="",
    )

    return bea_pvt_df


def add_wage_cagr_feature(start_year, end_year, api_key):
    """
    Calculate an MSA's median wage CAGR between two years using the
    BEA's MARPI table.

    A user guide for the BEA interface can be found here:
    https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf

    Args:
        year_list (list): Years for calculating CAGR amount - input needs to
                            be ['end_year', 'beginning_year']
        api_key (string): Uniquely generated BEA API key
    Returns:
        bea_pvt_df (pd.DataFrame)
    """

    # Components of the API endpoint from the Census
    endpoint = f"https://apps.bea.gov/api/data/?UserID={api_key}&method=GetData"
    table = f"&datasetname=Regional&TableName=MARPI&LineCode=2"
    criteria = f"&Year={end_year},{start_year}&GeoFips=MSA&ResultFormat=json"

    # Make API call and format column names
    response = requests.get(endpoint + table + criteria)
    json_data = json.loads(response.text)
    bea_df = pd.DataFrame.from_dict(json_data["BEAAPI"]["Results"]["Data"])
    bea_df.rename(columns={"GeoFips": "MSA_ID"}, inplace=True)

    # Pivot table and perform CAGR calculation
    bea_df["DataValue"] = bea_df["DataValue"].replace("(NA)", np.nan)
    bea_df["DataValue"] = bea_df["DataValue"].str.replace(",", "").astype("float")
    bea_pvt_df = bea_df.pivot(index="MSA_ID", columns="TimePeriod", values="DataValue")
    bea_pvt_df["wage_growth_cagr"] = cagr(
        bea_pvt_df, end_year, start_year, timing="year", operation="column", prepend="",
    )

    return bea_pvt_df


def add_industry_delta_and_cagr_features(data, industry_dict, start_year, end_year):
    """
    Take industry data from the function "generate_acs_employment_data" and
    calculate the delta and cagr between the two years under consideration.

    Args:
        data (pd.DataFrame): Input dataframe - from "generate_acs_employment_data"
        industry_dict (dict): mapping column labels to easier text for analysis
        year_list (list): Years for calculating CAGR amount - input needs to
                        be ['end_year', 'beginning_year']
    Returns:
        data (pd.DataFrame)
     """

    # Generate list of columns for performing calculations
    add_items = list(industry_dict.values()) + ["total_workforce"]

    # Calculate delta and CAGR values
    for item in add_items:
        data[item + "_delta"] = (
            data[item + "_" + str(end_year)] - data[item + "_" + str(start_year)]
        )

        col_text = item + "_"
        data[item + "_cagr"] = cagr(
            data,
            end_year,
            start_year,
            timing="year",
            operation="column",
            prepend=col_text,
        )

    return data


def generate_economic_output(bea_key, params=Box):
    """
    Generate output and summary of ACS and BEA data. Different markers of economic heath:
    education, mobility, economic growth, by industry.

    Args:
        bea_key (str, environment variable): API key from BEA
        params (Box, config): Config variables
    Returns:
        filtered_df (pd.DataFrame): Dataframe with summary of economic performance
        in the recent past for different MSAs
     """

    detailed_df = generate_acs_employment_data(
        params.industry_dict, params.base_years.end_year
    ).join(
        generate_acs_employment_data(
            params.industry_dict, params.base_years.start_year
        ),
        lsuffix="_" + str(params.base_years.end_year),
        rsuffix="_" + str(params.base_years.start_year),
    )

    detailed_df = add_industry_delta_and_cagr_features(
        detailed_df,
        params.industry_dict,
        params.base_years.start_year,
        params.base_years.end_year,
    )

    detailed_df = (
        detailed_df.join(
            add_gdp_cagr_feature(
                params.base_years.start_year, params.base_years.end_year, bea_key
            ),
            rsuffix="_gdp",
        )
        .join(
            add_wage_cagr_feature(
                params.wage_years.start_year, params.wage_years.end_year, bea_key
            ),
            rsuffix="_wage",
        )
        .join(add_population_feature(params.input_year))
        .join(add_attainment_feature(params.input_year))
        .join(add_mobility_feature(params.input_year))
    )

    filtered_cols = [
        "NAME_2018",
        "total_workforce_cagr",
        "gdp_growth_cagr",
        "wage_growth_cagr",
        "deg_count",
        "total_move",
        "pop",
    ]
    filtered_df = detailed_df.loc[
        detailed_df["gdp_growth_cagr"].notnull(), filtered_cols
    ]

    filtered_df["volatile_economy"] = detailed_df[params.neg_list].sum(axis=1)
    filtered_df["pct_educated"] = filtered_df["deg_count"] / filtered_df["pop"]
    filtered_df["pct_educated_moved"] = filtered_df["total_move"] / filtered_df["pop"]

    return filtered_df
