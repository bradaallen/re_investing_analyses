import numpy as np
from src.utils.helpers import pct2float, string2float


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

    return cleaned_input_data
