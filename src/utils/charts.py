import pandas as pd
import re
from src.trends.population_trends import generate_acs_employment_data


def view_economic_makeup(industry_dict, year, plt_list):
    """
    To identify the makeup of the economy (in headcount) by MSA

    Args:
        industry_dict (dict): mapping column labels to easier text for analysis
        year (int): year in which survey was conducted by the Census
        plt_list (list): List of MSAs to highlight for chart
    Returns:
        economic_workforce_plt (plt)
    """

    # Generate economic data and normalize; set index to NAME (for plotting purposes)
    economic_workforce_plt = generate_acs_employment_data(industry_dict, year).drop(
        ["total_workforce"], axis=1
    )
    economic_workforce_plt.set_index("NAME", inplace=True)
    economic_workforce_plt = economic_workforce_plt.div(
        economic_workforce_plt.sum(1), axis=0
    )

    return economic_workforce_plt.loc[plt_list].plot(
        kind="bar", stacked=True, figsize=(10, 7)
    )


def view_economic_changes(detailed_data, plt_list):
    """
    Identifies changes in headcount by industry, by MSA between the two years
    highlighted for analysis. Currently focused on the 2013-2018 analysis and
    will need to be updated if more flexible in the future.

    Args:
        detailed_data (pd.DataFrame): detailed dataframe generated for analysis
        plt_list (list): List of MSAs to highlight for chart
    Returns:
        economic_changes_plt (plt)
    """

    # Takes detailed output (includes delta and CAGR columns) and keep only columns
    # with "_delta" present in them. Set index to NAME for plotting purposes.
    delta_list = [
        i
        for i in detailed_data.columns
        if re.search("_delta", i) and not re.search("_workforce", i)
    ]

    # TODO: Allow this function to be more flexible than a 2018 analysis.
    economic_changes_plt = detailed_data.set_index("NAME_2018")

    return economic_changes_plt.loc[plt_list, delta_list].plot.bar(
        stacked=True, figsize=(10, 7)
    )


def view_economic_dynamism(detailed_data, plt_list):
    """
    Identifies changes in GDP by industry, by MSA between the two years
    highlighted for analysis. Currently focused on the 2013-2018 analysis and
    will need to be updated if more flexible in the future.

    Args:
        detailed_data (pd.DataFrame): detailed dataframe generated for analysis
        plt_list (list): List of MSAs to highlight for chart
    Returns:
        economic_dynamism_plt (plt)
    """

    # Takes detailed output (includes delta and CAGR columns) and keep only columns
    # with "_delta" present in them. Set index to NAME for plotting purposes.
    cagr_list = [
        i
        for i in detailed_data.columns
        if re.search("_cagr", i)
        and not re.search("_workforce", i)
        and not re.search("_growth", i)
    ]

    # TODO: Allow this function to be more flexible than a 2018 analysis.
    economic_dynamism_df = detailed_data.set_index("NAME_2018")
    economic_dynamism_plt = economic_dynamism_df.loc[
        economic_dynamism_df["gdp_growth_cagr"].notnull(), cagr_list
    ]

    return economic_dynamism_plt.loc[plt_list,].plot.bar(figsize=(10, 7))
