import re
import math
import pandas as pd
import matplotlib.pyplot as plt
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


def view_inventory_by_msa(input_data, ncol=2):
    """
    Shows relationship between new listing, inventory, and sales on one axis.
    Median sale price over time is indexed on the secondary axis.

    Args:
        input_data (pd.DataFrame): detailed dataframe of Redfin data
        ncol (int): Number of columns to generate in the plot
    Returns:
        None (plt shows up in sysout)
    """

    # Setup data for generating plots. We want to have one subplot per MSA, two columns,
    # and a flexible # of rows (based on the # of MSAs to enter). We will loop through
    # MSAs using a counter.
    msa_list = list(input_data.region.unique())
    nrow = math.ceil(len(msa_list) / ncol)
    count = 0

    # Initialize the subplots
    fig, axes = plt.subplots(nrow, ncol)

    for r in range(nrow):
        for c in range(ncol):
            # try:
            plotting_df = input_data.loc[
                input_data.region == msa_list[count],
                [
                    "month_of_period_end",
                    "region",
                    "new_listings",
                    "inventory",
                    "sales",
                    "indexed_sale_price",
                ],
            ].set_index("month_of_period_end")

            plotting_df[["new_listings", "inventory", "sales"]].plot(
                title=msa_list[count], ax=axes[r, c], figsize=(14, 600)
            )
            plotting_df["indexed_sale_price"].plot(secondary_y=True, ax=axes[r, c])
            count += 1
            # except:
            #    count += 1
            #    continue

    return None


def view_price_growth_across_msas(input_data, msas_per_subplot=5):
    """
    Shows comparisons of indexed sale prices across MSAs.

    Args:
        input_data (pd.DataFrame): detailed dataframe of Redfin data
        msas_per_subplot (int): Number of MSAs to compare per subplot
    Returns:
        None (plt shows up in sysout)
    """

    # Setup data for generating plots. We want to have 5 MSAs per subplot, 2 columns,
    # and a flexible # of rows (based on the # of MSAs to enter). We will loop through
    # MSAs using a counter.
    # TODO: Make more flexible - can combine with next function and input list for MSAs.
    plotting_df = input_data.pivot(
        index="month_of_period_end", columns="region", values="indexed_sale_price"
    )
    col_list = list(plotting_df.columns)

    ncol = 2
    nrow = math.ceil((len(col_list) / msas_per_subplot) / ncol)
    count = 0

    # Initialize the subplots
    fig, axes = plt.subplots(nrow, ncol)

    for r in range(nrow):
        for c in range(ncol):
            try:
                plotting_df[
                    col_list[count * msas_per_subplot : (count + 1) * msas_per_subplot]
                ].plot(ax=axes[r, c], figsize=(14, 100))
                count += 1
            except TypeError:
                count += 1
                print(
                    f"These MSAs have no numeric data to plot: {list(col_list[count * msas_per_subplot: (count + 1) * msas_per_subplot])}."
                )
                continue

    return None


def view_moi_by_msa(input_data):
    """
    Shows relationship between DOM, MOI, and sale price, over time.

    Args:
        input_data (pd.DataFrame): detailed dataframe of Redfin data
    Returns:
        None (plt shows up in sysout)
    """

    # Setup data for generating plots. We want to have 5 MSAs per subplot, 2 columns,
    # and a flexible # of rows (based on the # of MSAs to enter). We will loop through
    # MSAs using a counter.
    # TODO: Make more flexible - can combine with prior function and input list for MSAs.
    msa_list = list(input_data.region.unique())
    ncol = 2
    nrow = math.ceil(len(msa_list) / ncol)
    count = 0

    # Initialize the subplots
    fig, axes = plt.subplots(nrow, ncol)

    for r in range(nrow):
        for c in range(ncol):
            plotting_df = input_data.loc[
                input_data.region == msa_list[count],
                [
                    "month_of_period_end",
                    "region",
                    "days_on_market",
                    "indexed_sale_price",
                    "rolling_moi",
                ],
            ].set_index("month_of_period_end")

            plotting_df[["days_on_market", "indexed_sale_price"]].plot(
                title=msa_list[count], ax=axes[r, c], figsize=(14, 600)
            )
            plotting_df["rolling_moi"].plot(secondary_y=True, ax=axes[r, c])
            count += 1

    return None
