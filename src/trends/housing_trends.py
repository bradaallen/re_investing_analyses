import os
import pandas as pd
from box import Box
from statsmodels.tsa.seasonal import seasonal_decompose
from src.utils.math_and_type_helpers import cagr, pct2float, string2float


def clean_redfin_data(redfin_data, crosswalk_data):
    """
    Prepares Redfin data (from URL below) by adding index, formatting column types,
    and updating column names.
    URL: https://www.redfin.com/blog/data-center/

    Args:
        redfin_data (pd.DataFrame): Raw data loaded from Redfin's data center. Pulled
        from a Tableau plug-in at the link in the desc. Manual process & all strings.
        crosswalk_data (pd.DataFrame): Mapping table to add MSA data (allows for joins
        with other analyses). Built myself, saved with the codebase.
    Returns:
        cleaned_redfin_data (pd.DataFrame)
    """

    # Reformat columns to allow for tabbing / hinting
    redfin_data.columns = [
        col.strip().lower().replace(" ", "_") for col in list(redfin_data.columns)
    ]

    # Identify columns that require reformatting to be set to numeric
    pct_list = [
        "median_sale_price_mom",
        "median_sale_price_yoy",
        "homes_sold_mom",
        "homes_sold_yoy",
        "inventory_mom",
        "inventory_yoy",
        "new_listings_mom",
        "new_listings_yoy",
        "average_sale_to_list",
        "average_sale_to_list_mom",
        "average_sale_to_list_yoy",
    ]
    float_list = [
        "median_sale_price",
        "homes_sold",
        "new_listings",
        "inventory",
        "days_on_market",
        "days_on_market_mom",
        "days_on_market_yoy",
    ]

    # Reformat types using helper functions and datetime setting
    for item in pct_list:
        redfin_data[item] = redfin_data[item].apply(lambda x: pct2float(x))

    for item in float_list:
        redfin_data[item] = redfin_data[item].apply(lambda x: string2float(x))

    redfin_data["month_of_period_end"] = pd.to_datetime(
        redfin_data["month_of_period_end"], format="%b-%y"
    )

    # Add MSA_ID column and set to string
    tmp_crosswalk = (
        crosswalk_data.loc[
            crosswalk_data["REDFIN_DATA"].notnull(), ["REDFIN_DATA", "MSA_ID"]
        ]
        .set_index("REDFIN_DATA")
        .astype("str")
    )
    redfin_data = redfin_data.join(tmp_crosswalk, on="region")

    cleaned_redfin_data = redfin_data

    return cleaned_redfin_data


def add_monthly_sales(redfin_data, title):
    """
    Infer monthly sales using inventory and new listing data from Redfin.

    Args:
        redfin_data (pd.DataFrame): Cleaned and formatted Redfin data
        title (str): Column title to use in updated df
    Returns:
        monthly_sales_series (pd.Series): Series with sales data
    """

    # Get list of MSAs for analysis and initialize field to populate
    msa_list = list(redfin_data.MSA_ID.unique())
    redfin_data[title] = 0

    # Sales inferred by looking at the delta between this and next month's inventory,
    # plus new listings for the current month
    for item in msa_list:
        next_month_inventory = redfin_data.loc[
            redfin_data.MSA_ID == item, "inventory"
        ].shift(periods=-1)
        redfin_data.loc[redfin_data.MSA_ID == item, title] = (
            redfin_data.loc[redfin_data.MSA_ID == item, "inventory"]
            - next_month_inventory
            + redfin_data.loc[redfin_data.MSA_ID == item, "new_listings"]
        )

    monthly_sales_series = redfin_data[title]

    return monthly_sales_series


def create_indexed_prices(
    input_data, period_index_column, MSA_ID_column, price_column, new_column_name
):
    """
    Indexes Redfin monthly price data to first price in dataset (Feb 1, 2012).

    Args:
        input_data (pd.DataFrame): Cleaned and formatted Redfin data
        period_index_column (str): Column with datetime - for indexing
        MSA_ID_column (str): Index is on a per-MSA basis, this specifies the
        location for that data
        price_column (str): Column with prices to be indexed
        new_column_name (str): Column title to use in updated df
    Returns:
        indexed_column (pd.DataFrame): Series with indexed price data
    """

    # Pivot data to allow for easy indexing
    pivot_for_indexing = input_data.pivot(
        index=period_index_column, columns=MSA_ID_column, values=price_column
    )

    # Indexes data to first date in series by MSA (Feb 2012).
    for msa in pivot_for_indexing:
        pivot_for_indexing[msa] = pivot_for_indexing[msa].div(
            pivot_for_indexing[msa][0] / 100, axis=0
        )

    indexed_column = pivot_for_indexing.stack().rename(new_column_name).reset_index()

    return indexed_column


def add_rolling_window(
    input_data,
    period_index_column,
    MSA_ID_column,
    rolling_column,
    window_period,
    new_column_name,
):
    """
    Imputes rolling average for given variables over cleaned Redfin data.

    Args:
        input_data (pd.DataFrame): Cleaned and formatted Redfin data
        period_index_column (str): Column with datetime - for indexing
        MSA_ID_column (str): Index is on a per-MSA basis, this specifies the
        location for that data
        rolling_column (str): Column with values to be imputed on a rolling basis
        window_period (int): Length of window period to use (based on period)
        new_column_name (str): Column title to use in updated df
    Returns:
        rolling_column (pd.DataFrame): Series with imputed rolling data
    """

    # Pivot data to allow for easy indexing
    pivot_for_indexing = input_data.pivot(
        index=period_index_column, columns=MSA_ID_column, values=rolling_column
    )

    # Performs rolling window calculation by MSA
    for msa in pivot_for_indexing:
        pivot_for_indexing[msa] = (
            pivot_for_indexing[msa].rolling(window=window_period).mean()
        )

    rolling_column = pivot_for_indexing.stack().rename(new_column_name).reset_index()

    return rolling_column


def create_balanced_and_seller_summary(
    prepped_redfin_data, crosswalk_data, market_threshold
):
    """
    Calculate CAGR and length of "buyers" and "sellers markets", based on a
    defined "market threshold" - rolling months of inventory
    Args:
        prepped_redfin_data (pd.DataFrame): Cleaned and formatted Redfin data
        market_threshold (int): Threshold between sellers and buyers markets (in MOI)
    Returns:
        summary_df (pd.DataFrame): Summary DF - market performance by MSA
    """

    # Initialize MSA list and final dataframe
    msa_list = list(prepped_redfin_data.MSA_ID.unique())
    summary_df = pd.DataFrame(
        data=None,
        index=msa_list,
        columns=[
            "balanced_dom",
            "sellers_dom",
            "balanced_cagr",
            "sellers_cagr",
            "balanced_max_length",
            "sellers_max_length",
        ],
    )

    # For each MSA, take the values in which a price is available and
    # calculate CAGRs for balanced and sellers markets
    for item in msa_list:
        tmp_df = prepped_redfin_data.loc[
            (prepped_redfin_data.MSA_ID == item)
            & (prepped_redfin_data.rolling_isp.notnull()),
        ]
        flag_series = tmp_df["rolling_moi"] > market_threshold

        # Some markets are only sellers markets (all periods below the threshold)
        # This condition will skip calculating for balanced markets if that is
        # the case.

        if flag_series.sum() > 0:
            # Only sums values when a sellers market is flagged. Therefore, the
            # value that shows up most frequently is the longest balanced market.
            balanced_market_series = (~flag_series).cumsum()[flag_series] == (
                ~flag_series
            ).cumsum()[flag_series].value_counts().idxmax()

            # The # of times the .idxmax() shows up is the length of the balanced
            # market.
            balanced_market_month = (
                (~flag_series).cumsum()[flag_series].value_counts().max()
            )

            # Uses the .idxmax() to identify the first and last values in the index
            # for the market period. This is used in CAGR and DOM calculations.
            bmin = balanced_market_series[balanced_market_series].index.min()
            bmax = balanced_market_series[balanced_market_series].index.max()

            if balanced_market_month > 12:
                # DOM
                summary_df.loc[item, "balanced_dom"] = tmp_df.loc[
                    bmin:bmax, "days_on_market"
                ].mean()
                # CAGR
                summary_df.loc[item, "balanced_cagr"] = cagr(
                    tmp_df,
                    bmax,
                    bmin,
                    timing="month",
                    operation="apply",
                    prepend="rolling_isp",
                )
                # Length
                summary_df.loc[item, "balanced_max_length"] = balanced_market_month

        if (~flag_series).sum() > 0:
            # Same calculations as performed in the balanced section, but the inverse.
            # Please refer above for the logic.
            sellers_market_series = (
                flag_series.cumsum()[~flag_series]
                == (flag_series).cumsum()[~flag_series].value_counts().idxmax()
            )
            sellers_market_month = (
                flag_series.cumsum()[~flag_series].value_counts().max()
            )

            smin = sellers_market_series[sellers_market_series].index.min()
            smax = sellers_market_series[sellers_market_series].index.max()

            if sellers_market_month > 12:
                # DOM
                summary_df.loc[item, "sellers_dom"] = tmp_df.loc[
                    smin:smax, "days_on_market"
                ].mean()
                # CAGR
                summary_df.loc[item, "sellers_cagr"] = cagr(
                    tmp_df,
                    smax,
                    smin,
                    timing="month",
                    operation="apply",
                    prepend="rolling_isp",
                )
                # Length
                summary_df.loc[item, "sellers_max_length"] = sellers_market_month

    crosswalk_data["MSA_ID"] = crosswalk_data["MSA_ID"].astype("int").astype("str")
    summary_df = summary_df.merge(
        crosswalk_data[["MSA_ID", "NAME_2018"]],
        left_index=True,
        right_on="MSA_ID",
        how="left",
    )

    return summary_df


def trendline(x):
    """
    Used in lambda function. Looks at 1, 3, 6m averages for DOM
    to determine if market is heating up or cooling down.
    """
    m = x.mean()

    if x.between(m - 1, m + 1).sum() == 3:
        return "Stable DOM"
    elif x["6m_dom"] > x["1m_dom"]:
        return "Declining DOM"
    else:
        return "Increasing DOM"


def preliminary_market_type(existing_summary_data, dom=60, moi=4):
    """
    Used to make a quick inference on the quality of a market:
        * Contraction:  DOM above threshold; L6M MOI less than threshold;
            # TODO: Add condition on when permits are greater than than 1 per 2 jobs
        * Expansion:  DOM below threshold; L6M MOI greater than threshold;
        * Trough:  DOM above threshold; L6M MOI greater than threshold;
        * Peak:  TOM below threshold; L6M MOI less than threshold;
            # TODO: Add condition on when permits are less than 1 per 2 jobs
    Args:
        existing_summary_data (pd.DataFrame): Summary data from function
            "create_balanced_and_seller_summary"
        dom (int): Threshold between market cycles for DOM
        moi (int): Threshold between market cycles for MOI
    Returns:
        condition (str): Preliminary market label
    """

    x = existing_summary_data
    condition = None
    if (x[0:3].mean() > dom) & (x[3] < moi):
        condition = "Contraction"
    if (x[0:3].mean() < dom) & (x[3] > moi):
        condition = "Expanding"
    if (x[0:3].mean() > dom) & (x[3] > moi):
        condition = "Trough"
    if (x[0:3].mean() < dom) & (x[3] < moi):
        condition = "Peak"

    return condition


def create_dom_and_market_cycle_summary(prepped_redfin_data, crosswalk_data):
    """
    Summary data on market and DOM trends over last 1, 3, 6 months.

    Args:
        prepped_redfin_data (pd.DataFrame): Cleaned and formatted Redfin data
    Returns:
        dmc_summary_df (str): Summary data with DOM trends and market categorization
    """

    # Initialize table for summary
    dmc_summary_df = pd.DataFrame(
        data=None,
        index=prepped_redfin_data.MSA_ID.unique(),
        columns=["1m_dom", "3m_dom", "6m_dom", "last_6m_moi", "market_type"],
    )

    for item in prepped_redfin_data.MSA_ID.unique():
        dom_series = prepped_redfin_data.loc[
            prepped_redfin_data.MSA_ID == item,
            ["month_of_period_end", "days_on_market"],
        ].set_index("month_of_period_end")

        # Strip seasonality from data using 'seasonal_compose', use  resulting trend for
        # inferring DOM over several time periods.
        try:
            result = seasonal_decompose(dom_series, model="additive", freq=12)

            dmc_summary_df.loc[item, "1m_dom"] = result.trend.dropna().last("1M").mean()
            dmc_summary_df.loc[item, "3m_dom"] = result.trend.dropna().last("3M").mean()
            dmc_summary_df.loc[item, "6m_dom"] = result.trend.dropna().last("6M").mean()
            dmc_summary_df.loc[item, "last_6m_moi"] = prepped_redfin_data.loc[
                prepped_redfin_data.MSA_ID == item, "rolling_moi"
            ][-2:-1].values[0]

        except ValueError:
            print(
                f"The series for MSA_ID: {item} has challenges with inferring a trendline."
            )
            continue

    # Use DOM calculations above to infer overall market characteristics
    for index, row in dmc_summary_df.iterrows():
        dmc_summary_df.loc[row[0:2].name, "dom_trend"] = trendline(row[0:3])
        dmc_summary_df.loc[row[0:2].name, "market_type"] = preliminary_market_type(
            row[0:5]
        )

    crosswalk_data["MSA_ID"] = crosswalk_data["MSA_ID"].astype("int").astype("str")
    dmc_summary_df = dmc_summary_df.merge(
        crosswalk_data[["MSA_ID", "NAME_2018"]],
        left_index=True,
        right_on="MSA_ID",
        how="left",
    )

    return dmc_summary_df


def generate_housing_output(params=Box):
    """
    2 DataFrames providing information on market classifications, summaries, and DOM trends.

    Args:
        params (Box config): Cleaned and formatted Redfin data
    Returns:
        bss_df (pd.DataFrame): Summary of balanced and seller market classification and length
        dom_df (pd.DataFrame): Days-of-market analysis and classification of current market quality
    """

    # Load Redfin and crosswalk data
    redfin_df = pd.read_csv(os.path.join(params.data_home, params.redfin_data))
    crosswalk_df = pd.read_csv(os.path.join(params.data_home, params.crosswalk))

    # Clean Redfin data; add sales and months of inventory
    redfin_df = clean_redfin_data(redfin_df, crosswalk_df)
    redfin_df["sales"] = add_monthly_sales(redfin_df, "sales")
    redfin_df["months_of_inventory"] = redfin_df["inventory"] / redfin_df["sales"]

    redfin_df = redfin_df.merge(
        create_indexed_prices(
            redfin_df,
            "month_of_period_end",
            "region",
            "median_sale_price",
            new_column_name="indexed_sale_price",
        ),
        how="left",
        on=["month_of_period_end", "region"],
    )

    redfin_df = redfin_df.merge(
        add_rolling_window(
            redfin_df,
            "month_of_period_end",
            "region",
            "months_of_inventory",
            window_period=9,
            new_column_name="rolling_moi",
        ),
        how="left",
        on=["month_of_period_end", "region"],
    ).merge(
        add_rolling_window(
            redfin_df,
            "month_of_period_end",
            "region",
            "indexed_sale_price",
            window_period=6,
            new_column_name="rolling_isp",
        ),
        how="left",
        on=["month_of_period_end", "region"],
    )

    # Generate summary output
    bss_df = create_balanced_and_seller_summary(
        redfin_df, crosswalk_df, market_threshold=4
    )
    dom_df = create_dom_and_market_cycle_summary(redfin_df, crosswalk_df)

    return bss_df, dom_df
