import os
from dotenv import load_dotenv

from src.trends.economic_trends import generate_economic_output
from src.trends.housing_trends import generate_housing_output
from src.trends.pricing_trends import generate_pricing_output
from src.trends.shortage_trends import generate_shortage_output

from src.utils.io_helpers import DEFAULT_HOME, load_config


def main(TopN=50):
    """
    Main function for executing pipeline for GSB annual life satisfaction survey.

    Args:
        TopN (int): XXX
    Returns:
        None: All output is saved in the output filepath specified in constants.py.
    """

    # Set filepaths, load config, and environment variables
    home_path = None
    home_path = DEFAULT_HOME if home_path == "" or home_path is None else home_path
    cfg = load_config(os.path.join(home_path, "src\config.yaml"))
    load_dotenv(dotenv_path=cfg.filepaths.secrets_path + ".env")

    # 1. Generate base table with detailed economic data (from ACS)
    economic_df = generate_economic_output(
        os.getenv("BEA_KEY"), params=cfg.population_trends
    )

    # 2. Generate housing data - balanced and sellers markets (BSS), days on market analysis (DOM)
    bss_df, dom_df = generate_housing_output(params=cfg.filepaths)

    # 3. Add price-to-rent trends
    detailed_ptf_df, summary_ptf_df = generate_pricing_output(
        cfg.filepaths.data_home, cfg.filepaths.crosswalk, params=cfg.pricing_trends
    )

    # 4. Add housing shortage trends (permits vs. job creation)
    clean_df = generate_shortage_output(
        cfg.filepaths.secrets_path,
        google_params=cfg.pricing_trends.google_spreadsheets,
        shortage_params=cfg.filepaths,
    )

    # 5. Join data to create master table for analysis. Keep select columns per dataset
    final_df = economic_df.join(
        [
            dom_df[["MSA_ID", "last_6m_moi", "market_type", "dom_trend"]].set_index(
                "MSA_ID"
            ),
            clean_df[
                ["MSA_ID", "jobs_per_permit_(2008-2018)", "shortage_categories"]
            ].set_index("MSA_ID"),
            summary_ptf_df,
        ]
    )

    # 6. Perform scoring. Bring in relevant dictionaries from config and create flags.
    market_classification_dict = cfg.overall_market_rubric.market_classification_dict
    price_rent_ratio_dict = cfg.overall_market_rubric.price_rent_ratio_dict
    housing_shortage_dict = cfg.overall_market_rubric.housing_shortage_dict
    population_trends_dict = cfg.overall_market_rubric.population_trends_dict

    final_df["market_flag"] = list(
        map(market_classification_dict.get, final_df["market_type"])
    )
    final_df["ptr_flag"] = list(map(price_rent_ratio_dict.get, final_df["rating"]))
    final_df["shortage_flag"] = list(
        map(housing_shortage_dict.get, final_df["shortage_categories"])
    )

    # 7. Apply weighting for pop trends to "Top N" markets.
    for key, value in population_trends_dict.items():
        final_df[value[0]] = 0
        final_df.loc[final_df.nlargest(TopN, key).index, value[0]] = value[1]

    final_df["first_sort"] = final_df[
        [item for item, weight in list(population_trends_dict.values())]
        + ["market_flag", "ptr_flag", "shortage_flag"]
    ].sum(axis=1)

    return final_df.sort_values(by="first_sort", ascending=False)


if __name__ == "__main__":
    main()
