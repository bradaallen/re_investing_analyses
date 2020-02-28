import pandas as pd


def cagr(input_data, end, begin, timing="year", operation="column", prepend=""):
    """
    XXX
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
