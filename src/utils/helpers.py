import pandas as pd
import re
import pickle
import os.path
import googleapiclient.discovery
import google_auth_oauthlib.flow
import google.auth.transport.requests
from src.constants import SPREADSHEET_ID, RANGE_NAME, SCOPES


def cagr(input_data, end, begin, timing="year", operation="column", prepend=""):
    """
    Calculate CAGR for a given time period and operation.

    Args:
        input_data (pd.DataFrame): dataframe with columns for calculating cagr
        end (int): ending period for cagr
        begin (int): beginning period for cagr
        timing (str): period between end and begin dates (year or month)
        operation (str): declare whether analysis is row-wise or column-wise
        prepend (str, optional): if information is required for column names
    Returns:
        cagr (float)
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


def string2float(x):
    """
    Converts string values (with commas) to float for analysis.
    If representing monetary amounts, $ and K (if present) are stripped.
    """
    x = str(x)
    x = re.sub(",", "", x)

    if "$" in x:
        x = x.strip("$|K")
        x = float(x) * 1000
    return float(x)


def pct2float(x):
    """
    Converts string percentages to floats for analysis.
    """
    x = str(x)
    return float(x.strip("%")) / 100


def import_spreadsheet_data(TOKEN, CREDENTIALS):
    """
    Imports spreadsheet data for analysis and consumption. Requires credentials for Google API
    (stored in credentials.json with the file, SPREADSHEET_ID, RANGE_NAME, and SCOPE
    (found in constants.py). Once imported, results are converted from a dictionary to a dataframe.
    Args:
        TOKEN: pickle file provided by Google
        CREDENTIALS: provided by Google; stored as a .json
    Returns:
        values_df (pd.DataFrame)
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(TOKEN):
        with open(TOKEN, "rb") as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(google.auth.transport.requests.Request())
        else:
            flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS, SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(TOKEN, "wb") as token:
            pickle.dump(creds, token)

    service = googleapiclient.discovery.build("sheets", "v4", credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = (
        sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    )
    values = result.get("values", [])
    values_df = pd.DataFrame(data=values[1:], columns=values[0])

    return values_df
