import pickle
import os.path
import pycurl
import pandas as pd
import googleapiclient.discovery
import google_auth_oauthlib.flow
import google.auth.transport.requests
from io import BytesIO
from src.constants import SPREADSHEET_ID, RANGE_NAME, SCOPES


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


def curl_census_link(link, cert_filepath):
    """
    XXX

    """

    b_obj = BytesIO()
    crl = pycurl.Curl()

    # Set URL value & pem certificate
    crl.setopt(crl.URL, link)
    crl.setopt(pycurl.CAINFO, cert_filepath)

    # Write bytes that are utf-8 encoded
    crl.setopt(crl.WRITEDATA, b_obj)

    # Perform a file transfer
    crl.perform()

    # End curl session
    crl.close()

    # Get the content stored in the BytesIO object (in byte characters)
    get_body = b_obj.getvalue()

    # Decode the bytes stored in get_body to HTML
    return get_body.decode("utf8")
