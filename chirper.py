"""

Installation:

    pip install --upgrade oauth2client gspread google-api-python-client ZODB zodbpickle tweepy iso8601
"""

import time
import datetime
import json
import httplib2
import os
import sys

# Authorize server-to-server interactions from Google Compute Engine.
from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools

# ZODB
import ZODB
import ZODB.FileStorage
import BTrees.OOBTree
from persistent.mapping import PersistentMapping
import random
import transaction

# Date parsing
import iso8601

# https://github.com/burnash/gspread
import gspread

# Twitter client
import tweepy

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None


# We need permissions to drive list files, drive read files, spreadsheet manipulation
SCOPES = ['https://www.googleapis.com/auth/devstorage.read_write', 'https://www.googleapis.com/auth/drive.metadata.readonly', 'https://spreadsheets.google.com/feeds']
CLIENT_SECRET_FILE = 'client_secrets.json'
APPLICATION_NAME = 'MEGACORP SPREADSHEET SCRAPER BOT'
OAUTH_DATABASE = "oauth_authorization.json"

FIRST_TWEET_CHOICES = [
    "WE AT MEGACORP THINK YOU MIGHT LIKE US - http://megacorp.example.com",
]

SECOND_TWEET_CHOICES = [
    "AS WELL, WE ARE PROBABLY CHEAPER THAN COMPETITORCORP INC. http://megacorp.example.com/prices",
    "AS WELL, OUR FEATURE SET IS LONGER THAN MISSISSIPPI http://megacorp.example.com/features",
    "AS WELL, OUR CEO IS VERY HANDSOME http://megacorp.example.com/team",

]

# Make sure our text is edited correctly
for tweet in FIRST_TWEET_CHOICES + SECOND_TWEET_CHOICES:
    assert len(tweet) < 140

# How many tweets can be send in one run... limit for testing / debugging
MAX_TWEET_COUNT = 10


# https://developers.google.com/drive/web/quickstart/python
def get_google_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """

    credential_path = os.path.join(os.getcwd(), OAUTH_DATABASE)

    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatability with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def get_tweepy():
    """Create a Tweepy client instance."""
    creds = json.load(open("twitter_oauth.json", "rt"))

    auth = tweepy.OAuthHandler(creds["consumer_key"], creds["consumer_secret"])
    auth.set_access_token(creds["access_token"], creds["access_token_secret"])
    api = tweepy.API(auth)
    return api


def get_database():
    """Get or create a ZODB database where we store information about processed spreadsheets and sent tweets."""

    storage = ZODB.FileStorage.FileStorage('chirper.data.fs')
    db = ZODB.DB(storage)
    connection = db.open()
    root = connection.root

    # Initialize root data structure if not present yet
    with transaction.manager:
        if not hasattr(root, "files"):
            root.files = BTrees.OOBTree.BTree()
        if not hasattr(root, "twitter_handles"):
            # Format of {added: datetime, imported: datetime, sheet: str, first_tweet_at: datetime, second_tweet_at: datetime}
            root.twitter_handles = BTrees.OOBTree.BTree()


    return root


def extract_twitter_handles(spread, sheet_id, column_id="L"):
    """Process one spreadsheet and return Twitter handles in it."""

    twitter_url_prefix = ["https://twitter.com/", "http://twitter.com/"]

    worksheet = spread.open_by_key(sheet_id).sheet1

    col_index = ord(column_id) - ord("A") + 1

    # Painfully slow, 2600 records = 3+ min.
    start = time.time()
    print("Fetching data from sheet {}".format(sheet_id))
    twitter_urls =  worksheet.col_values(col_index)
    print("Fetched everything in {} seconds".format(time.time() - start))

    valid_handles = []

    # Cell contents are URLs (possibly) pointing to a Twitter
    # Extract the Twitter handle from these urls if they exist
    for cell_content in twitter_urls:

        if not cell_content:
            continue

        # Twitter handle as it
        if "://" not in cell_content:
            valid_handles.append(cell_content.strip())
            continue

        # One cell can contain multiple URLs, comma separated
        urls = [url.strip() for url in cell_content.split(",")]

        for url in urls:
            for prefix in twitter_url_prefix:
                if url.startswith(prefix):
                    handle = url[len(prefix):]

                    # Clean old style fragment URLs e.g #!/foobar
                    if handle.startswith("#!/"):
                        handle = handle[len("#!/"):]

                    valid_handles.append(handle)

    return valid_handles


def watch_files(http, title_match=None, folder_id=None) -> list:
    """Check all Google Drive files which match certain file pattern.

    Drive API:

    https://developers.google.com/drive/web/search-parameters

    :return: Iterable GDrive file list
    """

    service = discovery.build('drive', 'v2', http=http)

    if folder_id:
        results = service.files().list(q="'{}' in parents".format(folder_id)).execute()
    elif title_match:
        results = service.files().list(q="title contains '{}'".format(title_match)).execute()
    else:
        raise RuntimeError("Unknown criteria")

    return results["items"]


def scan_for_new_spreadsheets(http, db):
    """Check Google Drive for new spreadsheets.

        1. Use Google Drive API to list all files matching our spreadsheet criteria
        2. If the file is not seen before add it to our list of files to process
    """
    # First discover new spreadsheets

    discovered = False

    for file in watch_files(http, folder_id="0BytechWnbrJVTlNqbGpWZllaYW8"):
        title = file["title"]
        last_char = title[-1]

        # It's .csv, photos, etc. misc files
        if not last_char.isdigit():
            continue

        with transaction.manager:
            file_id = file["id"]
            if file_id not in db.files:
                print("Discovered file {}: {}".format(file["title"], file_id))
                db.files[file_id] = PersistentMapping(file)
                discovered = True

    if not discovered:
        print("No new spreadsheets available")


def extract_twitter_handles_from_spreadsheets(spread, db):
    """Extract new Twitter handles from spreadsheets.

        1. Go through all spreadsheets we know.
        2. If the spreadsheet is not marked as processed extract Twitter handles out of it
        3. If any of the Twitter handles is unseen before add it to the database with empty record

    """

    # Then extract Twitter handles from the files we know about
    for file_id, file_data in db.files.items():

        spreadsheet_creation_date = iso8601.parse_date(file_data["createdDate"])

        print("Processing {} created at {}".format(file_data["title"], spreadsheet_creation_date))

        # Check the processing flag on the file
        if not file_data.get("processed"):
            handles = extract_twitter_handles(spread, file_id)

            # Using this transaction lock we write all the handles to the database once or none of them
            with transaction.manager:
                for handle in handles:
                    # If we have not seen this
                    if handle not in db.twitter_handles:
                        print("Importing Twitter handle {}".format(handle))
                        db.twitter_handles[handle] = PersistentMapping({"added": spreadsheet_creation_date, "imported": datetime.datetime.utcnow(), "sheet": file_id})

                file_data["processed"] = True


def send_tweet(twitter, msg):
    """Send a Tweet.
    """

    try:
        twitter.update_status(status=msg)
    except tweepy.error.TweepError as e:
        try:
            # {"errors":[{"code":187,"message":"Status is a duplicate."}]}
            resp = json.loads(e.response.text)
            if resp.get("errors"):
                if resp["errors"][0]["code"] == 187:
                    print("Was duplicate {}".format(msg))
                    time.sleep(10 + random.randint(0, 10))
                    return
        except:
            pass

        raise RuntimeError("Twitter doesn't like us: {}".format(e.response.text or str(e))) from e

    # Throttle down the bot
    time.sleep(30 + random.randint(0, 90))


def tweet_everything(twitter, db):
    """Run through all users and check if we need to Tweet to them. """

    tweet_count = 0

    for handle_id, handle_data in db.twitter_handles.items():

        with transaction.manager:

            # Check if we had not sent the first Tweet yet and send it
            if not handle_data.get("first_tweet_at"):

                tweet = "@{} {}".format(handle_id, random.choice(FIRST_TWEET_CHOICES))

                print("Tweeting {} at {}".format(tweet, datetime.datetime.utcnow()))
                send_tweet(twitter, tweet)
                handle_data["first_tweet_at"] = datetime.datetime.utcnow()
                tweet_count += 1

            # Check if we had not sent the first Tweet yet and send it
            elif not handle_data.get("second_tweet_at"):

                tweet = "@{} {}".format(handle_id, random.choice(SECOND_TWEET_CHOICES))

                print("Tweeting {} at {}".format(tweet, datetime.datetime.utcnow()))
                send_tweet(twitter, tweet)
                handle_data["second_tweet_at"] = datetime.datetime.utcnow()
                tweet_count += 1

        if tweet_count >= MAX_TWEET_COUNT:
            # Testing limiter - don't spam too much if our test run is out of control
            break


def main():

    script_name = sys.argv[1] if sys.argv[0] == "python" else sys.argv[0]
    print("Starting {} at {} UTC".format(script_name, datetime.datetime.utcnow()))

    # open database
    db = get_database()

    # get OAuth permissions from Google for Drive client and Spreadsheet client
    credentials = get_google_credentials()
    http = credentials.authorize(httplib2.Http())
    spread = gspread.authorize(credentials)
    twitter = get_tweepy()

    # Do action
    scan_for_new_spreadsheets(http, db)
    extract_twitter_handles_from_spreadsheets(spread, db)
    tweet_everything(twitter, db)


main()
