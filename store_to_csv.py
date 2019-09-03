import csv
import os
import time
import datetime as dt
from datetime import datetime, timedelta
import credentials
import tweepy
from tweepy import OAuthHandler

# Necessary steps for tweepy authorisation
# PersonalCredentials
consumer_key = credentials.get_consumer_key()
consumer_secret = credentials.get_consumer_secret()
access_token = credentials.get_access_token()
access_secret = credentials.get_access_secret()
# Authorisation
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

# One less appealing thing about the timestamp from tweepy is that it is in UK timezone. I found this function to
# change it.


def datetime_from_utc_to_local(utc_datetime):
    now_timestamp = time.time()
    offset = dt.datetime.fromtimestamp(now_timestamp) - dt.datetime.utcfromtimestamp(now_timestamp)
    return utc_datetime + offset


# Empty Arrays
tmp_text = []
tmp_id = []
tmp_min_id = []
tmp_max_id = []
tmp_timestamp = []

# Specify Search Variables
query = "*"
geocodes = ["51.746888,5.923912,4.92km", "51.700942,5.995977,3.83km", "51.638775,6.048827,5.21km",
            "51.573164,6.082442,3.65km", "51.543287,5.870964,3.45km", "51.541790,5.947528,2.28km",
            "51.552680,6.131537,2.75km", "51.488365,5.920051,5.95km", "51.464641,6.054996,12.2km",
            "51.353214,6.106489,9.81km", "51.288877,5.951449,11.25km", "51.218864,5.606763,2.25km",
            "51.246824,5.770156,10.71km", "51.163785,5.963509,9.8km", "51.165521,6.134136,3.84km",
            "51.055988,5.867689,8.84km", "50.860897,5.865022,17.43km"]

# <editor-fold desc="Function to obtain min_id using tweepy">
# We use since_id to define the amount of days that we are allowed to look back
# This will be used as the minimum id that we are searching for
# We are allowed to look back ~10 days
max_date = datetime.now() - timedelta(days=1)
tweet_date = "{0}-{1:0>2}-{2:0>2}".format(max_date.year, max_date.month, max_date.day)

for geocode in geocodes:
    for tweet in tweepy.Cursor(api.search, q=query, geocode=geocode, until=tweet_date).items(1):
        tmp_min_id.append(tweet.id)
        tmp_timestamp.append(tweet.created_at)
print("Min ID = " + str(max(tmp_min_id)) + ", created at: " + str(datetime_from_utc_to_local(max(tmp_timestamp))))
tmp_timestamp = []
# </editor-fold>
min_id = max(tmp_min_id)  # using folded function from one line above

# <editor-fold desc="Function to obtain max_id using tweepy -> id of latest tweet">
# We use max_id in the loop, but the starting value will be the id of latest tweet

for geocode in geocodes:
    for tweet in tweepy.Cursor(api.search, q=query, geocode=geocode).items(1):
        tmp_max_id.append(tweet.id)
        tmp_timestamp.append(tweet.created_at)
print("Max ID = " + str(max(tmp_max_id)) + ", created at: " + str(datetime_from_utc_to_local(max(tmp_timestamp))))
tmp_timestamp = []
# </editor-fold>
max_id = max(tmp_max_id)  # using folded function from one line above, this is id of latest tweet
max_id_tmp = max_id

# Open CSV file where we will store the tweets
# First check whether the file exists already
file_name = "tweets_tryout.csv"
file_exists = os.path.isfile(file_name)
# If it exist we simply open it
if file_exists is True:
    csvFile = open(file_name, "a", newline="", encoding="utf-8")
    csvWriter = csv.writer(csvFile, delimiter=";")
# If it does not exist we add the headers
else:
    csvFile = open(file_name, "a", newline="", encoding="utf-8")
    csvWriter = csv.writer(csvFile, delimiter=";")
    csvWriter.writerow(["created_at", "id", "id_str", "text", "source", "truncated", "in_reply_to_status_id",
                        "in_reply_to_status_id_str", "in_reply_to_user_id", "in_reply_to_user_id_str",
                        "in_reply_to_screen_name", "user.id", "user.id_str", "user.name", "user.screen_name",
                        "user.location", "user.url", "user.description", "user.verified", "user.followers_count",
                        "user.friends_count", "user.listed_count", "user.favourites_count", "user.statuses_count",
                        "user.created_at", "user.geo_enabled", "user.lang", "coordinates", "place", "is_quote_status",
                        "retweet_count", "favorite_count", "entities", "favorited", "retweeted", "lang", "geocode"])


# Let's extract some tweets and save them in CSV
for geocode in geocodes:
    while max_id_tmp > min_id:  # the function will loop till the latest post we are allowed to obtain
        try:
            # specify search function
            for tweet in tweepy.Cursor(api.search, q=query, geocode=geocode, since_id=min_id, max_id=max_id_tmp).items(100):
                tmp_id.append(tweet.id)
                tmp_timestamp.append(tweet.created_at)
                csvWriter.writerow([tweet.created_at, tweet.id, tweet.id_str, tweet.text.encode("utf-8"),
                                    tweet.source.encode("utf-8"), tweet.truncated, tweet.in_reply_to_status_id,
                                    tweet.in_reply_to_status_id_str, tweet.in_reply_to_user_id, tweet.in_reply_to_user_id_str,
                                    tweet.in_reply_to_screen_name, tweet.user.id, tweet.user.id_str,
                                    tweet.user.name.encode("utf-8"), tweet.user.screen_name,
                                    tweet.user.location.encode("utf-8"), tweet.user.url, tweet.user.description.encode("utf-8"),
                                    tweet.user.verified, tweet.user.followers_count, tweet.user.friends_count,
                                    tweet.user.listed_count, tweet.user.favourites_count, tweet.user.statuses_count,
                                    tweet.user.created_at, tweet.user.geo_enabled, tweet.user.lang, tweet.coordinates,
                                    tweet.place, tweet.is_quote_status, tweet.retweet_count, tweet.favorite_count,
                                    tweet.entities, tweet.favorited, tweet.retweeted, tweet.lang, geocode])
            max_id_tmp_old = max_id_tmp - 1
            max_id_tmp = min(tmp_id) - 1  # make the id of oldest tweet of the 100 obtained the max for next batch
            print(str(len(tmp_id)) + " tweets added." + " New max ID = " + str(max_id_tmp) + ". Min ID = " + str(min_id)
                  + ". Tweets were made between: " + str(datetime_from_utc_to_local(min(tmp_timestamp)))
                  + " and " + str(datetime_from_utc_to_local(max(tmp_timestamp))))
            # put arrays back to empty, for next loop
            tmp_id = []
            tmp_timestamp = []
            # specify breaks
        except ValueError:
            print("No more new tweets, continue...")
            max_id_tmp = max_id
            break
        except tweepy.error.TweepError as e:
            if e.api_code == 500:
                timeout = 1000
                print("Twitter error response, status code = 500. Wait for 15 min.")
                continue
            elif e.api_code == 503:
                timeout = 1000
                print("Twitter error response, status code = 503. Wait for 15 min.")
                continue
            else:
                print(e.reason)
                break
print("Done")

csvFile.close()
