import time
import datetime as dt
from datetime import datetime, timedelta
import tweepy
import sqlite3
import pandas as pd


def datetime_from_utc_to_local(utc_datetime):
    now_timestamp = time.time()
    offset = dt.datetime.fromtimestamp(now_timestamp) - dt.datetime.utcfromtimestamp(now_timestamp)
    return utc_datetime + offset


def retrieve_geocodes_from_db(database_file_name):
    geocodes_table = pd.read_sql_query("SELECT * FROM bounding_box", sqlite3.connect(database_file_name))
    longitude = geocodes_table['longitude'].tolist()
    latitude = geocodes_table['latitude'].tolist()
    geocode_radius = geocodes_table['range'].tolist()
    geocodes = []
    for i in range(0, len(longitude)):
        geocode = str(longitude[i]) + ',' + str(latitude[i]) + ',' + str(geocode_radius[i])
        geocodes.append(geocode)
    return geocodes


def find_geocode_id(geocode, database_file_name):
    geocodes_table = pd.read_sql_query("SELECT * FROM bounding_box", sqlite3.connect(database_file_name))
    longitude = geocodes_table['longitude'].tolist()
    latitude = geocodes_table['latitude'].tolist()
    geocode_radius = geocodes_table['range'].tolist()
    geocodes = []
    for i in range(0, len(longitude)):
        geocode_check = str(longitude[i]) + ',' + str(latitude[i]) + ',' + str(geocode_radius[i])
        geocodes.append(geocode_check)
    geocodes_table_add = pd.DataFrame({'geocode': geocodes})
    geocodes_table['geocode'] = geocodes_table_add['geocode']
    bounding_box_id = int(geocodes_table.loc[geocodes_table['geocode'] == str(geocode), 'bounding_box_id'].iloc[0])
    return bounding_box_id


def find_max_id(api, query):
    for tweet in tweepy.Cursor(api.search, q=query, geocode='51.216749,5.959129,78km').items(1):
        max_id_tmp = tweet.id
    return max_id_tmp


def find_min_id_api(api, query, geocode, days_back):
    tmp_min_id = []
    tmp_timestamp = []
    max_date = datetime.now() - timedelta(days=days_back)
    tweet_date = "{0}-{1:0>2}-{2:0>2}".format(max_date.year, max_date.month, max_date.day)

    for tweet in tweepy.Cursor(api.search, q=query, geocode=geocode, until=tweet_date).items(1):
        tmp_min_id.append(tweet.id)
        tmp_timestamp.append(tweet.created_at)
    if tmp_min_id == []:
        return [0, dt.datetime(2018, 1, 1, 0, 0, 0)]
    else:
        return max(tmp_min_id), max(tmp_timestamp)


def find_min_id_db(database_file_name):
    conn = sqlite3.connect(database_file_name)
    c = conn.cursor()
    c.execute("SELECT MAX (id) FROM 'tweets' WHERE is_ext_seach = 0")
    return c.fetchone()[0]


def save_user_to_db(c, user):
    c.execute(
        "INSERT OR IGNORE INTO 'users' ('id', 'id_str', 'name', 'screen_name', 'location', 'url', 'description',"
        "'is_verified', 'followers_count', 'friends_count', 'listed_count', 'favourites_count', 'statuses_count',"
        "'created_at', 'is_geo_enabled', 'lang') VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (user.id, user.id_str, user.name, user.screen_name, user.location, user.url, user.description, user.verified,
         user.followers_count, user.friends_count, user.listed_count, user.favourites_count, user.statuses_count,
         user.created_at, user.geo_enabled, user.lang))


def save_media_to_db(c, tweet):
    if tweet.entities.get('media'):
        number_of_media = list(range(len(tweet.entities.get('media'))))
        for i in number_of_media:
            media_display_url_i = tweet.entities.get('media')[i].get('display_url')
            media_expanded_url_i = tweet.entities.get('media')[i].get('expanded_url')
            media_id_i = tweet.entities.get('media')[i].get('id')
            media_id_str_i = tweet.entities.get('media')[i].get('id_str')
            media_indices_start_i = tweet.entities.get('media')[i].get('indices')[0]
            media_indices_end_i = tweet.entities.get('media')[i].get('indices')[1]
            media_media_url_i = tweet.entities.get('media')[i].get('media_url')
            media_media_url_https_i = tweet.entities.get('media')[i].get('media_url_https')
            media_source_status_id_i = tweet.entities.get('media')[i].get('source_status_id')
            media_source_status_id_str_i = tweet.entities.get('media')[i].get('source_status_id_str')
            media_type_i = tweet.entities.get('media')[i].get('type')
            media_url_i = tweet.entities.get('media')[i].get('url')
            c.execute(
                "INSERT OR IGNORE INTO 'media' ('display_url', 'expanded_url', 'media_id', 'media_id_str', "
                "'indices_start', 'indices_end', 'media_url', 'media_url_https', 'source_status_id', "
                "'source_status_id_str', 'type', 'url', 'tweet_id') "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (media_display_url_i, media_expanded_url_i, media_id_i, media_id_str_i,
                 media_indices_end_i, media_indices_start_i, media_media_url_i, media_media_url_https_i,
                 media_source_status_id_i, media_source_status_id_str_i, media_type_i, media_url_i, tweet.id))


def save_tweets_to_db(tweet, c, bounding_box_id):
    # Tweets Table
    try:
        quoted_status_id = tweet.quoted_status_id
        quoted_status_id_str = tweet.quoted_status_id_str
    except AttributeError:
        quoted_status_id = None
        quoted_status_id_str = None
    try:
        quoted_status = tweet.quoted_status.text
    except AttributeError:
        quoted_status = None
    try:
        retweeted_status = tweet.retweeted_status.text
    except AttributeError:
        retweeted_status = None
    c.execute("INSERT OR IGNORE INTO 'tweets' "
              "('created_at', 'id', 'id_str', 'text', 'source', 'is_truncated', 'in_reply_to_status_id', "
              "'in_reply_to_status_id_str', 'in_reply_to_user_id', 'in_reply_to_user_id_str',"
              "'in_reply_to_screen_name', 'user_id', 'quoted_status_id', 'quoted_status_id_str', 'is_quote_status',"
              "'quoted_status', 'retweeted_status', 'retweet_count', 'favorite_count', 'is_favorited', "
              "'is_retweeted', 'lang', 'bounding_box_id')"
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
              , (tweet.created_at, tweet.id, tweet.id_str, tweet.text, tweet.source, tweet.truncated,
                 tweet.in_reply_to_status_id, tweet.in_reply_to_status_id_str, tweet.in_reply_to_user_id,
                 tweet.in_reply_to_user_id_str, tweet.in_reply_to_screen_name, tweet.user.id,
                 quoted_status_id, quoted_status_id_str, tweet.is_quote_status, quoted_status, retweeted_status,
                 tweet.retweet_count, tweet.favorite_count, tweet.favorited, tweet.retweeted, tweet.lang,
                 bounding_box_id))
    # Users Table
    save_user_to_db(c, tweet.user)
    # Hashtags Table
    if tweet.entities.get('hashtags'):
        number_of_hashtags = list(range(len(tweet.entities.get('hashtags'))))
        for i in number_of_hashtags:
            hashtag_i = tweet.entities.get('hashtags')[i].get('text')
            hashtag_indices_start_i = tweet.entities.get('hashtags')[i].get('indices')[0]
            hashtag_indices_end_i = tweet.entities.get('hashtags')[i].get('indices')[1]
            c.execute(
                "INSERT OR IGNORE INTO 'hashtag' ('index_start', 'index_end', 'text', 'tweet_id') "
                "VALUES (?, ?, ?, ?)",
                (hashtag_indices_start_i, hashtag_indices_end_i, hashtag_i, tweet.id))
    # Media Table
    save_media_to_db(c, tweet)
    # URL Table
    if tweet.entities.get('urls'):
        number_of_urls = list(range(len(tweet.entities.get('urls'))))
        for i in number_of_urls:
            url_i = tweet.entities.get('urls')[i].get('url')
            url_display_i = tweet.entities.get('urls')[i].get('display_url')
            url_expanded_i = tweet.entities.get('urls')[i].get('expanded_url')
            url_indices_start_i = tweet.entities.get('urls')[i].get('indices')[0]
            url_indices_end_i = tweet.entities.get('urls')[i].get('indices')[1]
            c.execute("INSERT OR IGNORE INTO 'url' ('tweet_id', 'display_url', 'expanded_url', 'indices_start', "
                      "'indices_end', 'url') "
                      "VALUES (?, ?, ?, ?, ?, ?)",
                      (tweet.id, url_display_i, url_expanded_i, url_indices_start_i, url_indices_end_i, url_i))
    # user_mentions Table
    if tweet.entities.get('user_mentions'):
        number_of_user_mentions = list(range(len(tweet.entities.get('user_mentions'))))
        for i in number_of_user_mentions:
            user_mentions_id_i = tweet.entities.get('user_mentions')[i].get('id')
            user_mentions_id_str_i = tweet.entities.get('user_mentions')[i].get('id_str')
            user_mentions_indices_start_i = tweet.entities.get('user_mentions')[i].get('indices')[0]
            user_mentions_indices_end_i = tweet.entities.get('user_mentions')[i].get('indices')[1]
            user_mentions_name_i = tweet.entities.get('user_mentions')[i].get('name')
            user_mentions_screen_name_i = tweet.entities.get('user_mentions')[i].get('screen_name')
            c.execute("INSERT OR IGNORE INTO 'user_mention' ('tweet_id', 'user_mention_id', 'user_mention_id_str', "
                      "'indices_start', 'indices_end', 'name', 'screen_name') "
                      "VALUES (?, ?, ?, ?, ?, ?, ?)",
                      (tweet.id, user_mentions_id_i, user_mentions_id_str_i, user_mentions_indices_start_i,
                       user_mentions_indices_end_i, user_mentions_name_i, user_mentions_screen_name_i))
    # Place Table
    if tweet.entities.get('place'):
        c.execute("INSERT OR IGNORE INTO 'place' ('id', 'url', 'place_type', 'name', 'full_name', 'country_code', "
                  "'country') "
                  "VALUES (?, ?, ?, ?, ?, ?, ?)",
                  (tweet.entities.get('place').get('id'), tweet.entities.get('place').get('url'),
                   tweet.entities.get('place').get('place_type'), tweet.entities.get('place').get('name'),
                   tweet.entities.get('place').get('full_name'), tweet.entities.get('place').get('country_code'),
                   tweet.entities.get('place').get('country'),))
