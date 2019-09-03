import pandas as pd
import re
import emoji
import sqlite3
import nltk
from nltk.stem.snowball import SnowballStemmer
from sqlalchemy import create_engine
from langid.langid import LanguageIdentifier, model
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException
import datetime
# nltk.download('stopwords')
# nltk.download('wordnet')


# Functions to extract entities from tweets
def extract_hash_tags(j):
    hashtag_list = list(hashtag_table.text[hashtag_table.tweet_id == tweets_table.loc[j, 'id']])
    return ', '.join(hashtag_list)


def extract_at(j):
    ats_list = list(user_table.screen_name[user_table.tweet_id == tweets_table.loc[j, 'id']])
    return ', '.join(ats_list)


def extract_url(text):
    url_list = list(set(part[:] for part in text.split() if part.startswith('http')))
    # url_list = list(url_table.url[url_table.tweet_id == tweets_table.loc[j, 'id']])
    return ', '.join(url_list)


def extract_emoji(text):
    return ', '.join(c for c in text if c in emoji.UNICODE_EMOJI)


# Functions to strip entities from tweets
def strip_links(text):
    link_regex = re.compile('((https?):((//)|(\\\\))+([\w\d:#@%/;$()~_?+-=\\\.&](#!)?)*)', re.DOTALL)
    links = re.findall(link_regex, text)
    # links = list(url_table.url[url_table.tweet_id == tweets_table.loc[j, 'id']])
    for link in links:
        text = text.replace(link[0], ', ')
    return text


def strip_emoji(text):
    emoji_list = [c for c in extract_emoji(text)]
    return ' '.join([str for str in text.split() if not any(i in str for i in emoji_list)])


def strip_ats(text, j):
    ats_list = list(user_table.screen_name[user_table.tweet_id == tweets_table.loc[j, 'id']])
    return ' '.join([str for str in text.split() if not any(i in str for i in ats_list)])


def strip_hashtag(text, j):
    hashtag_list = list(hashtag_table.text[hashtag_table.tweet_id == tweets_table.loc[j, 'id']])
    return ' '.join([str for str in text.split() if not any(i in str for i in hashtag_list)])


def strip_number(text):
    return ''.join(i for i in text if not i.isdigit())


def strip_special(text):
    return re.sub('[^\w ]|_', ' ', text)


def freq(text):
    frequency = pd.Series(''.join(str(text)).split()).value_counts()[:10]
    return list(frequency.index)


# Function to clean grammatical form
def lang(language):
    if language == 'en':
        return "english"
    elif language == "nl":
        return "dutch"
    else:
        return "english"


def stop(language):
    return nltk.corpus.stopwords.words(language)


def save_tweet(id_tweet, tweet, retweet):
    return cursor.execute("REPLACE INTO cleaned_tweets (id, clean_tweets, clean_retweets)"
                          "VALUES(?, ?, ?)", (id_tweet, tweet, retweet))
def save_emoji(id_tweet, emoji_tweet):
    return cursor.execute("REPLACE INTO emoji(id, emoji) VALUES(?, ?)", (id_tweet, emoji_tweet))

def save_lang(id_tweet, lang):
    return cursor.execute("REPLACE INTO lang(id, lang) VALUES(?, ?)", (id_tweet, lang))

def clean_text(text, j):
    if pd.notnull(text):
        text = strip_links(text)
        text = strip_hashtag(text, j)
        text = strip_ats(text, j)
        text = strip_emoji(text)
        text = strip_special(text)
        text = strip_number(text)
        text = text.lower()
        text = re.sub(' +', ' ', text)
    return text


def stem_text(text, language):
    text = " ".join(word for word in text.split() if len(word) > 2)
    text = " ".join(word for word in text.split() if word not in stop(lang(language)))
    text = " ".join(SnowballStemmer(lang(language)).stem(word)for word in text.split())
    text = re.sub(' +', ' ', text)
    return text


def identify_language(text):
    identifier = LanguageIdentifier.from_modelstring(model, norm_probs=True)
    identified_language = identifier.classify(text)[0]
    identified_language_prob = identifier.classify(text)[1]
    # , identified_language_prob
    return identified_language


# Path to database
database_file_name = 'C:\\Users\Poramapa\PycharmProjects\scrapelimburg\data\db_tweets_full.sqlite'

# Read data from db file
conn = sqlite3.connect(database_file_name)
cursor = conn.cursor()
print("Opened database successfully")

# create cleaned_data table
try:
    conn.execute('''CREATE TABLE `cleaned_tweets` (
    `id`	INTEGER NOT NULL UNIQUE,
    'clean_tweets' TEXT,
    'clean_retweets' TEXT,
    'stem_tweets' TEXT);''')
except sqlite3.OperationalError as e:
    print('sqlite error:', e.args[0])

try:
    conn.execute('''CREATE TABLE `emoji` (
    `id`	INTEGER NOT NULL UNIQUE,
    'emoji' TEXT);''')
except sqlite3.OperationalError as e:
    print('sqlite error:', e.args[0])

try:
    conn.execute('''CREATE TABLE `lang` (
    `id`	INTEGER NOT NULL UNIQUE,
    'lang' TEXT);''')
except sqlite3.OperationalError as e:
    print('sqlite error:', e.args[0])

engine = create_engine('sqlite:///%s' % database_file_name, echo=False)

# Prepare tables and clean duplicate
print('prepare tables')
tweets_table = pd.read_sql_query("SELECT created_at, id, text, is_truncated, user_id, retweeted_status "
                                 "FROM tweets WHERE id NOT IN (SELECT id FROM cleaned_tweets)", conn)

print(str(len(tweets_table.text)) + " new tweets to be cleaned.")

hashtag_table = pd.read_sql_query("SELECT tweet_id, index_start, index_end, text FROM hashtag"
                                  " WHERE tweet_id NOT IN (SELECT id FROM cleaned_tweets)", conn)
hashtag_table = hashtag_table.sort_values('tweet_id')
hashtag_table = hashtag_table.drop_duplicates(keep='first')

user_table = pd.read_sql_query("SELECT * FROM user_mention WHERE tweet_id NOT IN (SELECT id FROM cleaned_tweets)", conn)
user_table = user_table.sort_values('tweet_id')
user_table = user_table.drop_duplicates(keep='first')

url_table = pd.read_sql_query("SELECT tweet_id, display_url, expanded_url, indices_start, indices_end, url "
                              "FROM url WHERE tweet_id NOT IN (SELECT id FROM cleaned_tweets)", conn)
url_table = url_table.sort_values('tweet_id')
url_table = url_table.drop_duplicates(keep='first')

# Identifying language
# print('identifying languages')
# tweets_table.apply(identify_language, args=(tweets_table.id, tweets_table.text))

# Cleaning Tweets
print('start cleaning at ', datetime.datetime.now())
printcounter = 0
k = 0

while k < len(tweets_table.created_at):

    printcounter += 1
    if printcounter == 10000:
        print("10000 tweets cleaned at ", datetime.datetime.now())
        printcounter = 0

    tweet_id = int(tweets_table.id[k])
    try:
        language = detect(tweets_table.text[k])
    except LangDetectException:
        language = 'unidentified'

    clean_tweets = clean_text(tweets_table.text[k], k)
    clean_retweets = clean_text(tweets_table.retweeted_status[k], k)
    my_emoji = extract_emoji(tweets_table.text[k])

    # insert data into sql
    save_lang(tweet_id, language)
    save_tweet(tweet_id, clean_tweets, clean_retweets)

    if len(my_emoji) > 0:
        save_emoji(tweet_id, my_emoji)

    k = k + 1

print("Finished cleaning tweets. Continuing with stemming.")

# Next, we get rid of stop words and stem
cleaned_tweets_table = pd.read_sql_query("SELECT id, clean_tweets FROM cleaned_tweets WHERE stem_tweets IS NULL ", conn)
language_tweets_table = pd.read_sql_query("SELECT id, lang FROM lang ", conn)

printcounter = 0
k = 0
while k < len(cleaned_tweets_table.clean_tweets):
    printcounter += 1
    if printcounter == 10000:
        print("10000 tweets stemmed at ", datetime.datetime.now())
        printcounter = 0
    tweet_id = int(cleaned_tweets_table.id[k])
    tweet_lang = str(language_tweets_table.loc[language_tweets_table['id'] == tweet_id, 'lang'].iloc[0])
    if ((tweet_lang == 'en') or (tweet_lang == 'nl')) and (len(cleaned_tweets_table.clean_tweets[k]) > 2):
        stemmed_tweet = stem_text(cleaned_tweets_table.clean_tweets[k], tweet_lang)
        cursor.execute("UPDATE cleaned_tweets SET stem_tweets=? WHERE id=?", (stemmed_tweet, tweet_id))
        conn.commit()
        k += 1
    else:
        k += 1
        continue

print('finished at ', datetime.datetime.now())
conn.commit()
cursor.close()
conn.close()
