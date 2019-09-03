import pandas as pd
import re
import string
import emoji
import sqlite3
# import numpy as np
# import nltk





# Functions to extract entities from tweets
def extract_hash_tags(s):
    hashtag_list = list(set(part[1:] for part in s.split() if part.startswith('#')))
    return ', '.join(hashtag_list)


def extract_at(s):
    at_list = list(set(part[1:] for part in s.split() if part.startswith('@')))
    return ', '.join(at_list)


def extract_url(s):
    url_list = list(set(part[:] for part in s.split() if part.startswith('http')))
    return ', '.join(url_list)


# !! Does not work -> emoji in csv saved as bytes, how is it stored in sqllite?
def extract_emoji(s):
    return ', '.join(c for c in s if c in emoji.UNICODE_EMOJI)


def strip_links(text):
    link_regex = re.compile('((https?):((//)|(\\\\))+([\w\d:#@%/;$()~_?\+-=\\\.&](#!)?)*)', re.DOTALL)
    links = re.findall(link_regex, text)
    for link in links:
        text = text.replace(link[0], ', ')
    return text


# Functions to strip entities from tweets
def strip_links(text):
    link_regex = re.compile('((https?):((//)|(\\\\))+([\w\d:#@%/;$()~_?\+-=\\\.&](#!)?)*)', re.DOTALL)
    links = re.findall(link_regex, text)
    for link in links:
        text = text.replace(link[0], ', ')
    return text


def strip_all_entities(text):
    entity_prefixes = ['@', '#']
    for seperator in string.punctuation:
        if seperator not in entity_prefixes:
            text = text.replace(seperator, ' ')
    words = []
    for word in text.split():
        word = word.strip()
        if word:
            if word[0] not in entity_prefixes:
                words.append(word)
    return ' '.join(words)


# Read data from csv file
data = pd.read_csv('tweets_tryout.csv', sep=';')
print(data[['text']].head(n=5))

# delete first two characters (i.e. b' or b") and last character (i.e. ' or ")
data['text'] = data['text'].str[2:-1]
print(data[['text']].head(n=5))

# Extract # and add them to a column
data['extracted_hashtags'] = data['text'].apply(lambda x: extract_hash_tags(x))
print(data[['text', 'extracted_hashtags']].head(n=30))

# Extract @ and add them to a column
data['extracted_at'] = data['text'].apply(lambda x: extract_at(x))
print(data[['text', 'extracted_at']].head(n=30))

# Extract url and add them to a column
data['extracted_url'] = data['text'].apply(lambda x: extract_url(x))
print(data[['text', 'extracted_url']].head(n=30))

# Extract emoji and add them to a column
# !! Does not yet work
data['extracted_emoji'] = data['text'].apply(lambda x: extract_emoji(x))
print(data[['text', 'extracted_emoji']].head(n=30))

# Make all tweets lower case
data['text'] = data['text'].apply(lambda x: " ".join(x.lower() for x in x.split()))
print(data[['text']].head(n=5))

# Strip tweets from entities (# and @) and punctuation
data['text'] = data['text'].apply(lambda x: strip_all_entities(strip_links(x)))
print(data[['text']].head(n=10))

data.to_csv('tweets_tryout_clean.csv', sep=';')