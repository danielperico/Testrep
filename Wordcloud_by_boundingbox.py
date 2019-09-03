from wordcloud import WordCloud, STOPWORDS
import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
import math
import numpy as np
from PIL import Image

sqlite_file = 'C:\\Users\Poramapa\PycharmProjects\scrapelimburg\data\db_tweets_27Jan.sqlite'
conn = sqlite3.connect(sqlite_file)
cursor = conn.cursor()

central1 = [32, 16, 25]
central2 = [14, 33, 21, 34]
other_south = [2, 3, 4, 5, 6, 7, 8, 27 , 31, 11, 10, 12, 18, 22]
north1 = [36, 15, 17, 24]
north2 = [19, 20, 35]

central1_word = ' '
central2_word = ' '
other_south_word = ' '
north1_word = ' '
north2_word = ' '

central1_count = 0
central2_count = 0
other_south_count = 0
north1_count = 0
north2_count = 0

for box_id in range(1,37):
    print('****', box_id, '****')
    # hash_table = pd.read_sql_query("select tweets.bounding_box_id, group_concat(hashtag.text, ' ')"
    #                                "from tweets inner join hashtag on tweets.id = hashtag.tweet_id group by tweets."
    #                                "bounding_box_id", conn)

    cleaned_tweets_table = pd.read_sql_query("SELECT id, stem_tweets FROM cleaned_tweets WHERE stem_tweets IS not NULL "
                                             "and clean_retweets is NULL and id in(select id from tweets where (lang = 'en' "
                                             "and bounding_box_id = %s))" % box_id, conn)
    cleaned_retweets_table = pd.read_sql_query("SELECT distinct group_concat(id), stem_tweets, count(distinct id) FROM cleaned_tweets WHERE "
                                               "cleaned_tweets.stem_tweets IS not NULL and cleaned_tweets.clean_retweets "
                                               "IS not NULL and id in(select id from tweets where (lang = 'en' and bounding_box_id = %s)) "
                                               "group by stem_tweets" % box_id, conn)
    # bounding_box = pd.read_sql_query("select * from bounding_box", conn)
    # hash_table = hash_table.rename(columns={"group_concat(hashtag.text, ' ')": "hashtag", })
    cleaned_retweets_table = cleaned_retweets_table.rename(columns={"group_concat(id)": "id", "count(distinct id)":"count"})

    print('start weighting retweets for', len(cleaned_retweets_table.stem_tweets), ' retweets')
    k=1
    loop = len(cleaned_retweets_table.stem_tweets)
    while k < loop :
        adding = int(round(math.log(cleaned_retweets_table.loc[int(k), 'count'])+1,0))
        i=0
        while i < adding:
            cleaned_retweets_table = cleaned_retweets_table.append(cleaned_retweets_table.iloc[k], ignore_index=True)
            i=i+1
        k+=1


    tweets_table = pd.concat([cleaned_retweets_table,cleaned_tweets_table], join="inner")
    count_tweet = len(tweets_table.stem_tweets)
    print('total tweets', count_tweet)

    if box_id in central1:
        central1_count = central1_count + count_tweet
    elif box_id in central2:
        central2_count = central2_count + count_tweet
    elif box_id in north1:
        north1_count = north1_count + count_tweet
    elif box_id in north2:
        north2_count = north2_count + count_tweet
    elif box_id in other_south:
        other_south_count = other_south_count + count_tweet

    stopwords = set(STOPWORDS)

    if count_tweet > 0 :
        comment_words = ' '
        for val in tweets_table.stem_tweets:

            # typecaste each val to string
            val = str(val)

            # split the value
            tokens = val.split()

            # Converts each token into lowercase
            for i in range(len(tokens)):
                tokens[i] = tokens[i].lower()

            for words in tokens:
                if box_id in central1:
                    central1_word = central1_word + words + ' '
                elif box_id in central2:
                    central2_word = central2_word + words + ' '
                elif box_id in north1:
                    north1_word = north1_word + words + ' '
                elif box_id in north2:
                    north2_word = north2_word + words + ' '
                elif box_id in other_south:
                    other_south_word = other_south_word + words + ' '
                else:
                    comment_words = comment_words + words + ' '


        if len(comment_words) > 1:
            mask_path = str('C://Users/Poramapa/PycharmProjects/scrapelimburg/bounding_box_map/' + str(box_id) + '.gif')
            mask = np.array(Image.open(mask_path))

            wordcloud = WordCloud(width=1000, height=800, collocations=False,
                                      background_color='white', stopwords=stopwords,
                                      min_font_size=10, mask=mask).generate(comment_words)

            # plot the WordCloud image
            plt.figure(figsize=(10, 10), facecolor=None)
            plt.imshow(wordcloud)
            plt.axis("off")
            plt.tight_layout(pad=0)
            plt.savefig(
                    'C://Users/Poramapa/PycharmProjects/scrapelimburg/bounding_box1/bounding_box' + str(box_id) + '_' + str(count_tweet) + '.png')
            plt.close("all")

#central1
mask_path = str('C://Users/Poramapa/PycharmProjects/scrapelimburg/bounding_box_map/central1.gif')
mask = np.array(Image.open(mask_path))

wordcloud = WordCloud(width=1000, height=800, collocations=False,
                      background_color='white', stopwords=stopwords,
                      min_font_size=10, mask=mask).generate(central1_word)

# plot the WordCloud image
plt.figure(figsize=(10, 10), facecolor=None)
plt.imshow(wordcloud)
plt.axis("off")
plt.tight_layout(pad=0)
plt.savefig('C://Users/Poramapa/PycharmProjects/scrapelimburg/bounding_box1/bounding_box_central1_' + str(central1_count) + '.png')
plt.close("all")

#central2
mask_path = str('C://Users/Poramapa/PycharmProjects/scrapelimburg/bounding_box_map/central2.gif')
mask = np.array(Image.open(mask_path))

wordcloud = WordCloud(width=1000, height=800, collocations=False,
                      background_color='white', stopwords=stopwords,
                      min_font_size=10, mask=mask).generate(central2_word)

# plot the WordCloud image
plt.figure(figsize=(10, 10), facecolor=None)
plt.imshow(wordcloud)
plt.axis("off")
plt.tight_layout(pad=0)
plt.savefig('C://Users/Poramapa/PycharmProjects/scrapelimburg/bounding_box1/bounding_box_central2_' + str(central2_count) + '.png')
plt.close("all")

#north1
mask_path = str('C://Users/Poramapa/PycharmProjects/scrapelimburg/bounding_box_map/north1.gif')
mask = np.array(Image.open(mask_path))

wordcloud = WordCloud(width=1000, height=800, collocations=False,
                      background_color='white', stopwords=stopwords,
                      min_font_size=10, mask=mask).generate(north1_word)

# plot the WordCloud image
plt.figure(figsize=(10, 10), facecolor=None)
plt.imshow(wordcloud)
plt.axis("off")
plt.tight_layout(pad=0)
plt.savefig('C://Users/Poramapa/PycharmProjects/scrapelimburg/bounding_box1/bounding_box_north1_' + str(north1_count) + '.png')
plt.close("all")

#north2
mask_path = str('C://Users/Poramapa/PycharmProjects/scrapelimburg/bounding_box_map/north2.gif')
mask = np.array(Image.open(mask_path))

wordcloud = WordCloud(width=1000, height=800, collocations=False,
                      background_color='white', stopwords=stopwords,
                      min_font_size=10, mask=mask).generate(north2_word)

# plot the WordCloud image
plt.figure(figsize=(10, 10), facecolor=None)
plt.imshow(wordcloud)
plt.axis("off")
plt.tight_layout(pad=0)
plt.savefig('C://Users/Poramapa/PycharmProjects/scrapelimburg/bounding_box1/bounding_box_north2_' + str(north2_count) + '.png')
plt.close("all")

#other_south
mask_path = str('C://Users/Poramapa/PycharmProjects/scrapelimburg/bounding_box_map/other_south.gif')
mask = np.array(Image.open(mask_path))

wordcloud = WordCloud(width=1000, height=800, collocations=False,
                      background_color='white', stopwords=stopwords,
                      min_font_size=10, mask=mask).generate(other_south_word)

# plot the WordCloud image
plt.figure(figsize=(10, 10), facecolor=None)
plt.imshow(wordcloud)
plt.axis("off")
plt.tight_layout(pad=0)
plt.savefig('C://Users/Poramapa/PycharmProjects/scrapelimburg/bounding_box1/bounding_box_other_south_' + str(other_south_count) +'.png')
plt.close("all")


cursor.close()
conn.close()
