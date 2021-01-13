#! usr/bin/env python3

import praw
from praw.models import MoreComments
import pandas as pd
import datetime as dt


# ----------------- AUTHENTICATION -----------------------------

# Establishes the credentials for this study and announces that you will be scraping the site

reddit = praw.Reddit(client_id='[insert_client_id]',
                     client_secret = '[insert_user_agent]',
                     user_agent = '[insert_title_of_crawler] (by /u/[insert_user_name])',
                     username = 'username',
                     password = 'password',
                     refresh_token = '[insert_refresh_token]'
                     )


# Establish a dictionary of fields that will be included in your dataset
topics_dict = { "title":[],"score":[],"id":[], "url":[],"comms_num":[],"created":[],"body":[]}

# Dictionary of subreddits that you would like to scrape
# My research interests lie in scraping the Mental Health / Depression oriented subreddits
subs = {1:'BPD', 2:'cripplingalcoholism', 3:'opiates', 4:'suicidewatch', 5:'depression', 6:'anxiety', 7:'bipolar', 8:'mentalhealth'}


# ----------------- DATA PULL -----------------------------

# Iterates over dictionary of subreddits (subs)
for i in subs:
    subreddit = reddit.subreddit(subs[i])
    hot_subreddit = subreddit.hot(limit = 1000)

    # Set your output file here. The subs[i] will update the file name based on your dictionary position
    topic_output_filename = f'/Users/{subs[i]}_topic_reddit.csv'
    comm_output_filename = f'/Users/{subs[i]}_comment_reddit.csv'

    for submission in hot_subreddit:
        topics_dict["title"].append(submission.title)
        topics_dict["score"].append(submission.score)
        topics_dict["id"].append(submission.id)
        topics_dict["url"].append(submission.url)
        topics_dict["comms_num"].append(submission.num_comments)
        topics_dict["created"].append(submission.created)
        topics_dict["body"].append(submission.selftext)

    topics_data = pd.DataFrame(topics_dict)


    def get_date(submission):
        time = submission
        return dt.datetime.fromtimestamp(time)


    timestamps = topics_data["created"].apply(get_date)

    topics_data = topics_data.assign(timestamp = timestamps)

    comms_dict = { "topic":[], "body":[], "comm_id":[], "created":[] }

    iteration = 1
    for topic in topics_data["id"]:
        print(str(iteration))
        iteration += 1
        submission = reddit.submission(id=topic)
        for top_level_comment in submission.comments:
            if isinstance(top_level_comment, MoreComments):
                continue
            comms_dict["topic"].append(topic)
            comms_dict["body"].append(top_level_comment.body)
            comms_dict["comm_id"].append(top_level_comment)
            comms_dict["created"].append(top_level_comment.created)

    comms_data = pd.DataFrame(comms_dict)

    timestamps = comms_data["created"].apply(get_date)
    comms_data = comms_data.assign(timestamp = timestamps)

    pd.DataFrame(topics_data).to_csv(topic_output_filename, encoding='utf-8')
    pd.DataFrame(comms_data).to_csv(comm_output_filename, encoding='utf-8')
