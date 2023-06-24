import json
import os
from typing import List

import tweepy as tweepy
from typing import Union

def data_paginator_to_json(paginator: tweepy.pagination.Paginator, directory: str) -> None:
    """
    Dato un "paginator", salva tutti i dati in un file json

    :param paginator: paginator di tweepy
    :param directory: directory dove salvare i files
    """

    if not os.path.isdir(directory):
        os.makedirs(directory)

    count = 1
    for response in paginator:
        if response.data is None:
            continue
        _data_list_to_json(response.data, f"{directory}/file_{count}")
        count += 1

def _data_list_to_json(data_list: List[Union[tweepy.tweet.Tweet, tweepy.user.User]], directory: str) -> None:
    """
    Salva lista tweet_list in json

    :param data_list: lista di tweepy.tweet.Tweet
    :param directory: : directory dove salvare il file
    """

    tweet_dict_list = []
    for tweet in data_list:
        tweet_dict = _get_data_dict(tweet)
        tweet_dict_list.append(tweet_dict)

    with open(f"{directory}.json", 'w+') as f:
        json.dump(tweet_dict_list, f)



def _get_data_dict(data: Union[tweepy.tweet.Tweet, tweepy.user.User]) -> dict:
    """
    :return: dato un tweet o un user, restituisce un dizionario con tutti i dati
    """

    if type(data) == tweepy.tweet.Tweet:
        return _get_tweet_dict(data)
    elif type(data) == tweepy.user.User:
        return _get_user_dict(data)


def _get_tweet_dict(data: tweepy.tweet.Tweet) -> dict:
    """
    Converte data in dict. I campi null non vengono aggiunti al dizionario

    :param data: tweet da convertire
    :return: dizionario con i dati del tweet
    """

    if data.referenced_tweets is not None:
        referenced_tweets_dict = _get_referenced_tweets_dict(data.referenced_tweets)
    else:
        referenced_tweets_dict = None

    dict = {}
    if data.in_reply_to_user_id is not None:
        dict["in_reply_to_user_id"] = data.in_reply_to_user_id
    if data.text is not None:
        dict["text"] = data.text
    if data.created_at is not None:
        dict["created_at_utc"] = int(data.created_at.replace().timestamp()) # converte created_at in UTC
    if data.author_id is not None:
        dict["author_id"] = data.author_id
    if data.id is not None:
        dict["id"] = data.id
    if data.public_metrics is not None:
        dict["public_metrics"] = data.public_metrics
    if data.referenced_tweets is not None:
        dict["referenced_tweets"] = referenced_tweets_dict
    if data.lang is not None:
        dict["lang"] = data.lang
    if data.source is not None:
        dict["source"] = data.source
    if data.conversation_id is not None:
        dict["conversation_id"] = data.conversation_id
    if data.reply_settings is not None:
        dict["reply_settings"] = data.reply_settings
    if data.geo is not None:
        dict["geo"] = data.geo
    if data.context_annotations is not None and len(data.context_annotations) > 0:
        dict["context_annotations"] = data.context_annotations
    if data.attachments is not None:
        dict["attachments"] = data.attachments
    if data.entities is not None:
        dict["entities"] = data.entities
    if data.possibly_sensitive is not None:
        dict["possibly_sensitive"] = data.possibly_sensitive
    if data.withheld is not None:
        dict["withheld"] = data.withheld
    # if data.edit_controls is not None: TODO non so perché non funziona (edit_controls non è presente in data)
    #     dict["edit_controls"] = data.edit_controls
    if data.edit_history_tweet_ids is not None:
        dict["edit_history_tweet_ids"] = data.edit_history_tweet_ids

    return dict



def _get_referenced_tweets_dict(referenced_tweets: List[tweepy.tweet.ReferencedTweet]) -> List[dict]:
    """
    :param referenced_tweets: lista contenente i tweet citati (spiegato qui: https://www.notion.so/Scaricare-dati-da-Twitter-3756b27d07b8431da7821ba981bf7a38#2250c96886bf480abe7fbb7dbf4390cf)
    :return: lista di dizionari con i dati dei referenced tweets
    """

    referenced_tweets_dict_list = []
    for referenced_tweet in referenced_tweets:
        tweet_dict = {
            "referenced_tweet_id": referenced_tweet.id,
            "referenced_tweet_type": referenced_tweet.type,
        }
        referenced_tweets_dict_list.append(tweet_dict)

    return referenced_tweets_dict_list


# TODO vedere se sono stati aggiunti tutti i campi recuperati da tweepy
def _get_user_dict(data: tweepy.user.User) -> dict:
    dict = {}
    if data.id is not None:
        dict["id"] = data.id
    if data.username is not None:
        dict["username"] = data.username
    if data.name is not None:
        dict["name"] = data.name
    if data.created_at is not None:
        dict["created_at_utc"] = int(data.created_at.replace().timestamp())
    if data.description is not None:
        dict["description"] = data.description
    if data.location is not None:
        dict["location"] = data.location
    if data.protected is not None:
        dict["protected"] = data.protected
    if data.verified is not None:
        dict["verified"] = data.verified
    if data.public_metrics is not None:
        if data.public_metrics["followers_count"] is not None:
            dict["followers_count"] = data.public_metrics["followers_count"]
        if data.public_metrics["following_count"] is not None:
            dict["following_count"] = data.public_metrics["following_count"]
        if data.public_metrics["tweet_count"] is not None:
            dict["tweet_count"] = data.public_metrics["tweet_count"]
        if "listed_count" in data.public_metrics and data.public_metrics["listed_count"] is not None:
            dict["listed_count"] = data.public_metrics["listed_count"]

    return dict