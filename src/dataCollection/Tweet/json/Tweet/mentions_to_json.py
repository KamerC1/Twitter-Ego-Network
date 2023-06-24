import tweepy
import json
import os
from typing import List

#!!Codice obsoleto, ora recupero i mention più velocemente con pyspark!!

def mentions_to_json(tweet_directory: str, save_directory) -> None:
    """
    Dati i tweet contenuti in tweet_directory, recupera le persone menzionate nei tweet
    e salva il risultato in un file json (i campi del file json sono spiegati nella funzione get_mentions_dict)

    :param tweet_directory: directory dove sono salvati i file json dei tweet
    :param save_directory: directory dove salvare i file json dei mentioning
    """

    if not os.path.isdir(save_directory):
        os.makedirs(save_directory)

    # Per ogni file, recupera dizionario dei mentioning
    mentions_list: List[List[dict]] = []
    for filename in os.scandir(tweet_directory):
        if filename.is_file():
            with open(filename, 'r') as file:
                tweets = json.load(file)
                mentions_list.extend(_get_tweets_mentions(tweets))

    # Salva i mentioning in un file json
    with open(f"{save_directory}/mentions.json", 'w+') as f:
        json.dump(mentions_list, f)

def _get_tweets_mentions(tweets: tweepy.tweet.Tweet) -> List[dict]:
    """
    Dato la lista di tweets, recupera, per ogni tweet, i mentioning

    :param tweets: lista di tweets, ogni elemento è un dizionario
    :return: lista di dizionari, ogni dizionario è un mentioning
    """

    mentions_list = []
    for tweet in tweets:
        if tweet is None:
            print("tweet is None")
            continue

        if 'entities' not in tweet: # a volte entities è null, quindi non è presente nel file json
            print("entities not in tweet")
            continue

        entities = tweet["entities"]
        if entities is None:
            continue
        if 'mentions' not in entities:
            continue

        for mentions in entities['mentions']:
            mentions_dict = _get_mentions_dict(tweet['id'], mentions)
            mentions_list.append(mentions_dict)

    return mentions_list

def _get_mentions_dict(tweet_id, mentions):
    """
    Restituisce dizionario con i dati del mentioning + id del tweet
    """

    return {
        'tweet_id': tweet_id,
        'username': mentions['username'],
        'user_id': mentions['id']
    }

def _get_reference_dict(tweet_id, mentions):
    """
    Restituisce dizionario con i dati del tweet a cui è stato fatto il retweet + id del retweet
    """

    return {
        'tweet_id': tweet_id,
        'username': mentions['username'],
        'user_id': mentions['id']
    }