import json
import os
import tweepy
from typing import List

def get_client() -> tweepy.Client:
    research_token = 'HIDDEN'
    personal_token = 'HIDDEN'

    client = tweepy.Client(bearer_token=research_token, wait_on_rate_limit=True)
    return client
client = get_client()

def get_users_followers(user_id: int, max_results, limit) -> tweepy.Paginator:
    """
        Recupera i followers dell'utente (i.e. le persone che seguono user_id) e li salva in un file json.
        :param user_id: user_id dell'utente
        :param max_results: numero massimo di risultati da recuperare per richiesta
        :param limit: numero massimo di richieste da fare
        :return: paginator
    """

    user = client.get_user(id=user_id).data

    # all_user_fields = ['created_at', 'description',
    #                'location', 'pinned_tweet_id', 'profile_image_url',
    #                'verified', 'verified_type', 'entities', 'public_metrics',
    #                'protected', 'url', 'withheld']

    selected_user_fields = ['created_at', 'public_metrics']

    all_responses = tweepy.Paginator(client.get_users_followers, id=user.id,
                                     user_fields=selected_user_fields, max_results=max_results, limit=limit)
    return all_responses

def get_users_following(user_id: int, max_results, limit) -> tweepy.Paginator:
    """
        Recupera i followers dell'utente (i.e. le persone che seguono user_id) e li salva in un file json.
        :param user_id: user_id dell'utente
        :param max_results: numero massimo di risultati da recuperare per richiesta
        :param limit: numero massimo di richieste da fare
        :return: paginator
    """

    user = client.get_user(id=user_id).data

    # all_user_fields = ['created_at', 'description',
    #                'location', 'pinned_tweet_id', 'profile_image_url',
    #                'verified', 'verified_type', 'entities', 'public_metrics',
    #                'protected', 'url', 'withheld']

    selected_user_fields = ['created_at', 'public_metrics']

    all_responses = tweepy.Paginator(client.get_users_following, id=user.id,
                                     user_fields=selected_user_fields, max_results=max_results, limit=limit)
    return all_responses
