import os
from typing import List

import tweepy

def get_client() -> tweepy.Client:
    os.environ['TOKEN'] = 'HIDDEN'  # token per research account
    # os.environ['TOKEN'] = 'HIDDEN' # token per account personale

    bearer_token = os.getenv('TOKEN')
    client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)
    return client

def get_user(starting_user_id, num_min_total_user, max_results, limit):
    """
    Recupera parte dei following di starting_user_id,
    successivamente vengono recuperati i following di questi utenti, e così via per ogni utente recuperato
    (spiegazione qui: https://www.notion.so/Snowball-7faf5691e62448a688ca3758049370ec#c0a9aea89b9144c8af19dd0ca20f5968)

    :param starting_user_id: id dell'utente da cui partire
    :param num_min_total_user: numero minimo di utenti da recuperare
    :param max_results: numero massimo di utenti da recuperare per ogni chiamata
    :param limit: numero massimo di chiamate da fare
    :return: lista di utenti senza duplicati
    """
    client = get_client()
    user_list = _get_following_from_user(user_id=starting_user_id, max_results=max_results, limit=limit, client=client)

    for user in user_list:
        temp_user_list = _get_following_from_user(user_id=user.id, max_results=max_results, limit=limit, client=client)
        user_list.extend(temp_user_list)

        if len(user_list) >= num_min_total_user:
            break

    return _remove_duplicate(user_list)




def _get_following_from_user(user_id: int, max_results, limit, client: tweepy.Client) -> List[tweepy.user.User]:
    """
    Restituisc i following di user_id (i.e. le persone che seguono user_id)
    """

    user_list = []
    all_responses = tweepy.Paginator(client.get_users_following, id=user_id,
                                     user_fields=None, max_results=max_results, limit=limit)

    if all_responses is None:
        return user_list

    for response in all_responses:
        if response.data is None:
            print(response.errors)
            print("___________")
            return user_list

        for user in response.data:
            user_list.append(user)

    return user_list


def _remove_duplicate(user_list: List[tweepy.user.User]) -> List[tweepy.user.User]:
    """
    Rimuove i duplicati dalla lista user_list
    """

    user_unique_list = []
    for user in user_list:
        if not _is_user_present(user_unique_list, user.id):
            user_unique_list.append(user)

    return user_unique_list

def _is_user_present(user_list: List[tweepy.user.User], user_id) -> bool:
    """
    :return: True se user_id è presente in user_list, False altrimenti
    """

    for user in user_list:
        if user.id == user_id:
            return True

    return False
