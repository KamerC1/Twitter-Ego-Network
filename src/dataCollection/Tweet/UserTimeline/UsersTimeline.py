import os
from typing import List
import tweepy as tweepy

from src.dataCollection.Tweet.json.data_to_json import data_paginator_to_json
from src.utils.utils import write_log


class UsersTimeline:

    def __init__(self, users_id: List[int], user_api: str, log_file_path, save_dir):
        """

        :param users_id: id degli utenti di cui dobbiamo recuperare la timeline
        :param user_api: Proprietario delle api (può essere: Andrea, Chiara, Kamer) "
        :param log_file_path: percorso del file di log
        :param save_dir: directory dove salvare i file json
        """
        self.user_id: List[int] = users_id

        self.client = self._get_twitterClient(user_api)
        self.start_time = "2015-03-01T00:00:00Z"
        # self.start_time = "2018-03-01T00:00:00Z"
        self.end_time = "2022-04-01T00:00:00Z"
        self.max_numTweet = 100000000 #ho messo un valore altissimo così raccoglie tutti i tweet
        self.log_file_path = log_file_path
        self.save_dir = save_dir

        self.tweet_requested_fields = ['created_at', 'in_reply_to_user_id', 'entities', "referenced_tweets"]

    def saveUsersTimeline(self):
        """
        Salva i tweet degli utenti come file json utilizzando la funzione search_all_tweets (RICHIEDE API ACADEMIC)
        """
        
        write_log(self.log_file_path, f"=========Inizio download===========")

        for user_id in self.user_id:
            self._saveUserTweets(user_id, self._getTimeline)

        write_log(self.log_file_path, f"=========Fine download===========")

    def saveUsersRecentTimeline(self):
        """
        Salva i tweet degli utenti come file json utilizzando la funzione search_recent_tweets (NON RICHIEDE API ACADEMIC)
        """
        
        write_log(self.log_file_path, f"=========Inizio download===========")

        for user_id in self.user_id:
            self._saveUserTweets(user_id, self._getRecenteTimeline)

        write_log(self.log_file_path, f"=========Fine download===========")

    def _saveUserTweets(self, user_id: int, tweetFunction):
        """
        Salva i tweet dell'utente user_id come file json
        """
        saveTweet_dir = f"{self.save_dir}/{user_id}/tweet"

        if os.path.exists(saveTweet_dir):
            write_log(self.log_file_path, f"User {user_id} already exists")
            return

        try:
            write_log(self.log_file_path, f"Scaricamento timeline di {user_id}")

            paginator = tweetFunction(user_id)
            data_paginator_to_json(paginator, saveTweet_dir)

            write_log(self.log_file_path, f"Scaricamento timeline di {user_id} completato")
        except Exception as e:
            write_log(self.log_file_path, f"Errore scaricamento timeline di {user_id}: {str(e)}")

    def _getTimeline(self, user_id: int):
        """
        Recupera timeline dell'utente user_id attraverso la funzione search_all_tweets (RICHIEDE API ACADEMIC)
        :param user_id: utente di cui recuperare la timeline (deve essere solo l'id numerico)
        """

        max_result = 500
        limit = self.max_numTweet / max_result
        return tweepy.Paginator(self.client.search_all_tweets,
                                query=f"from:{user_id}", tweet_fields=self.tweet_requested_fields,
                                start_time=self.start_time, end_time=self.end_time,
                                max_results=max_result, limit=limit)

    def _getRecenteTimeline(self, user_id: int):
        """
        Recupera timeline dell'utente user_id attraverso la funzione get_users_tweets (NON RICHIEDE API ACADEMIC)
        :param user_id: utente di cui recuperare la timeline (deve essere solo l'id numerico)
        """

        max_result = 100
        limit = self.max_numTweet / max_result

        return tweepy.Paginator(self.client.get_users_tweets,
                                id=user_id, tweet_fields=self.tweet_requested_fields,
                                start_time=self.start_time, end_time=self.end_time,
                                max_results=max_result, limit=limit)

    @staticmethod
    def _get_twitterClient(user_api: str):
        bearer_token = ""
        if user_api == "A":
            bearer_token = "HIDDEN"
        elif user_api == "C":
            bearer_token = "HIDDEN"
        elif user_api == "K":
            bearer_token = "HIDDEN"

        if bearer_token == "":
            raise Exception("Errore: api non trovata")

        return tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)
