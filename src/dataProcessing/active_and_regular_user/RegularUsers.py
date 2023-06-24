import pyspark.pandas as ps
from pyspark.pandas import DataFrame

from src.dataProcessing.tweet.Tweet import Tweet


class RegularUsers:
    def __init__(self, database_dir):
        self.tweet = Tweet(database_dir)

    def get_regular_users(self, startPeriod_utc, endPeriod_utc) -> DataFrame:
        """
        Get the regular users in the period [startPeriod_utc, endPeriod_utc]
        """

        tweet = self.tweet.get_socialTweet_period(startPeriod_utc, endPeriod_utc)
        # Dato che è possibile menzionare più utenti in un tweet => ci possono essere righe che contengo lo stesso id
        tweet = tweet.drop_duplicates(subset=["id"]).reset_index(drop=True)
        return self._get_regular_users(tweet)

    @staticmethod
    def _get_regular_users(tweet_data) -> DataFrame:

        # Aggiunge le colonne year e month a partire da created_at_utc
        tweet_data['created_at'] = ps.to_datetime(tweet_data['created_at_utc'], unit='s')
        tweet_data["year"] = tweet_data['created_at'].dt.year
        tweet_data["month"] = tweet_data['created_at'].dt.month

        IIT = ps.sql("SELECT *, "
                     "created_at_utc - LAG(created_at_utc) OVER (PARTITION BY user_id ORDER BY created_at_utc) as IIT_seconds "
                     "FROM {tweet_data} ", tweet_data=tweet_data)

        # Elimina il primo tweet di ogni mese
        regular_users_tweet = ps.sql("SELECT *, "
                                     "ROW_NUMBER() OVER (PARTITION BY user_id, month, year ORDER BY created_at_utc) as row_number "
                                     "FROM {IIT}", IIT=IIT)
        regular_users_tweet = ps.sql("SELECT * "
                                     "FROM {regular_users_tweet} "
                                     "WHERE row_number != 1", regular_users_tweet=regular_users_tweet)

        # Recupera solo i mesi dove gli utenti avevano un MEAN(IIT) <= 3 giorni
        days_3_in_seconds = 259200
        regular_users_tweet = ps.sql("SELECT user_id, year, month, MEAN(IIT_seconds) AS IIT_avg "
                                     "FROM {regular_users_tweet} "
                                     "GROUP BY user_id, year, month "
                                     "HAVING MEAN(IIT_seconds) <= {days_3_in_seconds} ", 
                                     regular_users_tweet=regular_users_tweet, days_3_in_seconds=days_3_in_seconds)

        # Recupera solo gli utenti che hanno almeno il 50% dei mesi con un MEAN(IIT) <= 3 giorni
        regular_users_tweet = ps.sql("SELECT user_id, COUNT(*) as numero_di_mesi_attivo "
                                     "FROM {regular_users_tweet} "
                                     "GROUP BY user_id "
                                     "HAVING COUNT(*) >= 12", 
                                     regular_users_tweet=regular_users_tweet)

        return regular_users_tweet