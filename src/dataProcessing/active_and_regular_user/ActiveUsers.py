import pyspark.pandas as ps
from pyspark.pandas import DataFrame

from src.dataProcessing.tweet.Tweet import Tweet


class ActiveUsers:
    def __init__(self, database_dir):
        self.tweet = Tweet(database_dir)

    def get_active_users(self, startPeriod_utc, endPeriod_utc) -> DataFrame:
        """
        Get the active users in the period [startPeriod_utc, endPeriod_utc]
        """
        tweet = self.tweet.get_socialTweet_period(startPeriod_utc, endPeriod_utc)
        return self._get_active_users(tweet, endPeriod_utc)

    def _get_active_users(self, tweet_data, endPeriod_utc):
        active_users_tweets = self._compute_T_inactive(tweet_data, endPeriod_utc)
        month_6_in_seconds = 15780000
        return ps.sql("SELECT distinct user_id "
                      "FROM {active_users_tweets} "
                      "WHERE T_inactive_seconds < MAX_IIT_seconds + {month_6_in_seconds} ",
                      active_users_tweets=active_users_tweets, month_6_in_seconds=month_6_in_seconds)

    @staticmethod
    def _compute_T_inactive(tweet_data, endPeriod_utc) -> DataFrame:
        """
        Compute the time between the last tweet of the user and the download date
        :param tweet_data: tweet_data: tweet + retweet data
        :param endPeriod_utc: ultimo istante in cui analizzare i dati
        """

        IIT = ps.sql("SELECT *, "
                     "created_at_utc - LAG(created_at_utc) OVER (PARTITION BY user_id ORDER BY created_at_utc) as IIT_seconds "
                     "FROM {tweet_data} ",
                     tweet_data=tweet_data)

        T_inactive_seconds = ps.sql("SELECT *, "
                                    "{endPeriod_utc} - MAX(created_at_utc) OVER (PARTITION BY user_id) as T_inactive_seconds "
                                    "FROM {IIT} ",
                                    IIT=IIT, endPeriod_utc=endPeriod_utc)

        MAX_IIT_seconds = ps.sql("SELECT *, MAX(IIT_seconds) OVER (PARTITION BY user_id) as MAX_IIT_seconds "
                                 "FROM {T_inactive_seconds}",
                                 T_inactive_seconds=T_inactive_seconds)
        return MAX_IIT_seconds