from typing import List

import pyspark.pandas as ps
from src.dataProcessing.tweet.Tweet import Tweet


class BasicEgoNetwork:
    def __init__(self, tweet_database_dir, user_id: List[int]):
        """

        :param tweet_database_dir: database dove sono contenuti tutti i tweet
        :param regularActiveUser_retweet_dir: retweet degli utenti di cui vogliamo recuperare la ego network in uno specifico periodo
        :param regularActiveUser_texted_tweet_dir: texted_tweet degli utenti di cui vogliamo recuperare la ego network in uno specifico periodo
        """
        self.tweet = Tweet(tweet_database_dir)
        self.user_id = tuple(user_id)

    def get_observedEgoNetwork(self, startPeriod_utc, endPeriod_utc, min_relationship_duration):
        """
        Rimuove le relazioni che sono nate verso la fine dell'analisi

        :param last_accepted_interaction_utc: Ultimo istante in cui si considerano le interazioni tra ego e alter
        """

        egoNetwork = self._get_egoNetwork(startPeriod_utc, endPeriod_utc)
        year_in_seconds = 31536000
        contact_frequency = ps.sql("SELECT *, "
                                   "({endPeriod_utc} - first_interaction_utc)/{year_in_seconds} as L "
                                   "FROM {egoNetwork} ", 
                                   endPeriod_utc=endPeriod_utc, year_in_seconds=year_in_seconds, egoNetwork=egoNetwork)

        return ps.sql("SELECT *, num_interaction/L as contact_frequency "
                      "FROM {contact_frequency} "
                      "WHERE L >= {min_relationship_duration}", 
                      contact_frequency=contact_frequency, min_relationship_duration=min_relationship_duration)

    def _get_egoNetwork(self, startPeriod_utc, endPeriod_utc):
        """
        Costruisce la ego network degli utenti osservando solo i tweet publicati nel perido [startPeriod_utc, endPeriod_utc]
        """
        
        retweet = self._get_retweet()
        mention = self._get_mention()
        reply = self._get_reply()

        ego_network = ps.concat([retweet, mention, reply])
        ego_network = ps.sql("SELECT * "
                             "FROM {ego_network} "
                             "WHERE created_at_utc BETWEEN {startPeriod_utc} AND {endPeriod_utc}",
                             ego_network=ego_network, startPeriod_utc=startPeriod_utc, endPeriod_utc=endPeriod_utc)


        ego_network = ps.sql("SELECT * "
                             "FROM {ego_network} "
                             "WHERE ego != alter AND "
                             "ego is not NULL AND "
                             "alter is not NULL ",
                             ego_network=ego_network)

        return ps.sql("SELECT ego, alter, "
                           "COUNT(*) as num_interaction, "
                           "MIN(created_at_utc) as first_interaction_utc, "
                           "MAX(created_at_utc) as last_interaction_utc "
                       "FROM {ego_network} "
                       "GROUP BY ego, alter ",
                       ego_network=ego_network)

    def _get_retweet(self):
        retweet = self.tweet.retweet
        user_id_list = self.user_id
        return ps.sql("SELECT id, user_id as ego, retweeted_user_id as alter, created_at_utc "
                      "FROM {retweet} "
                      "WHERE user_id in {user_id_list}",
                      retweet=retweet, user_id_list=user_id_list)

    def _get_reply(self):
        reply_tweet = self.tweet.get_replyTweet()
        user_id_list = self.user_id
        return ps.sql("SELECT id, user_id as ego, in_reply_to_user_id as alter, created_at_utc "
                      "FROM {reply_tweet} "
                      "WHERE user_id in {user_id_list}",
                      reply_tweet=reply_tweet, user_id_list=user_id_list)

    def _get_mention(self):
        mention_tweet = self.tweet.get_mentionTweet()
        user_id_list = self.user_id
        return ps.sql("SELECT id, user_id as ego, mentioned_user_id as alter, created_at_utc "
                      "FROM {mention_tweet} "
                      "WHERE user_id in {user_id_list}",
                      mention_tweet=mention_tweet, user_id_list=user_id_list)