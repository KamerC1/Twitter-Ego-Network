import os
from typing import Dict
import pyspark.pandas as ps

from src.utils.utils import write_log


class TweetDB:
    """
    Salva i dati grezzi dei tweet in entità del database (le entità sono: tweet, retweet, quote, reply)
    """
    

    def __init__(self, rawData_dir):
        self.tweet_dict = self._get_tweet_dir(rawData_dir)

    @staticmethod
    def _get_tweet_dir(rawData_dir) -> Dict[str, str]:
        """
        Restituisce un dizionario che ha come chiave l'id dell'utente e come valore il path della directory "tweet"
        """
        # Recupera tutti i nomi di directory nella directory
        dir_names = [name for name in os.listdir(rawData_dir) if os.path.isdir(os.path.join(rawData_dir, name))]

        #Costruisce il dizionario considerando solo le directory che contengono la directory "tweet"
        #Come chiave vi è l'id dell'utente e come valore il path della directory "tweet"
        tweet_dir_paths = {}
        for dir_name in dir_names:
            tweet_path = os.path.join(rawData_dir, dir_name, "tweet")
            if os.path.exists(tweet_path) and os.path.isdir(tweet_path) and len(os.listdir(tweet_path)) != 0:
                    tweet_dir_paths[dir_name] = tweet_path

        return tweet_dir_paths

    def saveTweetEntity(self, save_dir_database, log_file):
        """
        NB save_dir_database deve essere vuota perché viene fatto l'append dei dati
        """
        for user_id, raw_tweet_dir in self.tweet_dict.items():
            try:
                self._rawTweet_to_database(user_id, raw_tweet_dir, save_dir_database)
                print(f"Conversione dei tweet dell'utente {user_id} in database completata")
            except Exception as e:
                print(f"Errore nella conversione dei tweet dell'utente {user_id} in database: {e}")
                write_log(log_file, f"Errore nella conversione dei tweet dell'utente {user_id} in database: {e}")

    def _rawTweet_to_database(self, user_id, raw_tweet_dir, save_dir_database):
        self._saveReTweetEntity(user_id, raw_tweet_dir, save_dir_database)
        self._saveTextTweetEntity(user_id, raw_tweet_dir, save_dir_database)
        self._saveReplyEntity(raw_tweet_dir, save_dir_database)
        self._saveQuoteEntity(raw_tweet_dir, save_dir_database)
        self._saveMentionsEntity(raw_tweet_dir, save_dir_database)

    @staticmethod
    def _saveReTweetEntity(user_id, raw_tweet_dir, save_dir_database):
        raw_tweet = ps.read_json(raw_tweet_dir)

        raw_tweet = raw_tweet.assign(user_id=user_id)
        retweet = ps.sql("SELECT id, user_id, created_at_utc, edit_history_tweet_ids, "
                         "referenced_tweets[0]['referenced_tweet_id'] as retweeted_id, "
                         "entities['mentions']['id'][0] as retweeted_user_id, "
                         "entities['mentions']['username'][0] as retweeted_username "
                         "FROM {raw_tweet} "
                         "WHERE referenced_tweets[0]['referenced_tweet_type'] IS NOT NULL AND "
                         "referenced_tweets[0]['referenced_tweet_type'] == 'retweeted'",
                         raw_tweet=raw_tweet)
        retweet.to_parquet(f"{save_dir_database}/retweet", mode='a')

    @staticmethod
    def _saveTextTweetEntity(user_id, raw_tweet_dir, save_dir_database):
        raw_tweet = ps.read_json(raw_tweet_dir)

        raw_tweet = raw_tweet.assign(user_id=user_id)
        texted_tweet = ps.sql("SELECT id, user_id, created_at_utc, edit_history_tweet_ids, text "
                              "FROM {raw_tweet} "
                              "WHERE referenced_tweets[0]['referenced_tweet_type'] IS NULL "
                              "OR referenced_tweets[0]['referenced_tweet_type'] != 'retweeted'",
                              raw_tweet=raw_tweet)
        texted_tweet.to_parquet(f"{save_dir_database}/texted_tweet", mode='a')

    @staticmethod
    def _saveReplyEntity(raw_tweet_dir, save_dir_database):
        raw_tweet = ps.read_json(raw_tweet_dir)

        reply = ps.sql("SELECT id as texted_tweet_id, in_reply_to_user_id, "
                       "referenced_tweets[0]['referenced_tweet_id'] as replied_tweet_id "
                       "FROM {raw_tweet} "
                       "WHERE referenced_tweets[0]['referenced_tweet_type'] == 'replied_to'",
                       raw_tweet=raw_tweet)
        reply.to_parquet(f"{save_dir_database}/reply", mode='a')

    @staticmethod
    def _saveQuoteEntity(raw_tweet_dir, save_dir_database):
        raw_tweet = ps.read_json(raw_tweet_dir)

        quote = ps.sql("SELECT id as texted_tweet_id, "
                       "referenced_tweets[0]['referenced_tweet_id'] as quoted_tweet_id "
                       "FROM {raw_tweet} "
                       "WHERE referenced_tweets[0]['referenced_tweet_type'] == 'quoted'", 
                       raw_tweet=raw_tweet)
        quote.to_parquet(f"{save_dir_database}/quote", mode='a')

    @staticmethod
    def _saveMentionsEntity(raw_tweet_dir, save_dir_database):
        raw_tweet = ps.read_json(raw_tweet_dir)

        mentions_temp = ps.sql("SELECT id as tweet_id, entities['mentions'] as mentions "
                               "FROM {raw_tweet} "
                               "WHERE entities['mentions'] IS NOT NULL AND "
                               "(referenced_tweets IS NULL OR referenced_tweets[0]['referenced_tweet_type'] != 'retweeted')",
                               raw_tweet=raw_tweet)

        mentions = mentions_temp.explode("mentions")
        mentions = ps.sql("SELECT tweet_id, mentions['id'] as user_id, mentions['username'] as username "
                          "FROM {mentions}",
                          mentions=mentions)

        mentions.to_parquet(f"{save_dir_database}/mentions", mode='a')