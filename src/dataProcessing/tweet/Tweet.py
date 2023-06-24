from typing import List

import pyspark.pandas as ps
from pyspark.pandas import DataFrame

class Tweet:
    def __init__(self, database_dir):
        self.texted_tweet = ps.read_parquet(f"{database_dir}/texted_tweet")
        self.retweet = ps.read_parquet(f"{database_dir}/retweet")
        self.mention_tweet = ps.read_parquet(f"{database_dir}/mentions")
        self.reply_tweet = ps.read_parquet(f"{database_dir}/reply")

    def get_socialTweet_period(self, first_tweet_utc, last_tweet_utc):
        """
        Recupera i tweet sociali (i.e. i tweet rivolti ad un'altra persona: reply, mention, retweet) che sono stati creati nel periodo [first_tweet_utc, last_tweet_utc]
        """
        
        socialTweet = self.get_socialTweet()
        # Dato che è possibile menzionare più utenti in un tweet => ci possono essere righe che contengo lo stesso id
        socialTweet = socialTweet.drop_duplicates(subset=["id"]).reset_index(drop=True)
        return ps.sql("SELECT * FROM {socialTweet} "
                     "WHERE created_at_utc BETWEEN {first_tweet_utc} AND {last_tweet_utc}",
                     socialTweet=socialTweet, first_tweet_utc=first_tweet_utc, last_tweet_utc=last_tweet_utc)

    def get_socialTweet(self) -> DataFrame:
        """
        Recupera tutti i tweet sociali (i.e. i tweet rivolti ad un'altra persona: reply, mention, retweet)
        """
        
        mention_tweet = self.get_mentionTweet()
        reply_tweet = self.get_replyTweet()

        mention_tweet = mention_tweet[['id', 'user_id', 'created_at_utc']]
        retweet = self.retweet[['id', 'user_id', 'created_at_utc']]
        reply_tweet = reply_tweet[['id', 'user_id', 'created_at_utc']]

        socialTweet = ps.concat([reply_tweet, mention_tweet, retweet])
        return socialTweet

    def get_tweet_in_period(self, first_tweet_utc, last_tweet_utc) -> DataFrame:
        """
        Recupera tutti i tweet che sono stati creati nel periodo [first_tweet_utc, last_tweet_utc]
        """
        
        tweet = self.get_tweet()
        return ps.sql("SELECT * FROM {tweet} "
                        "WHERE created_at_utc BETWEEN {first_tweet_utc} AND {last_tweet_utc}",
                        tweet=tweet, first_tweet_utc=first_tweet_utc, last_tweet_utc=last_tweet_utc)

    def get_tweet(self) -> DataFrame:
        """
        Get texted_tweet and retweet data from the database and merge them
        :param database_dir: path to the database
        """
        texted_tweet = self.texted_tweet[['user_id', 'created_at_utc']]
        retweet = self.retweet[['user_id', 'created_at_utc']]
        tweet = ps.concat([texted_tweet, retweet])
        return tweet

    def get_mentionTweet(self) -> DataFrame:
        """
        Recupera i texted_tweet che contengono menzioni
        """
        
        texted_tweet = self.texted_tweet
        mention_tweet = self.mention_tweet
        return ps.sql("SELECT texted_tweet.*, mention_tweet.user_id as mentioned_user_id, "
                            "mention_tweet.username as mentioned_username "
                     "FROM {texted_tweet} texted_tweet JOIN {mention_tweet} mention_tweet "
                     "ON texted_tweet.id = mention_tweet.tweet_id",
                     texted_tweet=texted_tweet, mention_tweet=mention_tweet)

    def get_replyTweet(self) -> DataFrame:
        """
        Recupera i texted_tweet che sono dei reply
        """
        
        texted_tweet = self.texted_tweet
        reply_tweet = self.reply_tweet
        reply_tweet = ps.sql("SELECT * "
                             "FROM {texted_tweet} texted_tweet JOIN {reply_tweet} reply_tweet "
                             "ON texted_tweet.id = reply_tweet.texted_tweet_id", 
                             texted_tweet=texted_tweet, reply_tweet=reply_tweet).drop("texted_tweet_id")

        # spiegato qui il perché: https://www.notion.so/Relazione-tra-reply-e-mention-c2a6001643d044af8425614b5f7b8826
        # (In breve, i reply sono anche mention, ma a volte, alcuni reply non sono contenuti in mention
        # => qui raccolgo solo i reply che non sono in "mention")
        mention_tweet = self.get_mentionTweet()
        return ps.sql("SELECT * "
                      "FROM {reply_tweet} "
                      "WHERE id NOT IN (SELECT id FROM {mention_tweet})",
                      reply_tweet=reply_tweet, mention_tweet=mention_tweet)


    def get_user_textedTweet(self, user_list: List[str]) -> DataFrame:
        return self._get_user_tweet(self.texted_tweet, user_list)

    def get_user_retweet(self, user_list: List[str]) -> DataFrame:
        return self._get_user_tweet(self.retweet, user_list)

    @staticmethod
    def _get_user_tweet(tweet: DataFrame, user_list: List[str]) -> DataFrame:
        "Recupera i tweet di un utente"
        return ps.sql("SELECT * "
                      "FROM {tweet} "
                      "WHERE user_id IN {user_list}",
                      tweet=tweet, user_list=user_list)
