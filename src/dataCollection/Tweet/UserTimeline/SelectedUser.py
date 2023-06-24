from typing import List

import pyspark.pandas as ps

class SelectedUser:

    def __init__(self, userDB_dir, selectedUser_dir):
        self.usersDB = ps.read_parquet(userDB_dir)
        self.selected_users = ps.read_json(selectedUser_dir)

    def getSelectedUser_for_academicAPI(self) -> List[int]:
        """
        Seleziona gli utenti che hanno più di 3200 tweet totali (la funzione search_recent_tweets di tweepy non permette di recuperare più di 3200 tweet, 
        allora per questi utenti utilizziamo la funzione search_all_tweets che richiede le api academic) 
        """
        
        usersId = ps.sql("SELECT distinct user_id "
                         "FROM {selected_users} "
                         "WHERE user_id IN (SELECT id "
                                             "FROM {usersDB} "
                                             "WHERE tweet_count > 3200) ", 
                        selected_users=self.selected_users, usersDB=self.usersDB)
        return usersId.user_id.tolist()

    def getSelectedUser_for_userTimeline(self) -> List[int]:
        """
        Seleziona gli utenti che hanno meno di 3200 tweet totali (la funzione search_recent_tweets di tweepy permette di recuperare fino a 3200 tweet)
        """

        usersId = ps.sql("SELECT distinct user_id "
                         "FROM {selected_users} "
                         "WHERE user_id IN (SELECT id "
                                             "FROM {usersDB} "
                                             "WHERE tweet_count <= 3200) ", 
                        selected_users=self.selected_users, usersDB=self.usersDB)
        return usersId.user_id.tolist()
