import pyspark.pandas as ps
from pyspark.pandas import DataFrame

from src.dataProcessing.active_and_regular_user.ActiveUsers import ActiveUsers
from src.dataProcessing.active_and_regular_user.RegularUsers import RegularUsers
from src.dataProcessing.tweet.Tweet import Tweet


class RegularAndActiveUser:
    """
    Dati i tweet in database_dir, restituisce gli utenti che sono regolari e attivi nel periodo [startPeriod_utc, endPeriod_utc]
    """
    
    def __init__(self, database_dir, startPeriod_utc, endPeriod_utc):
        self.tweet = Tweet(database_dir)
        self.regularUsers = RegularUsers(database_dir)
        self.activeUsers = ActiveUsers(database_dir)
        self.startPeriod_utc = startPeriod_utc
        self.endPeriod_utc = endPeriod_utc

    def get_regularAndActiveUser(self) -> DataFrame:
        """
        Restituisce gli utenti che sono regolari e attivi nel periodo [startPeriod_utc, endPeriod_utc]
        """

        activeUsers = self.activeUsers.get_active_users(self.startPeriod_utc, self.endPeriod_utc)
        regularUsers = self.regularUsers.get_regular_users(self.startPeriod_utc, self.endPeriod_utc)
        return ps.sql("SELECT distinct user_id "
                      "FROM {activeUsers} "
                      "WHERE user_id IN (SELECT user_id FROM {regularUsers})", activeUsers=activeUsers, regularUsers=regularUsers)