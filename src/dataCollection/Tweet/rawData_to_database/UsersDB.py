import pyspark.pandas as ps

class UsersDB:
    def __init__(self, rawData_dir, year):
        self.users = ps.read_json(f"{rawData_dir}/tweet/{year}/*/followers")
        self.botometer = ps.read_json(f"{rawData_dir}/user/botometer/*")
        
    def saveUserEntity(self, save_dir):
        """
        Salva i dati recuperati da twitter in una entità del database
        """
        
        self.users = self._removeDuplicate()
        self.users.to_parquet(save_dir)

    def _removeDuplicate(self):
        #Due utenti possono seguire la stessa persona x => l'utente x compare due volte
        self.users.drop_duplicates(subset='id', inplace=True)
        return self.users.reset_index(drop=True)

    def saveBotometerEntity(self, save_dir):
        """
        Salva i dati recuperati da botometer in una entità del database
        """
        
        botometer = ps.sql("SELECT cap.universal as universal_cap, cap.english as english_cap, "
                                  "user.majority_lang as majority_lang, "
                                  "CAST(user.user_data.id_str AS INTEGER ) as user_id "
                            "FROM {botometer}", botometer=self.botometer)

        botometer.to_parquet(save_dir)