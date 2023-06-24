import pyspark.pandas as ps

class ActiveEgoNetwork:
    def __init__(self, egoNetwork_dir):
        self.egoNetwork = ps.read_parquet(egoNetwork_dir)

    def get_ActiveEgoNetwork(self):
        """
        Data la ego network, restituisce la ego network degli utenti attivi
        """
        
        
        egoNetwork = self.egoNetwork
        return ps.sql("SELECT * "
                      "FROM {egoNetwork} "
                      "WHERE contact_frequency >= 1 AND num_interaction > 1",
                      egoNetwork=egoNetwork)
