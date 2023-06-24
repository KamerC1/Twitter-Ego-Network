import numpy as np
import pandas as pd
from sklearn.cluster import MeanShift
import os


class Clustering:
    def __init__(self, activeEgoNetwork_dir):
        self.activeEgoNetwork = pd.read_parquet(activeEgoNetwork_dir)

    def save_clustering(self, save_dir):
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        userId_list = self._get_userId_list(self.activeEgoNetwork)

        for user_id in userId_list:
            print(f"Clustering {user_id}...")
            user_labels = self._meanShift_to_ego(user_id, self.activeEgoNetwork)
            user_labels.to_parquet(f"{save_dir}/{user_id}")

    @staticmethod
    def _get_userId_list(activeEgoNetwork):
        user_id = activeEgoNetwork['ego'].drop_duplicates()
        return user_id.tolist()

    @staticmethod
    def _meanShift_to_ego(user_id, activeEgoNetwork):
        contact_frequency = activeEgoNetwork[activeEgoNetwork['ego'] == user_id]

        contact_frequency_np = contact_frequency.contact_frequency.to_numpy(dtype=np.float64)
        contact_frequency_np2D = contact_frequency_np.reshape(-1, 1)
        ms = MeanShift(bin_seeding=True) #Chiara ha usato il bin_seeding (get_optimal_num_circles -> https://github.com/chibold/egonetworks/blob/master/egonetworks/core.py)
        ms.fit(contact_frequency_np2D)
        labels = ms.labels_

        return pd.DataFrame({'ego': user_id, 'alter_id': contact_frequency['alter'], 'labels': labels}).reset_index(drop=True)