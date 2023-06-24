import os
import jenkspy
import numpy as np
import pandas as pd

from src.dataAnalysis.clustering.Cluster import Cluster


class JenksClustering:
    def __init__(self, activeEgoNetwork_dir, clustering_dir):
        self.activeEgoNetwork = pd.read_parquet(activeEgoNetwork_dir).astype({'contact_frequency': 'float64'})
        self.numClusters_perEgos = Cluster(clustering_dir, activeEgoNetwork_dir).get_numClusters_perEgos().to_pandas()

    def save_clustering(self, save_dir):
        """
        Applica _jenksClustering a tutti gli ego
        """
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        userId_list = self._get_userId_list(self.numClusters_perEgos)

        for user_id in userId_list:
            print(f"_jenksClustering {user_id}...")
            ego_activeEgoNetwork = self.activeEgoNetwork.loc[self.activeEgoNetwork['ego'] == user_id].copy()
            num_circle = self.numClusters_perEgos.loc[self.numClusters_perEgos['ego'] == user_id, 'num_clusters'].iloc[0]
            user_labels = self._jenksClustering(ego_activeEgoNetwork, int(num_circle))
            user_labels.to_parquet(f"{save_dir}/{user_id}")

    @staticmethod
    def _get_userId_list(activeEgoNetwork):
        user_id = activeEgoNetwork['ego'].drop_duplicates()
        return user_id.tolist()


    def _jenksClustering(self, ego_activeEgoNetwork, num_circle):
        """
        Assegna un label ad ogni alter in base al clustering di jenks
        """

        breakList = self._jenksBreakList(ego_activeEgoNetwork, num_circle)

        for i in range(len(breakList) - 1):
            mask = (ego_activeEgoNetwork['contact_frequency'] > breakList[i]) & \
                   (ego_activeEgoNetwork['contact_frequency'] <= breakList[i + 1])
            ego_activeEgoNetwork.loc[mask, 'labels'] = i+1

        ego_activeEgoNetwork = ego_activeEgoNetwork.reset_index(drop=True)
        return ego_activeEgoNetwork

    @staticmethod
    def _jenksBreakList(ego_activeEgoNetwork, n_classes):
        """
        Applica l'algoritmo di Jenks per individuare i breakpoints
        """

        contact_frequency_np = ego_activeEgoNetwork.contact_frequency.to_numpy(dtype=np.float64)
        breakList = jenkspy.jenks_breaks(contact_frequency_np, n_classes=n_classes)
        unique_breakList = list(dict.fromkeys(breakList))
        unique_breakList[0] = 0 # spiegato qui: https://www.notion.so/Algoritmo-jenks-b86671c13d8846bcb0d59eeb09edfefd
        return unique_breakList #Rimuove i duplicati: a volte jenks_breaks non riesce a individuare esattamente n_classes