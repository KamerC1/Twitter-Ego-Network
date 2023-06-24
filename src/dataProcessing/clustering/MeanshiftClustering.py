import pandas as pd

from src.dataAnalysis.clustering.Cluster import Cluster


class MeanshiftClustering:
    def __init__(self, activeEgoNetwork_dir, clustering_dir):
        self.activeEgoNetwork = pd.read_parquet(activeEgoNetwork_dir)
        self.clustering = Cluster(clustering_dir, activeEgoNetwork_dir).clustering.to_pandas()

    def save_clustering(self, save_dir):
        """
        Applica _jenksClustering a tutti gli ego
        """
        userId_list = self._get_userId_list(self.activeEgoNetwork)

        for user_id in userId_list:
            print(f"_jenksClustering {user_id}...")
            ego_activeEgoNetwork = self.activeEgoNetwork.loc[self.activeEgoNetwork['ego'] == user_id]
            ego_clustering = self.clustering.loc[self.clustering['ego'] == user_id]
            user_labels = self._meanshiftClustering(ego_activeEgoNetwork, ego_clustering)
            user_labels.to_parquet(f"{save_dir}/{user_id}.parquet")

    @staticmethod
    def _get_userId_list(activeEgoNetwork):
        user_id = activeEgoNetwork['ego'].drop_duplicates()
        return user_id.tolist()

    @staticmethod
    def _meanshiftClustering(ego_activeEgoNetwork, ego_clustering):
        """
        Assegna un label ad ogni alter in base al clustering di meanshift
        """

        merged_df = pd.merge(ego_activeEgoNetwork[['ego', 'contact_frequency', 'alter']],
                             ego_clustering[['alter_id', 'labels']], left_on='alter', right_on='alter_id')
        ego_activeEgoNetwork = merged_df[['ego', 'contact_frequency', 'labels']].sort_values('contact_frequency')

        break_list = ego_activeEgoNetwork.groupby('labels').agg({'contact_frequency': ['min', 'max']}).reset_index()
        break_list.columns = ['labels', 'min', 'max']

        maxBreak_list = sorted(break_list['max'].tolist())
        minBreak_list = sorted(break_list['min'].tolist())

        for i in range(len(maxBreak_list)):
            mask = (ego_activeEgoNetwork['contact_frequency'] >= minBreak_list[i]) & \
                   (ego_activeEgoNetwork['contact_frequency'] <= maxBreak_list[i])
            ego_activeEgoNetwork.loc[mask, 'labels'] = i+1

        ego_activeEgoNetwork = ego_activeEgoNetwork.reset_index(drop=True)
        return ego_activeEgoNetwork