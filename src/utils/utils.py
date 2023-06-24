from datetime import datetime
from pandas import Series
import pandas as pd


def write_log(log_file_path, text):
    """
    Scrive un messaggio di log su un file di log.
    :param log_file_path: path del file di log
    :param text: messaggio da scrivere
    """
    print(text)
    with open(log_file_path, "a") as log_file:
        log_file.write(str(datetime.now())+" : "+text + "\n")

def get_outlier_threshold(values: Series) -> float:
    """
    Data una serie di valori, restituisce il valore oltre il quale si considerano outlier
    :param values: serie di valori (eg dimensione della ActiveEgoNetwork per ego)
    """

    q1 = values.quantile(0.25)
    q3 = values.quantile(0.75)
    iqr = q3 - q1

    # define the outlier threshold
    return q3 + 1.5 * iqr

def select_query(dataframe, value_series, column_name):
    """
    Restituisce un dataframe che contiene solo le righe che hanno il valore della colonna column_name contenuto nella lista value_list
    """
    return dataframe[dataframe[column_name].isin(value_series)]

