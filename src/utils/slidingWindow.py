import datetime
from src.utils.globalVariable import START_DOWNLOAD_UTC, END_DOWNLOAD_UTC


def get_period_list():
    """
    Restituisce una lista di tuple, ogni tupla è un periodo di 2 anni (in secondi) che parte da START_DOWNLOAD_UTC e finisce a END_DOWNLOAD_UTC. 
    Tra una tupla e l'altra passano 6 mesi.
    """
    
    period_list = []

    for n in range(0, 100):
        seconds_year = 31557600 #anni in secondi
        seconds_6month = seconds_year/2 #6mesi in secondi

        first_period = START_DOWNLOAD_UTC+n*seconds_6month
        second_period = first_period+2*seconds_year

        if second_period > END_DOWNLOAD_UTC:
            break

        period_list.append((first_period, second_period))

    return period_list

def utc_to_string(unix_timestamp):
    """
    Converte un timestamp UTC in una stringa nel formato YYYY.MM
    """
    
    utc_datetime = datetime.datetime.utcfromtimestamp(unix_timestamp)
    return utc_datetime.strftime('%Y.%m')