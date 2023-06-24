import json
import os
from typing import List

# TODO Codice obsoleto, ora recupero i mention più velocemente con pyspark

def get_username_from_dir(user_directory: str) -> List[str]:
    """
    Dato il path di una directory, recupera tutti gli utenti salvati nei file json
    :param user_directory: directory dove sono salvati i file json, ogni file json contiene una lista di utenti
           esempio file: https://www.notion.so/Struttura-directory-e-file-2192fb288b354138a5efca7173845a02#09ae7dad111e4456b5729b5c8c4ed5e6
    :return: lista username (N.B. può contenere duplicati)
    """

    username_list = []

    for filename in os.scandir(user_directory):
        if filename.is_file():
            with open(filename, 'r') as file:
                following_dict = json.load(file)
                username_list.extend(_get_username(following_dict))

    return username_list


def _get_username(user_list: List[dict]) -> List:
    """
    Dato la lista di utenti, recupera per ogni utente il suo username

    :param user_list: lista di utenti, ogni elemento è un dizionario
               esempio dizionario: https://www.notion.so/Struttura-directory-e-file-2192fb288b354138a5efca7173845a02#09ae7dad111e4456b5729b5c8c4ed5e6
    :return: lista di dizionari, ogni dizionario è un mentioning
    """

    username_list = []
    for user in user_list:
        username_list.append(user["username"])

    return username_list