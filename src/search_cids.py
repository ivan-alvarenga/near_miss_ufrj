import os
from typing import List
import pandas as pd



def read_codes(path: str) -> pd.DataFrame:
    """ Read the  CID-10 codes related to pregnancy """

    codes = pd.read_csv(path, sep=';')

    codes.description = codes.description.fillna('Sem descrição')

    return codes


def read_sp_files(path: str) -> List[str]:
    """ Returns a list of csv filenames """

    filenames: List[str] = os.listdir(path)
    csv_filenames: List[str] = [filename for filename in filenames if filename.endswith('.csv')]

    return csv_filenames


def verify_codes(sp: pd.Dataframe, codes: pd.DataFrame) -> List[str]:
    """ Will return the a list of the professional codes rows matches any of the codes """
    # TODO create left join with codes, then will be left only the matches

    # TODO return the list of matches