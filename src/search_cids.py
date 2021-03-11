import os
from typing import List
import pandas as pd
import numpy as np
from pandas.core.reshape.merge import merge



def read_codes(path: str) -> pd.DataFrame:
    """ Read the  CID-10 codes related to pregnancy """

    codes = pd.read_csv(path, sep=';')

    codes.description = codes.description.fillna('Sem descrição')

    return codes


def return_sp_files_on_path(path: str) -> List[str]:
    """ Returns a list of csv filenames """

    filenames: List[str] = os.listdir(path)
    csv_filenames: List[str] = [filename for filename in filenames if filename.endswith('.csv')]

    return csv_filenames


def verify_codes(sp_file_fullpath: str, codes: pd.DataFrame) -> pd.DataFrame:
    """ Will return the a list of the professional codes rows matches any of the codes """
    
    columns = ['SP_ATOPROF', 'SP_CIDPRI']
    types = {columns[0]: 'int64', columns[1]: str}

    sp_df = pd.read_csv(sp_file_fullpath, usecols=columns, dtype=types)
    print(f'{sp_df.shape[0]} linhas encontradas')

    # make left join
    merged = pd.merge(left=sp_df, right=codes, how='left', left_on=columns[1], right_on='code')
    # drop Nan
    merged.dropna(inplace=True)
    # remove duplicates
    merged.drop_duplicates(inplace=True)
    # drop duplicated column
    merged.drop('code', axis='columns', inplace=True)

    print(merged.head())
    print(f'{merged.shape[0]} após o join')

    return merged
