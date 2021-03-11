import src.search_cids as sc
import pandas as pd


if __name__ == '__main__':

    codes_file_path = '/home/user4/Projects/ufrj/ic/sp_cid_gravidez/data/codigos.csv'
    csv_files_path = '/home/user4/Data/ufrj/ic/SP/2015/csv/'
    columns_after_join = ['SP_ATOPROF', 'SP_CIDPRI', 'description']

    # get the sp files names 
    sp_csv_files = sc.return_sp_files_on_path(csv_files_path)

    # lê os códigos CID relacionados a gravidez
    preg_cid_codes = sc.read_codes(codes_file_path)

    # empty dataframe that will contain the results
    full_data = None

    for csv_file in sp_csv_files:
        
        full_file_path = csv_files_path + csv_file
        matches = sc.verify_codes(full_file_path, preg_cid_codes)
        
        if  not full_data:
            full_data = matches
        else:
            full_data.append(matches)
    
    if full_data:
        print('Escrevendo dados em um arquivo csv')
        full_data.to_csv('results/resultados.csv', sep=',')
