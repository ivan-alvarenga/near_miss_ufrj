
import pandas as pd
import os

o_cids = pd.read_csv('../csv/o_cids.csv', sep=';')

files = os.listdir('C:\\Users\\Visagio\\Projetos\\UFRJ\\IC\\SPSP')

for filename in files:
    try:
        print('lendo arquivo ' + filename)
        sp_df = pd.read_csv('~\\Projetos\\UFRJ\\IC\\SPSP\\' + filename)
        print('leitura concluída')

        # faz-se o merge do arquivo sp com o dataframe de CIDs
        print('fazendo cruzamento dos dados')
        merged_dfs = sp_df.merge(o_cids, left_on='SP_CIDPRI', right_on='code', how='left').drop('code', axis=1)
        print('cruzamento finalizado')

        # remove as linhas que não são dos CIDs selecionados
        clean_df = merged_dfs[merged_dfs['influence_type'].notna()]        

        # salva o resultado na pasta
        print('Salvando arquivo')
        clean_df.to_csv('../results/' + filename, sep=';', index=False)
        print('arquivo salvo')
    except Exception as e:
        print(e)
        print(f'arquivo {filename} não foi salvo.')

print('conversão finalizada.')
