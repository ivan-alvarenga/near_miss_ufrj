import pandas as pd
import os

main_path : str = 'C:/Users/Visagio/Projetos/UFRJ/IC/'

print('Initializing...')
print('reading AIH database')
aih_df = pd.read_csv(main_path +  'AIH/sih_sus_m_10_49.csv', sep='|')
print('read complete')

files = os.listdir(main_path + 'near_miss_ufrj/results/')

for filename in files:
    # read sp
    print(f'reading {filename}')
    sp_df = pd.read_csv(main_path + 'near_miss_ufrj/results/' + filename, sep=';')
    print('read complete')
    # merge
    print('merging with aih database')
    merged_df = sp_df.merge(aih_df, left_on='SP_NAIH', right_on='N_AIH', how='left')
    print('merge complete')
    # remove NaN
    print('removing NaN rows from data')
    clean_df = merged_df[merged_df['UF_ZI'].notna()]
    print('removal complete')
    # select columns
    print('selecting a subset of the columns')
    clean_df = clean_df[['UF_ZI', 'ANO_CMPT', 'MES_CMPT', 'N_AIH', 'IDENT', 'CEP', 'MUNIC_RES', 'UTI_MES_TO',
    'PROC_SOLIC', 'PROC_REA', 'IND_VDRL', 'MORTE', 'CAR_INT', 'NUM_FILHOS', 'CID_NOTIF', 'GESTRISCO', 'CID_ASSO',
    'CID_MORTE', 'ETNIA' ,'SP_AA', 'SP_MM', 'SP_NAIH', 'SP_DTINTER', 'SP_DTSAIDA', 'SP_CIDPRI', 'SP_CIDSEC', 'SP_U_AIH']]
    print('selection complete')
    # get sample
    print('selecting a sample of the rows')
    sample_df = clean_df.sample(frac=0.2, random_state=42)
    print('selection complete')
    # save the data frame
    print('saving file')
    sample_df.to_csv('../merged_results/aih_' + filename, sep=';', index=False)
    print('file successful saved.')
    print('---------------------------------------------------------------')