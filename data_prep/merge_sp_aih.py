import pandas as pd
import os

main_path : str = 'C:/Users/Visagio/Projetos/UFRJ/IC/'

aih_df = pd.read_csv(main_path +  'AIH/sih_sus_m_10_49.csv', sep='|')

files = os.listdir(main_path + 'near_miss_ufrj/results/')

for filename in files:

    sp_df = pd.read_csv(main_path + 'near_miss_ufrj/results/AIH_' + filename, sep=';')