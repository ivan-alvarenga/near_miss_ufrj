#!/Users/rafaelstomaz/opt/anaconda3
import sys
import json
import os
import shutil
import download_lib
import misc
import zipfile
from os import listdir
from os.path import isfile, join
import converteTudoDBC2DBF
import converteTudoDBF2CSV
import importTudoCSV
import converteTudoCNV2CSV
import pandas as pd
import time
import datetime
import utils_tabela_aux as utils

# from etl_lib import EtlPipeline

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk
from elasticsearch.exceptions import TransportError

inicio_etl = datetime.datetime.now()

#captura do JSON com instrucoes para o ETL
try:
    print(sys.argv[1])
    JSONFile = sys.argv[1]
except:
    print("usage: etl arquivo_de_entrada.json")
    exit()
if(JSONFile==None or JSONFile==""):
    print("usage: etl arquivo_de_entrada.json")
    exit()

with open(JSONFile) as json_file:
    dadosInput = json.load(json_file)

#criacao da pasta temporaria ~/.tmpEtlDATASUS/auxiliares/juncao/
print("1 de 9:Criando diretórios para arquivos temporários")
directoryTabelas=os.path.expanduser('~/')+".tmpEtlDATASUS_RJ_21_dez_sim/" + dadosInput['base']
directoryAuxiliares=directoryTabelas+'auxiliares/'
directoryJuncao=directoryAuxiliares+'juncao'
if os.path.exists(directoryTabelas):
    shutil.rmtree(directoryTabelas)

if os.path.exists(directoryJuncao):
    shutil.rmtree(directoryJuncao)

os.makedirs(directoryJuncao)

#download das tabelas e auxiliares
print("2 de 9:Fazendo download da tabela "+dadosInput['base']+" e das tabelas auxiliares")
time_delta = datetime.datetime.now()

if(dadosInput['base']=='SINASC'):
    download_lib.downloadSINASC(dadosInput['anos'], dadosInput['estados'], directoryTabelas)
    download_lib.downloadSINASCAuxiliares(dadosInput['ano'],directoryAuxiliares)
elif(dadosInput['base']=='SIM'):
    download_lib.downloadSIM(dadosInput['anos'], dadosInput['estados'],directoryTabelas)
    download_lib.downloadSIMAuxiliares(dadosInput['ano'],directoryAuxiliares)
elif(dadosInput['base']=='SIH'):
    download_lib.downloadSIH(dadosInput['anos'], dadosInput['estados'],directoryTabelas)
    download_lib.downloadSIHAuxiliares(dadosInput['ano'],directoryAuxiliares)
else:
    print('erro: base precisa ser "SIM" ou "SINASC" ou "SIH"')
    exit()

print("Duração de download: " + str(datetime.datetime.now() - time_delta))
time_delta = datetime.datetime.now()
#descompactacao do arquivo com tabelas auxiliares

misc.descompactaTudoZIP(directoryAuxiliares)
print("Duração de descompactar Zip: " + str(datetime.datetime.now() - time_delta))
time_delta = datetime.datetime.now()

#converte base DBC->DBF
print("3 de 9:Conversão da tabela "+dadosInput['base']+" DBC->DBF")
converteTudoDBC2DBF.converteTudoDBC2DBF(directoryTabelas)
misc.deletaTudoComExtensao(directoryTabelas,'dbc')
misc.deletaTudoComExtensao(directoryTabelas,'DBC')

print("Duração de Conversao de DBC->DBF: " + str(datetime.datetime.now() - time_delta))
time_delta = datetime.datetime.now()
#converte base DBf->CSV
print("4 de 9:Conversão da tabela "+dadosInput['base']+" DBF->CSV")
converteTudoDBF2CSV.converteTudoDBF2CSV(directoryTabelas)
misc.deletaTudoComExtensao(directoryTabelas,'dbf')
misc.deletaTudoComExtensao(directoryTabelas,'DBF')

print("Duração de Conversao de DBF->CSV: " + str(datetime.datetime.now() - time_delta))
time_delta = datetime.datetime.now()

print("Passo  5: Limpeza  e normalizacao dos dados das tabelas auxiliares - este processamento leva tempo")
dict_tabelas_auxiliares = dict()


todos_os_campos = []
todos_os_campos_com_auxiliar = []
todos_os_campos_tipo_data = []

#copia para a pasta /juncao todos os arquivos auxiliares
for campo in dadosInput['campos']:
    todos_os_campos.append(campo['campo'])
    if "tipo_campo_convertido" in campo and campo["tipo_campo_convertido"] == "datetime":
        todos_os_campos_tipo_data.append(campo["campo"])
    elif("tabelas_auxiliares" in campo):
        todos_os_campos_com_auxiliar.append(campo['campo'])
        #misc.deletaTudo(directoryJuncao)
        for file in campo['tabelas_auxiliares']:
            if dadosInput['base'] == 'SIH':
                if (file[len(file) - 3:] == 'CNV' or file[len(file) - 3:] == 'cnv'):
                    shutil.copyfile(directoryAuxiliares + 'CNV/' + file, directoryJuncao + '/' + file)
                elif (file[len(file) - 3:] == 'DBF' or file[len(file) - 3:] == 'dbf'):
                    shutil.copyfile(directoryAuxiliares + 'DBF/' + file, directoryJuncao + '/' + file)
            else:
                shutil.copyfile(directoryAuxiliares+file, directoryJuncao+'/'+file)
            print("arquivo " + file)

# conversao de todos os arquivos para csv
converteTudoCNV2CSV.converteTudoCNV2CSV(directoryJuncao)
misc.deletaTudoComExtensao(directoryJuncao,'CNV')
misc.deletaTudoComExtensao(directoryJuncao,'cnv')
converteTudoDBF2CSV.converteTudoDBF2CSV(directoryJuncao)
misc.deletaTudoComExtensao(directoryJuncao,'DBF')
misc.deletaTudoComExtensao(directoryJuncao,'dbf')


# guarda tabelas auxiliares normalizadas num df
for campo in dadosInput['campos']:
    if ("tabelas_auxiliares" in campo):
        for file in campo['tabelas_auxiliares']:
            if (file[len(file) - 3:] == 'DBF' or file[len(file) - 3:] == 'dbf'):
                print("_______________________________ " + file)
                dict_tabelas_auxiliares[file] = utils.atualiza_chaves_tabela_auxiliar(file, directoryJuncao)

            dict_tabelas_auxiliares[file] = pd.read_csv(directoryJuncao + '/' + file[:-3] + 'csv', header=0, dtype={'codigo': str, 'valor': str}, error_bad_lines=False)
            dict_tabelas_auxiliares[file] = utils.limpaTabelaAuxiliar(dict_tabelas_auxiliares[file], file)
            print("indexando " + file)
            dict_tabelas_auxiliares[file].set_index(keys="codigo", verify_integrity=True, inplace=True)
            dict_tabelas_auxiliares[file].index = dict_tabelas_auxiliares[file].index.map(str)


print("fim")