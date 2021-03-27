#!/Users/rafaelstomaz/opt/anaconda3
import sys
import json
import os
import shutil
import download_lib
import misc
import converteTudoDBC2DBF
import converteTudoDBF2CSV
import converteTudoCNV2CSV
import pandas as pd
import datetime
import pprint
import utils_tabela_aux as utils
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk
from elasticsearch.exceptions import TransportError

time_delta = datetime.datetime.now()
inicio_etl = datetime.datetime.now()

print("Passo 1 de 8:Criando diretórios para arquivos temporários")
dirDbc="/home/gxfs/Desktop/ufrj/ic_saude/datasets_sus_dbc"
dirDbf="/home/gxfs/Desktop/ufrj/ic_saude/datasets_sus_dbf"
dirCsv="/home/gxfs/Desktop/ufrj/ic_saude/datasets_sus_csv"

if os.path.exists(dirDbc) == False :
  print("dirDbc not exists")
  exit()
if os.path.exists(dirDbf) == False :
  print("dirDbf not exists")
  exit()
if os.path.exists(dirCsv) == False :
  print("dirCsv not exists")
  exit()

time_delta = datetime.datetime.now()

#converte base DBC->DBF
print("Passo 3 de 8:Conversão das tabelas DBC->DBF")
converteTudoDBC2DBF.converteTudoDBC2DBF(dirDbc, dirDbf)
#misc.deletaTudoComExtensao(directoryTabelas,'dbc')
#misc.deletaTudoComExtensao(directoryTabelas,'DBC')

print("Duração de Conversao de DBC->DBF: " + str(datetime.datetime.now() - time_delta))
time_delta = datetime.datetime.now()

#converte base DBf->CSV
print("Passo 4 de 8:Conversão das tabelas DBF->CSV")
converteTudoDBF2CSV.converteTudoDBF2CSV(dirDbf, dirCsv)
#misc.deletaTudoComExtensao(directoryTabelas,'dbf')
#misc.deletaTudoComExtensao(directoryTabelas,'DBF')

print("Duração de Conversao de DBF->CSV: " + str(datetime.datetime.now() - time_delta))
time_delta = datetime.datetime.now()

print("Duração total: " + str(datetime.datetime.now() - time_delta))

print("Fim!")
