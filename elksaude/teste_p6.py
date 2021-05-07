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
import pprint
# from etl_lib import EtlPipeline

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

with open(JSONFile, encoding='utf-8') as json_file:
    dadosInput = json.load(json_file)

#criacao da pasta temporaria ~/.tmpEtlDATASUS/auxiliares/juncao/
print("1 de 9:Criando diretórios para arquivos temporários")
directoryTabelas=os.path.expanduser('~/')+".tmpEtlDATASUS/"+dadosInput['base']
directoryAuxiliares=directoryTabelas+'auxiliares/'
directoryJuncao=directoryAuxiliares+'juncao'
# if os.path.exists(directoryTabelas):
#     shutil.rmtree(directoryTabelas)
# os.makedirs(directoryJuncao)


#importa as tabelas auxiliares para o MongoDb
print("6 de 9:Importação das tabelas auxiliares para o MongoDB")

for campo in dadosInput['campos']:
    if("tabelas_auxiliares" in campo):
        for file in campo['tabelas_auxiliares']:
            shutil.copyfile(directoryAuxiliares+file, directoryJuncao+'/'+file)
        if(campo['tabelas_auxiliares'][0][len(campo['tabelas_auxiliares'][0])-3:]=='CNV' or campo['tabelas_auxiliares'][0][len(campo['tabelas_auxiliares'][0])-3:]=='cnv'):
            converteTudoCNV2CSV.converteTudoCNV2CSV(directoryJuncao)
            misc.deletaTudoComExtensao(directoryJuncao+ '/','CNV')
            misc.deletaTudoComExtensao(directoryJuncao+ '/','cnv')
        elif(campo['tabelas_auxiliares'][0][len(campo['tabelas_auxiliares'][0])-3:]=='DBC' or campo['tabelas_auxiliares'][0][len(campo['tabelas_auxiliares'][0])-3:]=='dbc'):
            converteTudoDBC2DBF.converteTudoDBC2DBF(directoryJuncao)
            misc.deletaTudoComExtensao(directoryJuncao+ '/','DBC')
            misc.deletaTudoComExtensao(directoryJuncao+ '/','dbc')
            converteTudoDBF2CSV.converteTudoDBF2CSV(directoryJuncao)
            misc.deletaTudoComExtensao(directoryJuncao+ '/','DBF')
            misc.deletaTudoComExtensao(directoryJuncao+ '/','dbf')
        elif(campo['tabelas_auxiliares'][0][len(campo['tabelas_auxiliares'][0])-3:]=='DBF' or campo['tabelas_auxiliares'][0][len(campo['tabelas_auxiliares'][0])-3:]=='dbf'):
            converteTudoDBF2CSV.converteTudoDBF2CSV(directoryJuncao)
            misc.deletaTudoComExtensao(directoryJuncao+ '/','DBF')
            misc.deletaTudoComExtensao(directoryJuncao+ '/','dbf')


#csv to axiliarDict
print("7 de 9:Conversão de csv to dict "+dadosInput['base']+" csv->dict")
auxiliarDict = dict()
# misc.deletaTudo(directoryJuncao + '/')
for campo in dadosInput['campos']:
    if("tabelas_auxiliares" in campo):
        for file in campo['tabelas_auxiliares']:
            f = file[:-4]
            if not file in auxiliarDict:
                auxiliarDict[file] = pd.read_csv(directoryJuncao+'/'+f + '.csv')


pprint.pprint(auxiliarDict)


def substituiValorOriginalParaValorDasTabelasAuxiliares(campo, auxiliarDictFile, dfBase, fileName):
    if(fileName[-3:]=='CNV' or fileName[-3:]=='CNV'):
        auxiliarDictFileAsIndex = auxiliarDictFile.set_index('codigo')['valor']
        dfBase[campo].astype(campo['tipo_valor_auxiliar'], inline=True)
        dfBase[campo].map(auxiliarDictFileAsIndex)



#substituir valores da tabela originnal

for file in os.listdir(directoryTabelas):
    if file[-3:] == 'csv' or file[-3:] == 'CSV':
        df = pd.read_csv(directoryTabelas + '/' + file)
        for campo in dadosInput['campos']:
            if 'tabelas_auxiliares' in campo:
                substituiValorOriginalParaValorDasTabelasAuxiliares(campo, auxiliarDict[campo['tabelas_auxiliares']], dfBase, campo['tabelas_auxiliares'])


#             colunaDfBase = colunaDfBase.replace(valor, auxiliarDictFileWithIndex[auxiliarDictFileWithIndex['codigo'] == valor]['valor'])
#
# substituiValorOriginalParaValorDasTabelasAuxiliares('SEXO', df_aux, dfBase['SEXO'])
# your_function, args=(2,3,4)
#
#  df_aux = auxiliarDict['SEXO.CNV']
#
#  d = df_aux.set_index('codigo')['valor']

print("Final:Sucesso")
