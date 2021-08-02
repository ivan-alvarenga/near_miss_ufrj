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

with open(JSONFile) as json_file:  
    dadosInput = json.load(json_file)

#criacao da pasta temporaria ~/.tmpEtlDATASUS/auxiliares/juncao/
print("1 de 9:Criando diretórios para arquivos temporários")
directoryTabelas=os.path.expanduser('~/')+".tmpEtlDATASUS/"+dadosInput['base']
directoryAuxiliares=directoryTabelas+'auxiliares/'
directoryJuncao=directoryAuxiliares+'juncao'
if os.path.exists(directoryTabelas):
    shutil.rmtree(directoryTabelas)
os.makedirs(directoryJuncao)

#download das tabelas e auxiliares
print("2 de 9:Fazendo download da tabela "+dadosInput['base']+" e das tabelas auxiliares")
if(dadosInput['base']=='SINASC'):
    download_lib.downloadSINASC(dadosInput['ano'],directoryTabelas)
    download_lib.downloadSINASCAuxiliares(dadosInput['ano'],directoryAuxiliares)
elif(dadosInput['base']=='SIM'):
    download_lib.downloadSIM(dadosInput['ano'],directoryTabelas)
    download_lib.downloadSIMAuxiliares(dadosInput['ano'],directoryAuxiliares)
elif(dadosInput['base']=='SIH'):
    download_lib.downloadSIH(dadosInput['ano'],directoryTabelas)
    download_lib.downloadSIHAuxiliares(dadosInput['ano'],directoryAuxiliares)
else:
    print('erro: base precisa ser "SIM" ou "SINASC" ou "SIH"')
    exit()

#descompactacao do arquivo com tabelas auxiliares
misc.descompactaTudoZIP(directoryAuxiliares)

#converte base DBC->DBF
print("3 de 9:Conversão da tabela "+dadosInput['base']+" DBC->DBF")
converteTudoDBC2DBF.converteTudoDBC2DBF(directoryTabelas)
# misc.deletaTudoComExtensao(directoryTabelas,'dbc')
# misc.deletaTudoComExtensao(directoryTabelas,'DBC')

#converte base DBf->CSV
print("4 de 9:Conversão da tabela "+dadosInput['base']+" DBF->CSV")
converteTudoDBF2CSV.converteTudoDBF2CSV(directoryTabelas)
# misc.deletaTudoComExtensao(directoryTabelas,'dbf')
# misc.deletaTudoComExtensao(directoryTabelas,'DBF')

#importa a tabela principal para o MongoDB
print("5 de 9:Importação da tabela "+dadosInput['base']+" para o MongoDB")
databaseName=dadosInput['database']
collectionPrincipalName='datasus_original'
# mongoMisc=MongoMisc(databaseName)
# mongoMisc.deletaCollection(collectionPrincipalName)
importTudoCSV.importTudoCSV(databaseName,directoryTabelas,nome=collectionPrincipalName,drop=False)


#importa as tabelas auxiliares para o MongoDb
print("6 de 9:Importação das tabelas auxiliares para o MongoDB")
for campo in dadosInput['campos']:
    if("tabelas_auxiliares" in campo):
        misc.deletaTudo(directoryJuncao)
        for file in campo['tabelas_auxiliares']:
            shutil.copyfile(directoryAuxiliares+file, directoryJuncao+'/'+file)
        if(campo['tabelas_auxiliares'][0][len(campo['tabelas_auxiliares'][0])-3:]=='CNV' or campo['tabelas_auxiliares'][0][len(campo['tabelas_auxiliares'][0])-3:]=='cnv'):
            converteTudoCNV2CSV.converteTudoCNV2CSV(directoryJuncao)
#             misc.deletaTudoComExtensao(directoryJuncao,'CNV')
#             misc.deletaTudoComExtensao(directoryJuncao,'cnv')
#             importTudoCSV.importTudoCSV(databaseName,directoryJuncao,nome=campo['campo']+'_tmp',drop=False)
        elif(campo['tabelas_auxiliares'][0][len(campo['tabelas_auxiliares'][0])-3:]=='DBC' or campo['tabelas_auxiliares'][0][len(campo['tabelas_auxiliares'][0])-3:]=='dbc'):
            converteTudoDBC2DBF.converteTudoDBC2DBF(directoryJuncao)
#             misc.deletaTudoComExtensao(directoryJuncao,'DBC')
#             misc.deletaTudoComExtensao(directoryJuncao,'dbc')
            converteTudoDBF2CSV.converteTudoDBF2CSV(directoryJuncao)
#             misc.deletaTudoComExtensao(directoryJuncao,'DBF')
#             misc.deletaTudoComExtensao(directoryJuncao,'dbf')
#             importTudoCSV.importTudoCSV(databaseName,directoryJuncao,nome=campo['campo']+'_tmp',drop=False)
        elif(campo['tabelas_auxiliares'][0][len(campo['tabelas_auxiliares'][0])-3:]=='DBF' or campo['tabelas_auxiliares'][0][len(campo['tabelas_auxiliares'][0])-3:]=='dbf'):
            converteTudoDBF2CSV.converteTudoDBF2CSV(directoryJuncao)
#             misc.deletaTudoComExtensao(directoryJuncao,'DBF')
#             misc.deletaTudoComExtensao(directoryJuncao,'dbf')
#             importTudoCSV.importTudoCSV(databaseName,directoryJuncao,nome=campo['campo']+'_tmp',drop=False)


# #executa o ETL
# print("7 de 9:Execução do pipeline do MongoDB com o ETL, seja paciente, essa etapa pode demorar horas")
# etl=EtlPipeline(databaseName,collectionPrincipalName,dadosInput['collection'])
# for campo in dadosInput['campos']:
#     etl.addCampoUsado(campo['campo'])
# for campo in dadosInput['campos']:
#     if("tabelas_auxiliares" in campo):
#         etl.addJuncao(campo['campo'],campo['campo']+'_tmp',campo['campo_auxiliar_juncao'])
# etl.executaEtl()
#
# #limpeza das tabelas auxiliares do MongoDB
# print("8 de 9:Limpeza das tabelas auxiliares do MongoDB")
# for campo in dadosInput['campos']:
#     if("tabelas_auxiliares" in campo):
#         mongoMisc.deletaCollection(campo['campo']+'_tmp')
# mongoMisc.deletaCollection(collectionPrincipalName)
#
# #deletar pasta temporaria usada
# print("9 de 9:Limpeza da pasta temporária")
# '''if os.path.exists(directoryTabelas):
#     shutil.rmtree(directoryTabelas)'''

print("Final:Sucesso")
