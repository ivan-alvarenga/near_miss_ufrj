#!/Users/rafaelstomaz/opt/anaconda3
import sys
import json
import os
import shutil
from datetime import date

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
import pprint
# from etl_lib import EtlPipeline

#captura do JSON com instrucoes para o ETL
try:
    # print(sys.argv[1])
    JSONFile = sys.argv[1]
except:
    print("usage: etl arquivo_de_entrada.json")
    exit()
# if(JSONFile==None or JSONFile==""):
    print("usage: etl arquivo_de_entrada.json")
    exit()

with open(JSONFile, encoding='utf-8') as json_file:
    dadosInput = json.load(json_file)

#criacao da pasta temporaria ~/.tmpEtlDATASUS/auxiliares/juncao/
# print("1 de 9:Criando diretórios para arquivos temporários")
directoryTabelas=os.path.expanduser('~/')+".tmpEtlDATASUS_RJ_" + str(dadosInput['anos'][0]) + "/" + dadosInput['base']
directoryAuxiliares=directoryTabelas+'auxiliares/'
directoryJuncao=directoryAuxiliares+'juncao'



print("Passo  6: Limpeza  e normalizacao dos dados das tabelas auxiliares")
dict_tabelas_auxiliares = dict()

def adiciona_zero_nas_horas(hora):
    hora = str(hora)
    if len(hora) == 1:
        hora = '000' + hora
    elif len(hora) == 2:
        hora = '00' + hora
    elif len(hora) == 3:
        hora = '0' + hora
    return hora

def remove_codigo(str):
    if len(str.split(' ', 1)) > 1:
        return str.split(' ', 1)[1]
    return str

#funcao para limpar casos especiais das tabelas auxilixares
def limpaTabelaAuxiliar(df_tabelas_auxiliares, file):
    df_tabelas_auxiliares = df_tabelas_auxiliares[df_tabelas_auxiliares['valor'].notna()]
    if file[:-4] == 'FILTIDO':
        df_tabelas_auxiliares.drop(df_tabelas_auxiliares.index[0], inplace=True)
    elif file[:-4] == 'IDADEMAE':
        df_tabelas_auxiliares = df_tabelas_auxiliares[~df_tabelas_auxiliares["valor"].str.contains('Ign')]
    # elif file[:-4] == 'idanomal': descomentar
        # dict_tabelas_auxiliares.drop(dict_tabelas_auxiliares.index[10], inplace=True)
    elif file[:-4] == 'PARTO':
        df_tabelas_auxiliares.replace(to_replace='Cesário', value='Cesario', inplace=True)
    elif file[:-4] == 'PESO':
        df_tabelas_auxiliares.drop(
            df_tabelas_auxiliares[df_tabelas_auxiliares["valor"].str.contains('ign') == True].index, inplace=True
        )
        df_tabelas_auxiliares['codigo'] = df_tabelas_auxiliares['codigo'].apply(adiciona_zero_nas_horas)
    elif file[:-4] == 'HORAOBITO':
        df_tabelas_auxiliares['codigo'] = df_tabelas_auxiliares['codigo'].apply(adiciona_zero_nas_horas)
    elif file[:-4] == 'MUNICBR':
        df_tabelas_auxiliares['valor'] = df_tabelas_auxiliares['valor'].apply(remove_codigo)

    return df_tabelas_auxiliares


def atualiza_chaves_tabela_auxiliar(nome_tabela_auxiliar):
    dados = pd.read_csv(directoryJuncao + '/' + nome_tabela_auxiliar[:-3]+'csv')
    nome_tabela_sem_extensao = nome_tabela_auxiliar[:-4]
    if nome_tabela_sem_extensao == 'CNESDN18':
        dados.rename(columns={'CODESTAB': 'codigo', 'DESCESTAB': 'valor'}, inplace=True)
        dados.to_csv(directoryJuncao + '/' + nome_tabela_sem_extensao + '.csv', index=True)
    return dados

todos_os_campos = []
todos_os_campos_com_auxiliar = []
todos_os_campos_tipo_data = []

for campo in dadosInput['campos']:
    todos_os_campos.append(campo['campo'])
    if "tipo_campo_convertido" in campo and campo["tipo_campo_convertido"] == "datetime":
        todos_os_campos_tipo_data.append(campo["campo"])
    elif("tabelas_auxiliares" in campo):
        todos_os_campos_com_auxiliar.append(campo['campo'])
        #misc.deletaTudo(directoryJuncao)
        for file in campo['tabelas_auxiliares']:
            shutil.copyfile(directoryAuxiliares+file, directoryJuncao+'/'+file)
        if(campo['tabelas_auxiliares'][0][len(campo['tabelas_auxiliares'][0])-3:]=='CNV' or campo['tabelas_auxiliares'][0][len(campo['tabelas_auxiliares'][0])-3:]=='cnv'):
            converteTudoCNV2CSV.converteTudoCNV2CSV(directoryJuncao)
            # misc.deletaTudoComExtensao(directoryJuncao,'CNV')
            # misc.deletaTudoComExtensao(directoryJuncao,'cnv')
            dict_tabelas_auxiliares[file] = pd.read_csv(directoryJuncao + '/' + campo['tabelas_auxiliares'][0].upper().split('.CNV')[0] + '.csv', header=0, dtype={'codigo': str, 'valor': str})
            dict_tabelas_auxiliares[file] = limpaTabelaAuxiliar(dict_tabelas_auxiliares[file], file)
            dict_tabelas_auxiliares[file].set_index(keys="codigo", verify_integrity=True, inplace=True)
            dict_tabelas_auxiliares[file].index = dict_tabelas_auxiliares[file].index.map(str)

        # elif(campo['tabelas_auxiliares'][0][len(campo['tabelas_auxiliares'][0])-3:]=='DBC' or campo['tabelas_auxiliares'][0][len(campo['tabelas_auxiliares'][0])-3:]=='dbc'):
        #     converteTudoDBC2DBF.converteTudoDBC2DBF(directoryJuncao)
        #     # misc.deletaTudoComExtensao(directoryJuncao,'DBC')
        #     # misc.deletaTudoComExtensao(directoryJuncao,'dbc')
        #     converteTudoDBF2CSV.converteTudoDBF2CSV(directoryJuncao)
        #     # misc.deletaTudoComExtensao(directoryJuncao,'DBF')
        #     # misc.deletaTudoComExtensao(directoryJuncao,'dbf')
        elif(campo['tabelas_auxiliares'][0][len(campo['tabelas_auxiliares'][0])-3:]=='DBF' or campo['tabelas_auxiliares'][0][len(campo['tabelas_auxiliares'][0])-3:]=='dbf'):
            converteTudoDBF2CSV.converteTudoDBF2CSV(directoryJuncao)
            dict_tabelas_auxiliares[file] = atualiza_chaves_tabela_auxiliar(campo['tabelas_auxiliares'][0])
            dict_tabelas_auxiliares[file] = pd.read_csv(directoryJuncao + '/' + campo['tabelas_auxiliares'][0][:-3] + 'csv', header=0, dtype={'codigo': str, 'valor': str})
            dict_tabelas_auxiliares[file] = limpaTabelaAuxiliar(dict_tabelas_auxiliares[file], file)
            dict_tabelas_auxiliares[file].set_index(keys="codigo", verify_integrity=True, inplace=True)
            # dict_tabelas_auxiliares[file].set_index(keys="CODESTAB", verify_integrity=True, inplace=True)
            dict_tabelas_auxiliares[file].index = dict_tabelas_auxiliares[file].index.map(str)
        #     # misc.deletaTudoComExtensao(directoryJuncao,'DBF')
        #     # misc.deletaTudoComExtensao(directoryJuncao,'dbf')


def adiciona_zero_nos_dias(data):
    data = str(data)
    if len(data) == 7:
        data = '0' + data
    return data

def formata_datas(coluna_da_base):
    coluna_da_base = coluna_da_base.apply(adiciona_zero_nos_dias)
    return pd.to_datetime(coluna_da_base.astype(str), format='%d%m%Y', errors='coerce')

def substituiValorOriginalParaValorDasTabelasAuxiliares(coluna_do_dataframe):
    campo_json_do_dataframe = next((item for item in dadosInput['campos'] if item["campo"] == coluna_do_dataframe), None)
    if campo_json_do_dataframe is not None and 'tabelas_auxiliares' in campo_json_do_dataframe:
        for nome_arquivo_auxiliar in campo_json_do_dataframe['tabelas_auxiliares']:
            if nome_arquivo_auxiliar in dict_tabelas_auxiliares:
                aux = dict_tabelas_auxiliares[nome_arquivo_auxiliar]['valor']
                dfBase[coluna_do_dataframe + '_desc'] = dfBase[coluna_do_dataframe].map(lambda x: aux[x] if x in aux else x)


from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk
from elasticsearch.exceptions import TransportError

elastic_search_client = Elasticsearch([dadosInput['elastic_server']])

# Criando o indice para ser utilizado
# entrar no console do kibana em Management, selecionar o index e clicar
# em manage index e deletar index e rodar novamente
# Importante para primeiro acesso
def create_index_or_recreate(elastic_search_client, database):
    create_index_body = {
      'settings': {
        # just one shard, no replicas for testing
        'number_of_shards': 1,
        'number_of_replicas': 0
      }
    }

    # create empty index
    try:
        elastic_search_client.indices.create(
            index=str(database).lower(),
            body=create_index_body
        )
    except TransportError as e:
        # ignore already existing index
        if e.error in ['index_already_exists_exception', 'resource_already_exists_exception']:
            print('index - index alread_exists --')
            pass
        else:
            raise

# Normalizar as chaves para letras minusculas pq o elastic search eh case sensitive
# Isso evita duplicatas  erradas (Mes == mes)
def normalize(data, database, base):
    data = data.to_dict('records')

    for row in data:
        yield {"_index": str(database).lower(),
               "_type": str(base).lower(),
               "_source": {str(k).lower(): v for k, v in row.items()}}

# carregar o dado no ELK em streaming
def load_repo(elastic_search_client, data, database, base):
    create_index_or_recreate(elastic_search_client, database)

    # quantidade de itens processados com sucesso e falha
    success, failed = 0, 0

    # list of errors to be collected is not stats_only
    errors = []

    # se true somente printar a informacao do erro e contar as  falhas
    # se false, acumular os erros na lista criada acima
    stats_only = True

    pprint.pprint(data)
    pprint.pprint('for a do streaming ' + base)

    # we let the streaming bulk continuously process the commits as they come
    # in - since the `parse_commits` function is a generator this will avoid
    # loading all the commits into memory
    for ok, result in streaming_bulk(
            elastic_search_client,
            actions=data
    ):
        action, result = result.popitem()
        doc_id = '/%s/doc/%s' % (str(database).lower(), result['_id'])
        # process the information from ES whether the document has been
        # successfully indexed
        if not ok:
            if not stats_only:
                errors.append(result)
            print('Failed to %s document %s: %r' % (action, doc_id, result))
            failed += 1
        else:
            print(doc_id)
            success += 1

    return success, failed, errors


for file in os.listdir(directoryTabelas):
    if file[-3:] == 'csv' or file[-3:] == 'CSV':
        # print('\n\n############################################################')
        # print('\n\n Processando arquivo de base: ' + file + '\n')
        dfBase = pd.read_csv(directoryTabelas + '/' + file, dtype=str)

        for col in todos_os_campos:
            if col not in dfBase.columns:
                dfBase[col] = None

        # filtrar colunas para usar apenas as que tem o campo listado no json de entrada
        dfBase = dfBase[todos_os_campos]

        for col in todos_os_campos_com_auxiliar:
            substituiValorOriginalParaValorDasTabelasAuxiliares(col)

        #formatar datas - mudar para pegar campos q tem datetime
        for col in todos_os_campos_tipo_data:
            dfBase[col] = formata_datas(dfBase[col])

        print('printando saida')
        dfBase.to_csv(directoryTabelas + 'saida_' + str(time.time()) + file, index=True)

        # Normalizacao para importacao no ELK
        #Loading to elk
        # garantindo que poderemos rastrear os dados
        dfBase['_region'] = file[-10:-8]
        dfBase['_year'] = file[-8:-4]

        dfBase.replace({pd.NaT: None, pd.np.nan: None}, inplace=True)
        data = normalize(dfBase, dadosInput['database'], dadosInput['base'])

        success, failed, errors = load_repo(elastic_search_client, data, dadosInput['database'], dadosInput['base'])

        pprint.pprint('Sucess quantity: ' + str(success))
        pprint.pprint('Failed quantity : ' + str(failed))
        pprint.pprint('Errors : ' + str(errors))

        break #tirar break quando terminarem os testes






