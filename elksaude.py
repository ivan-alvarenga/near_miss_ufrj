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
print("Passo 1 de 8:Criando diretórios para arquivos temporários")
directoryTabelas=os.path.expanduser('~/')+".tmpEtlDATASUS_" + str(dadosInput['ano'])
directoryAuxiliares=directoryTabelas+'auxiliares/'
directoryJuncao=directoryAuxiliares+'juncao'
if os.path.exists(directoryTabelas):
    shutil.rmtree(directoryTabelas)

if os.path.exists(directoryJuncao):
    shutil.rmtree(directoryJuncao)

os.makedirs(directoryJuncao)

#download das tabelas e auxiliares
print("Passo 2 de 8:Fazendo download da tabela "+dadosInput['base']+" e das tabelas auxiliares")
time_delta = datetime.datetime.now()
#
if(dadosInput['base']=='SINASC'):
    download_lib.downloadSINASC(dadosInput['ano'], dadosInput['estados'], directoryTabelas)
    download_lib.downloadSINASCAuxiliares(dadosInput['ano'],directoryAuxiliares)
elif(dadosInput['base']=='SIM'):
    download_lib.downloadSIM(dadosInput['ano'], dadosInput['estados'],directoryTabelas)
    download_lib.downloadSIMAuxiliares(dadosInput['ano'],directoryAuxiliares)
elif(dadosInput['base']=='SIH'):
    download_lib.downloadSIH(dadosInput['ano'], dadosInput['estados'],directoryTabelas)
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
print("Passo 3 de 8:Conversão da tabela "+dadosInput['base']+" DBC->DBF")
converteTudoDBC2DBF.converteTudoDBC2DBF(directoryTabelas)
misc.deletaTudoComExtensao(directoryTabelas,'dbc')
misc.deletaTudoComExtensao(directoryTabelas,'DBC')

print("Duração de Conversao de DBC->DBF: " + str(datetime.datetime.now() - time_delta))
time_delta = datetime.datetime.now()
#converte base DBf->CSV
print("Passo 4 de 8:Conversão da tabela "+dadosInput['base']+" DBF->CSV")
converteTudoDBF2CSV.converteTudoDBF2CSV(directoryTabelas)
misc.deletaTudoComExtensao(directoryTabelas,'dbf')
misc.deletaTudoComExtensao(directoryTabelas,'DBF')

print("Duração de Conversao de DBF->CSV: " + str(datetime.datetime.now() - time_delta))
time_delta = datetime.datetime.now()



print("Passo 5 de 8: Limpeza  e normalizacao dos dados das tabelas auxiliares")
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

def remove_virgula_do_campo(row):
    list = row.split(',')
    return str(list[0]) + str(list[1]) + ',' + str(list[2])

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
                dict_tabelas_auxiliares[file] = utils.atualiza_chaves_tabela_auxiliar(file, directoryJuncao)

            dict_tabelas_auxiliares[file] = pd.read_csv(directoryJuncao + '/' + file[:-3] + 'csv', header=0, dtype={'codigo': str, 'valor': str}, error_bad_lines=False)
            dict_tabelas_auxiliares[file] = utils.limpaTabelaAuxiliar(dict_tabelas_auxiliares[file], file)

            if (file[len(file) - 3:] == 'CNV' or file[len(file) - 3:] == 'cnv'):
                dict_tabelas_auxiliares[file].set_index(keys="codigo", verify_integrity=True, inplace=True)
                dict_tabelas_auxiliares[file].index = dict_tabelas_auxiliares[file].index.map(str)

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
        if len(campo_json_do_dataframe['tabelas_auxiliares']) > 1:
            print("Coluna nao foi convertida: " + campo_json_do_dataframe['campo'])
            # size = len(campo_json_do_dataframe['tabelas_auxiliares'])
            # df_junta_result = pd.DataFrame()
            # for i in range(size -1):
            #     aux = dict_tabelas_auxiliares[campo_json_do_dataframe['tabelas_auxiliares'][i]]['valor']
            #     result = dfBase[coluna_do_dataframe].map(lambda x: aux[x] if x in aux else x)
            #     df_junta_result = pd.concat([df_junta_result, result])
            # dfBase[coluna_do_dataframe + '_desc'] = df_junta_result
            # df_junta_aux = pd.DataFrame()
            # for nome_arquivo_auxiliar in campo_json_do_dataframe['tabelas_auxiliares']:
            #     if nome_arquivo_auxiliar in dict_tabelas_auxiliares:
            #         aux = dict_tabelas_auxiliares[nome_arquivo_auxiliar]['valor']
            #         # df_junta_aux = pd.concat([df_junta_aux, aux])
            #         df_junta_aux = pd.concat([df_junta_aux, aux])

            #dfBase[coluna_do_dataframe + '_desc'] = dfBase[coluna_do_dataframe].map(lambda x:
            #                                                                        df_junta_aux[x] if x in df_junta_aux else x)
        if campo_json_do_dataframe['tabelas_auxiliares'][0] in dict_tabelas_auxiliares:
                aux = dict_tabelas_auxiliares[campo_json_do_dataframe['tabelas_auxiliares'][0]]['valor']
                desc = dfBase[coluna_do_dataframe].map(lambda x: aux[x] if x in aux else x)
                dfBase[coluna_do_dataframe + '_desc'] = desc




elastic_search_client = Elasticsearch([dadosInput['elastic_server']], timeout=180)

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


def delete_data_index_by_query_on_elasticsearch(elastic_search_client, database, base, year, region):
    return elastic_search_client.delete_by_query(
        index=database,
        conflicts='proceed',
        body={"query":
                  {"bool":
                       {"must":[
                           {"match":{"_year":year}},
                           {"match":{"_region":region}},
                           {"match":{"_type":str(base).lower()}}
                       ]}
                  }
        }
    )

def create_index_pattern_on_kibana(kibana_server_url, databbase):
    #create index pattern on kibana
    import requests

    payload={
      "attributes": {
        "title": databbase,
        "timeFieldName": 'dtnasc'
      }
    }

    ret = requests.post(kibana_server_url+ "/api/saved_objects/index-pattern/" + databbase, json=payload, headers={"kbn-xsrf":"true"}, verify=False)



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
            # print(doc_id)
            success += 1

    return success, failed, errors


print("Duração do tratamento das tabelas auxiliares e escrita de campos: " + str(datetime.datetime.now() - time_delta))
time_delta = datetime.datetime.now()

print("Passos 6 e 7 de 8: Construção da saída e carregamento dos dados no ElasticSearch")


for file in os.listdir(directoryTabelas):
    if file[-3:] == 'csv' or file[-3:] == 'CSV':
        dfBase = pd.read_csv(directoryTabelas + '/' + file, dtype=str)

        for col in todos_os_campos:
            if col not in dfBase.columns:
                dfBase[col] = None
                print("coluna que não estava na base " +dadosInput['base']  +":"+ col)

        # filtrar colunas para usar apenas as que tem o campo listado no json de entrada
        dfBase = dfBase[todos_os_campos]

        for col in todos_os_campos_com_auxiliar:
            substituiValorOriginalParaValorDasTabelasAuxiliares(col)

        #formatar datas - mudar para pegar campos q tem datetime
        for col in todos_os_campos_tipo_data:
            dfBase[col] = formata_datas(dfBase[col])

        print("Duração de tratamento do arquivo principal: " + str(
            datetime.datetime.now() - time_delta) + ' >>> arquivo: ' + file)
        time_delta = datetime.datetime.now()

        if dadosInput['base']  == 'SIH':
            dfBase.rename(columns={'NASC': 'DTNASC'}, inplace=True)

        print('printando saida')
        dfBase.to_csv(directoryTabelas + 'saida_' + file, index=True)

        # Normalizacao para importacao no ELK -- descomentar 301 a 335
        #Loading to elk
        # garantindo que poderemos rastrear os dados
        region = file[-10:-8]
        year = file[-8:-4]
        dfBase['_region'] = region
        dfBase['_year'] = year

        create_index_or_recreate(elastic_search_client, dadosInput['database'])

        dfBase.replace({pd.NaT: None, pd.np.nan: None}, inplace=True)
        data = normalize(dfBase, dadosInput['database'], dadosInput['base'])

        print('removendo index pelo ano e regiao')
        pprint.pprint('Regiao: ' + str(region))
        pprint.pprint('Ano: ' + str(year))

        # remover dados do elastic search para o  index e doctype conforme o ano e regiao, para evitar duplicidade
        delete_by_query = delete_data_index_by_query_on_elasticsearch(
            elastic_search_client,
            database=dadosInput['database'],
            base=dadosInput['base'],
            year=year,
            region=region
        )

        print('loading data')
        success, failed, errors = load_repo(elastic_search_client, data, dadosInput['database'], dadosInput['base'])


        pprint.pprint('Success quantity: ' + str(success))
        pprint.pprint('Failed quantity : ' + str(failed))
        pprint.pprint('Errors : ' + str(errors))
        pprint.pprint('======================================================================')

        #break #tirar break quando terminarem os testes


#criar index no kibana caso nao exista
print('criando indice no kibana caso nao exista')
create_index_pattern_on_kibana(dadosInput['kibana_server'], dadosInput['database'])




#deletar pasta temporaria usada
print("Passo 8 de 8:Limpeza da pasta temporária")
if os.path.exists(directoryTabelas):
    shutil.rmtree(directoryTabelas)

print("Duração total: " + str(datetime.datetime.now() - time_delta))

print("Fim!")
