#!/Users/rafaelstomaz/opt/anaconda3
import sys
import json
import os
import pandas as pd
import tabulate

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


print("2 de 9:Convertendo tipo dos dados")


# criando dicionario de tipos e das colunas da base
dict_tipos = dict()

for x in dadosInput['campos']:
    if 'tipo_valor_auxiliar' in x:
        dict_tipos[x['campo']] = x['tipo_valor_auxiliar']
print(dict_tipos)


dict_tipos_aux = dict()
for x in dadosInput['campos']:
    if 'tipo_campo_convertido' in x and 'tipo_valor_auxiliar' in x:
        dict_tipos_aux['codigo'] = x['tipo_valor_auxiliar']
        dict_tipos_aux['valor'] = x['tipo_campo_convertido']
    elif 'tipo_valor_auxiliar' in x:
        dict_tipos_aux['codigo'] = x['tipo_valor_auxiliar']
        dict_tipos_aux['valor'] = x['tipo_valor_auxiliar']
print(dict_tipos_aux)

data_copy = pd.read_csv(directoryTabelas + '/SINASCDNAP2015.csv')
data = pd.read_csv(directoryTabelas + '/SINASCDNAP2015.csv', dtype=dict_tipos, na_values=['not_available'])

data.info()


print("3 de 9: convertendo dados")


auxiliarDictSexo = pd.read_csv(directoryJuncao+'/FILTIDO'+ '.csv', header=0, dtype=dict_tipos_aux)

#funcao para limpar tabelas
#if FILTIDO
#auxiliarDictSexo.drop(auxiliarDictSexo.index[0], inplace=True)




auxiliarDictSexo.set_index(keys="codigo", verify_integrity=True, inplace=True)
#auxiliarDictSexoAsIndex = auxiliarDictSexo.set_index('codigo')['valor']

print(data["QTDFILMORT"])
print("auxiliarDictSexo['valor']")
print(auxiliarDictSexo['valor'])
data["QTDFILMORT"] = data["QTDFILMORT"].map(auxiliarDictSexo['valor'])
print(data["QTDFILMORT"])


