import pandas as pd

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
    # sinasc
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
    # sinasc
    # sim
    elif file[:-4] == 'CID10_20':
        df_tabelas_auxiliares.drop(df_tabelas_auxiliares.index[2219], inplace=True)
    elif file[:-4] == 'ESTAB06':
        df_tabelas_auxiliares.drop_duplicates(subset=['codigo'], inplace=True)
    elif file[:-4] == 'FONTINFO':
        df_tabelas_auxiliares.drop_duplicates(subset=['codigo'], inplace=True)
    elif file[:-4] == 'IDADE':
        df_tabelas_auxiliares.drop(
            df_tabelas_auxiliares[df_tabelas_auxiliares["valor"].str.contains('Ignorado') == True].index, inplace=True)
    elif file[:-4] in ['NATUR', 'natjur']:
        aux = df_tabelas_auxiliares[df_tabelas_auxiliares['codigo'].duplicated(keep=False)].groupby('codigo')[
            'valor'].apply(' | '.join).reset_index()
        df_tabelas_auxiliares.drop(df_tabelas_auxiliares[df_tabelas_auxiliares['codigo'].duplicated(keep=False)].index,
                                   inplace=True)
        df_tabelas_auxiliares = pd.concat([df_tabelas_auxiliares, aux])
    elif file[:-4] in ['NUMFILH', 'SEXOC', 'TIPOBITO', 'TCHBR', 'IDENT', 'SEXO', 'NATUREZA', 'INSTRU', 'CONTRAC', 'VINCPREV']:
        df_tabelas_auxiliares.drop_duplicates(subset=['codigo'], inplace=True, keep='last')

    return df_tabelas_auxiliares


def atualiza_chaves_tabela_auxiliar(nome_tabela_auxiliar, directoryJuncao):
    dados = pd.read_csv(directoryJuncao + '/' + nome_tabela_auxiliar[:-3]+'csv')
    nome_tabela_sem_extensao = nome_tabela_auxiliar[:-4]
    print(dados.columns)
    if nome_tabela_sem_extensao == 'CNESDN18':
        dados.rename(columns={'CODESTAB': 'codigo', 'DESCESTAB': 'valor'}, inplace=True)
    elif nome_tabela_sem_extensao == 'TCHBR':
        dados.rename(columns={'CGC_HOSP': 'codigo', 'RAZAO': 'valor'}, inplace=True)
    elif nome_tabela_sem_extensao in ['TB_SIGTAP', 'TCNESAC', 'TCNESAL', 'TCNESAM', 'TCNESAP', 'TCNESBA',"TCNESCE","TCNESDF","TCNESES","TCNESGO",
            "TCNESMA","TCNESMG","TCNESMS","TCNESMT","TCNESPA",
            "TCNESPB","TCNESPE","TCNESPI","TCNESPR","TCNESRJ",
            "TCNESRN","TCNESRO","TCNESRR","TCNESRS","TCNESSC",
            "TCNESSE","TCNESSP","TCNESTO"]:
        dados.rename(columns={'CHAVE': 'codigo', 'DS_REGRA': 'valor'}, inplace=True)
    elif nome_tabela_sem_extensao == 'CBO':
        dados.rename(columns={'CBO': 'codigo', 'DS_CBO': 'valor'}, inplace=True)
    elif nome_tabela_sem_extensao == "TCNESBR":
        dados.rename(columns={'CNES': 'codigo', 'NOMEFANT': 'valor'}, inplace=True)
    elif nome_tabela_sem_extensao == "cid10":
        dados.rename(columns={'CD_COD': 'codigo', 'CD_DESCR': 'valor'}, inplace=True)

    dados.to_csv(directoryJuncao + '/' + nome_tabela_sem_extensao + '.csv', index=True)
    return dados
