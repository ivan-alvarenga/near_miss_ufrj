#!/usr/bin/python3
import sys
import os

from ftplib import FTP
import dbf2csv
'''
@TODO: baixar outras tabelas e suas auxiliares
@TODO: chamar o script de juncao
@TODO: utilizar o script que importa tudo pro mongo
SIM, SINASC, E SIH(essa eh com o rafael)

etl.py :  escrever descricao do script abaixo
.
.
.
'''

def download(state, year):
    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    if year < 1994:
        raise ValueError("SINASC does not contain data before 1994")
    if year >= 1996:
        ftp.cwd('/dissemin/publicos/SINASC/NOV/DNRES/')
        fname = ('DN{}{}.dbc'.format(state, year))
    else:
        ftp.cwd('/dissemin/publicos/SINASC/ANT/DNRES/')
        fname = ('DNR{}{}.dbc'.format(state, str(year)[-2:]))

    ftp.retrbinary('RETR {}'.format(fname), open(fname, 'wb').write)

    filename = format(fname)
    return filename

def dbc2csv(filename):
    #DBC -> DBF -> CSV
    shortname = format(filename[:-4])
    dbc2dbf('./'+filename,'./'+ shortname +'.dbf')
    os.system("dbf2csv "+ shortname+".dbf > "+ shortname+".csv")

#https://github.com/akadan47/dbf2csv
#usar o main que esta no github  acima e abaixo
#https://github.com/akadan47/dbf2csv
def etl(anos, tabela_escolhida):
    print("estou aqui \n")
    return


def main():
        tabelas_aceitas = {'SIH', 'SIM', 'SINASC'}
        estado = str(sys.argv[1])
        ano    = int(sys.argv[2])
        tabela = str(sys.argv[3])
        #@TODO : Colocar cores
        print("Fazendo Download da Tabela " + tabela)
        print(str(sys.argv[1]),str(sys.argv[2]))
        filename = download(estado,ano)
        if tabela not in tabelas_aceitas:
            print("Por favor, digite a tabela corretamente.\n  {SIM,SINASC,SIH}\n")
            return 
        else:
            etl(ano, tabela)

	#@TODO : Por clausula de erro no dbc2csv
        dbc2csv(filename)
        return 0

if __name__ == '__main__' :
	main()


#DOWNLOAD SO FAZ DOWNLOAD
#CONVERTER PRA DBF
#CONVERTER PRA CSV

# #PROBLEMA# LOCAL PRA ONDE ELE BAIXA SOBRESCREVER
#@TODO : CRIAR PASTAS
#run : python3.6 nomeDoArquivo.py SP 2016 SINASC

#@TODO : CHECAR POR ERROS, ISSO EH IMPORTANTE 

