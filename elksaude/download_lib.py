#!/usr/bin/python3
import sys
import os

from ftplib import FTP

todosEstados={'AC','AL','AM','AP','BA','CE','DF','ES','GO','MA','MG','MS','MT','PA','PB','PE','PI','PR','RJ','RN','RO','RR','RS','SC','SE','SP','TO'}
todosAnos = [2010,2011,2012,2013,2014,2015]

def downloadSINASC(anos_escolhidos=None,estados_escolhidos=None,directory=None):

    if estados_escolhidos is None or not estados_escolhidos:
        estados_escolhidos = todosEstados

    if anos_escolhidos is None or not anos_escolhidos:
        anos_escolhidos = todosAnos
    elif not type(anos_escolhidos) == list:
        anos_escolhidos = [anos_escolhidos]

    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()

    if anos_escolhidos[0] < 1994:
        raise ValueError("SINASC does not contain data before 1994")
    if anos_escolhidos[0] >= 1996:
        ftp.cwd('/dissemin/publicos/SINASC/NOV/DNRES/')
    else:
        ftp.cwd('/dissemin/publicos/SINASC/ANT/DNRES/')

    if(directory==None):
    	directory=os.path.expanduser('~/')+"sinascTmp/"
    if(directory[len(directory)-1]!='/'):
    	directory=directory+'/'
    if not os.path.exists(directory):
    	os.makedirs(directory)


    for estado in estados_escolhidos:
        for year in anos_escolhidos:
            fileName='DN'+estado+str(year)
            try:
                ftp.retrbinary('RETR '+fileName+'.dbc', open(directory+fileName+'.dbc', 'wb').write)
                print("Download realizado para SINASC " + estado + " " + str(year))
            except:
                ftp.retrbinary('RETR '+fileName+'.DBC', open(directory+fileName+'.DBC', 'wb').write)

    ftp.quit()

def downloadSINASCAuxiliares(year,directory=None):
    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()

    if(directory==None):
    	directory=os.path.expanduser('~/')+"sinascTmp/"
    if(directory[len(directory)-1]!='/'):
    	directory=directory+'/'
    if not os.path.exists(directory):
    	os.makedirs(directory)

    if year < 1994:
        raise ValueError("SINASC does not contain data before 1994")
    if year >= 1996:
        ftp.cwd('/dissemin/publicos/SINASC/NOV/TAB/')
        ftp.retrbinary('RETR NASC_NOV_TAB.zip', open(directory+'NASC_NOV_TAB.zip', 'wb').write)
        print("Download realizado para tabela auxiliar SINASC " + str(year))
    else:
        ftp.cwd('/dissemin/publicos/SINASC/ANT/TAB/')
        ftp.retrbinary('RETR NASC_ANT_TAB.zip', open(directory+'NASC_ANT_TAB.zip', 'wb').write)
    
    ftp.quit()

def downloadSIM(anos_escolhidos,estados_escolhidos=None,directory=None):

    if estados_escolhidos is None or not estados_escolhidos:
        estados_escolhidos = todosEstados

    if anos_escolhidos is None or not anos_escolhidos:
        anos_escolhidos = todosAnos
    elif not type(anos_escolhidos) == list:
        anos_escolhidos = [anos_escolhidos]

    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()

    if anos_escolhidos[0] < 1996:
        raise ValueError("SIM before 1996 use CID9")
    else:
        ftp.cwd('/dissemin/publicos/SIM/CID10/DORES/')

    if(directory==None):
    	directory=os.path.expanduser('~/')+"simTmp/"
    if(directory[len(directory)-1]!='/'):
    	directory=directory+'/'
    if not os.path.exists(directory):
    	os.makedirs(directory)


    for estado in estados_escolhidos:
        for year in anos_escolhidos:
            fileName='DO'+estado+str(year)
            try:
                ftp.retrbinary('RETR '+fileName+'.dbc', open(directory+fileName+'.dbc', 'wb').write)
            except:
                ftp.retrbinary('RETR '+fileName+'.DBC', open(directory+fileName+'.DBC', 'wb').write)
    
    ftp.quit()

def downloadSIMAuxiliares(year,directory=None):
    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()

    if(directory==None):
    	directory=os.path.expanduser('~/')+"simTmp/"
    if(directory[len(directory)-1]!='/'):
    	directory=directory+'/'
    if not os.path.exists(directory):
    	os.makedirs(directory)

    if year < 1996:
        raise ValueError("SIM before 1996 use CID9")
    else:
        ftp.cwd('/dissemin/publicos/SIM/CID10/TAB/')
        ftp.retrbinary('RETR OBITOS_CID10_TAB.ZIP', open(directory+'OBITOS_CID10_TAB.ZIP', 'wb').write)
    
    ftp.quit()

def downloadSIH(anos_escolhidos,estados_escolhidos=None,directory=None):

    if estados_escolhidos is None or not estados_escolhidos:
        estados_escolhidos = todosEstados

    if anos_escolhidos is None or not anos_escolhidos:
        anos_escolhidos = todosAnos
    elif not type(anos_escolhidos) == list:
        anos_escolhidos = [anos_escolhidos]

    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()

    if anos_escolhidos[0] < 1992:
        raise ValueError("SIH does not contain data before 1992")
    elif(anos_escolhidos[0]>=1992 and anos_escolhidos[0]<2008): # todo trocar para ano
        ftp.cwd('/dissemin/publicos/SIHSUS/199201_200712/Dados/')
    else:
        ftp.cwd('/dissemin/publicos/SIHSUS/200801_/dados/')

    if(directory==None):
    	directory=os.path.expanduser('~/')+"sihTmp/"
    if(directory[len(directory)-1]!='/'):
    	directory=directory+'/'
    if not os.path.exists(directory):
    	os.makedirs(directory)


    for estado in estados_escolhidos:
        for year in anos_escolhidos:
            for mes in range(1,13):
                yearStr=str(year)
                yearStr=yearStr[len(yearStr)-2:len(yearStr)]
                if(mes<10):
                    mesStr='0'+str(mes)
                else:
                    mesStr=str(mes)
                fileName='RD'+estado+yearStr+mesStr
                try:
                    ftp.retrbinary('RETR '+fileName+'.dbc', open(directory+fileName+'.dbc', 'wb').write)
                except:
                    ftp.retrbinary('RETR '+fileName+'.DBC', open(directory+fileName+'.DBC', 'wb').write)
    
    ftp.quit()

def downloadSIHAuxiliares(year,directory=None):
    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()

    if(directory==None):
    	directory=os.path.expanduser('~/')+"sihTmp/"
    if(directory[len(directory)-1]!='/'):
    	directory=directory+'/'
    if not os.path.exists(directory):
    	os.makedirs(directory)

    if year < 1992:
        raise ValueError("SIH does not contain data before 1992")
    else:
        ftp.cwd('/dissemin/publicos/SIHSUS/200801_/Auxiliar/')
        ftp.retrbinary('RETR TAB_SIH.zip', open(directory+'TAB_SIH.zip', 'wb').write)
    
    ftp.quit()
