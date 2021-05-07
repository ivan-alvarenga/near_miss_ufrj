import os
import zipfile
from os import listdir
from os.path import isfile, join

def descompactaTudoZIP(diretorio):

	if(diretorio=="" or diretorio==None):dir='./'
	else: dir=diretorio

	onlyfiles = [f for f in listdir(dir) if isfile(join(dir, f))]
	for file in onlyfiles:
		fileLen=len(file)
		if(fileLen>=5):
			last3=file[fileLen-3:fileLen]
		else:
			last3=None
		if(last3!=None and (last3=='zip' or last3=='ZIP')):
			zip_ref = zipfile.ZipFile(dir+file, 'r')
			zip_ref.extractall(dir)
			zip_ref.close()

def deletaTudoComExtensao(diretorio,extensao):

	if(diretorio=="" or diretorio==None):dir='./'
	else: dir=diretorio

	if(extensao=="" or extensao==None or len(extensao)!=3):
		raise ValueError("deletaTudoComExtensao: extensao obrigatória com três caracteres")

	onlyfiles = [f for f in listdir(dir) if isfile(join(dir, f))]
	for file in onlyfiles:
		fileLen=len(file)
		if(fileLen>=5):
			last3=file[fileLen-3:fileLen]
		else:
			last3=None
		if(last3!=None and (last3==extensao.upper() or last3==extensao.lower())):
			os.remove(diretorio+"/"+file)

def deletaTudo(diretorio):

	if(diretorio=="" or diretorio==None):dir='./'
	else: dir=diretorio

	onlyfiles = [f for f in listdir(dir) if isfile(join(dir, f))]
	for file in onlyfiles:
		os.remove(diretorio+"/"+file)