import os
from os import listdir
from os.path import isfile, join

def converteTudoDBF2CSV(diretorio):

	if(diretorio=="" or diretorio==None):dir='.'
	else: dir=diretorio

	onlyfiles = [f for f in listdir(dir) if isfile(join(dir, f))]

	for file in onlyfiles:
		fileLen=len(file)
		if(fileLen>=5):
			last3=file[fileLen-3:fileLen]
		else:
			last3=None
		if(last3!=None and (last3=='dbf' or last3=='DBF')):
			print("dbf2csv "+dir+'/'+file+">"+dir+'/'+file[0:fileLen-3]+'csv')
			os.system("dbf2csv "+dir+'/'+file+">"+dir+'/'+file[0:fileLen-3]+'csv')

# if __name__ == '__main__':
#
# 	converteTudoDBF2CSV('/Users/rafaelstomaz/.tmpEtlDATASUS/')
