import os
from os import listdir
from os.path import isfile, join
from CNV2CSV import CNV2CSV

def converteTudoCNV2CSV(diretorio):

	if(diretorio=="" or diretorio==None):dir='.'
	else: dir=diretorio

	onlyfiles = [f for f in listdir(dir) if isfile(join(dir, f))]

	for file in onlyfiles:
		fileLen=len(file)
		if(fileLen>=5):
			last3=file[fileLen-3:fileLen]
		else:
			last3=None
		if(last3!=None and (last3=='cnv' or last3=='CNV')):
			CNV2CSV(dir+'/'+file)

# if __name__ == '__main__':

	# converteTudoCNV2CSV('/Users/rafaelstomaz/cnv_teste')