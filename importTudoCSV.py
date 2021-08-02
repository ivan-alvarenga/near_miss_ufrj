import os
from os import listdir
from os.path import isfile, join

def importTudoCSV(database,diretorio,nome=None,drop=True):

	if(diretorio=="" or diretorio==None):dir='.'
	else: dir=diretorio

	if(drop==False and nome==None):
		print("erro:obrigatorio nome da colecao")
		exit();

	onlyfiles = [f for f in listdir(dir) if isfile(join(dir, f))]

	contador=0
	for file in onlyfiles:
		fileLen=len(file)
		if(fileLen>=5):
			last3=file[fileLen-3:fileLen]
		else:
			last3=None
		if(last3!=None and (last3=='csv' or last3=='CSV')):
			print('##################################################')
			print('database >> ' + database)
			print('dir_file >> ' + dir+'/'+file)
			print('##################################################')

# 			if(drop):
# 				os.system('mongoimport -d '+database+' --drop --headerline --type=csv --file='+dir+'/'+file+' -c '+file[0:fileLen-4])
# 			else:
# 				if(contador==0):
# 					os.system('mongoimport -d '+database+' --drop --headerline --type=csv --file='+dir+'/'+file+' -c '+nome)
# 				else:
# 					os.system('mongoimport -d '+database+' --headerline --type=csv --file='+dir+'/'+file+' -c '+nome)
		contador=contador+1
