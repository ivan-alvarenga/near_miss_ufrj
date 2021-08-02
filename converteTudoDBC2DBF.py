import os
from os import listdir
from os.path import isfile, join

def converteTudoDBC2DBF(diretorio):

	if(diretorio=="" or diretorio==None):dir='.'
	else: dir=diretorio

	onlyfiles = [f for f in listdir(dir) if isfile(join(dir, f))]

	for file in onlyfiles:
		fileLen=len(file)
		if(fileLen>=5):
			last3=file[fileLen-3:fileLen]
		else:
			last3=None
		if(last3!=None and (last3=='dbc' or last3=='DBC')):
			print("./blast-dbf-master/blast-dbf "+dir+'/'+file+' '+dir+'/'+file[0:fileLen-3]+'dbf')
			os.system("./blast-dbf-master/blast-dbf "+dir+'/'+file+' '+dir+'/'+file[0:fileLen-3]+'dbf')

if __name__ == '__main__':

	converteTudoDBC2DBF('.')
