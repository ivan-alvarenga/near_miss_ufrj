import os
from os import listdir
from os.path import isfile, join

def converteTudoDBC2DBF(dirOrigem, dirDestino):

  if(dirOrigem=="" or dirOrigem==None):
    dirO='.'
  else: 
    dirO=dirOrigem

  if(dirDestino=="" or dirDestino==None):
    dirD='.'
  else: 
    dirD=dirDestino

  onlyfiles = [f for f in listdir(dirO) if isfile(join(dirO, f))]

  for file in onlyfiles:
    fileLen=len(file)
    if(fileLen>=5):
      last3=file[fileLen-3:fileLen]
    else:
      last3=None
    if(last3!=None and (last3=='dbc' or last3=='DBC')):
      print("./blast-dbf-master/blast-dbf "+dirO+'/'+file+' '+dirD+'/'+file[0:fileLen-3]+'dbf')
      os.system("./blast-dbf-master/blast-dbf "+dirO+'/'+file+' '+dirD+'/'+file[0:fileLen-3]+'dbf')
