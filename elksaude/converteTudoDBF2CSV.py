import os
from os import listdir
from os.path import isfile, join

def converteTudoDBF2CSV(dirOrigem, dirDestino):

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
    if(last3!=None and (last3=='dbf' or last3=='DBF')):
      print("./dbf2csv-master/dbf2csv/main.py "+dirO+'/'+file+">"+dirD+'/'+file[0:fileLen-3]+'csv')
      os.system("./dbf2csv-master/dbf2csv/main.py "+dirO+'/'+file+">"+dirD+'/'+file[0:fileLen-3]+'csv')

# if __name__ == '__main__':
#
#   converteTudoDBF2CSV('/Users/rafaelstomaz/.tmpEtlDATASUS/')
