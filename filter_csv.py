
import sys
import os
import codecs
from os import listdir
from os.path import isfile, join
'''
script para filtrar os registros de CSVs da base SP do AIH do DataSUS com as CIDs relativas a parto e puerpério
'''

CIDs = {'O829', 'O898', 'O746', 'P111', 'P132', 'O890', 'P113', 'P119', 'P131', 'P130', 'O840', 'O753', 'O995', 'O808', 'O745', 'O821', 'O102', 'P201', 'P102', 'O611', 'O893', 'O909', 'O653', 'P032', 'O69', 'O601', 'O892', 'O702', 'O904', 'O994', 'P108', 'O749', 'O710', 'P156', 'O47', 'O87', 'O669', 'O813', 'P133', 'O80', 'O709', 'O151', 'O70', 'O643', 'O992', 'O864', 'O81', 'P129', 'O663', 'O65', 'O632', 'O899', 'O721', 'P03', 'O67', 'O460', 'O649', 'O922', 'O757', 'O985', 'P017', 'O659', 'O694', 'O748', 'P038', 'O66', 'O75', 'P142', 'O861', 'O981', 'O91', 'O820', 'O833', 'O983', 'O471', 'O479', 'P152', 'P122', 'P035', 'O896', 'O670', 'O744', 'P153', 'O989', 'O822', 'P033', 'O895', 'O841', 'O815', 'O982', 'P134', 'O610', 'P13', 'P150', 'O758', 'P112', 'O90', 'O658', 'O99', 'O82', 'O722', 'P039', 'O152', 'O690', 'O723', 'O879', 'O640', 'O600', 'O700', 'O96', 'O986', 'O693', 'P159', 'O894', 'O602', 'P140', 'P155', 'O703', 'P148', 'O266', 'O828', 'O98', 'O831', 'O641', 'Z392', 'O650', 'P141', 'O103', 'O681', 'O267', 'F539', 'O751', 'O689', 'O752', 'O63', 'O664', 'P11', 'O809', 'O701', 'O648', 'O92', 'O645', 'O920', 'P10', 'O679', 'O642', 'O756', 'Z37', 'O421', 'P149', 'O420', 'P139', 'O912', 'O691', 'O984', 'O651', 'P034', 'P114', 'O862', 'P040', 'O903', 'O662', 'P120', 'P100', 'O990', 'O655', 'P12', 'P110', 'O863', 'O68', 'O683', 'O988', 'P103', 'O60', 'O94', 'O72', 'O878', 'O654', 'P031', 'O814', 'O109', 'P128', 'O74', 'O832', 'O668', 'O741', 'P109', 'O755', 'P143', 'P115', 'O921', 'P123', 'O10', 'O810', 'O618', 'O469', 'O998', 'F530', 'O980', 'F531', 'O993', 'P030', 'O678', 'O89', 'O695', 'O839', 'O891', 'O800', 'O468', 'O652', 'O848', 'P158', 'Z875', 'P104', 'O759', 'O997', 'O873', 'O661', 'P101', 'O811', 'P590', 'O104', 'O688', 'O910', 'O996', 'O422', 'O698', 'O849', 'Z390', 'P121', 'O100', 'O742', 'O84', 'O870', 'O470', 'O101', 'P151', 'F53', 'O747', 'O46', 'O712', 'P138', 'O692', 'O872', 'O660', 'O682', 'O61', 'O64', 'O619', 'O842', 'O639', 'F538', 'O680', 'O644', 'O871', 'O711', 'O905', 'O83', 'O623', 'O801', 'P15', 'P200', 'O991', 'O740', 'O838', 'O911', 'O699', 'O834', 'O743', 'O750', 'O908'}


if os.path.exists(sys.argv[1]) == False :
  print("usage: filter_csv caminho/diretorio/*.csv")
  exit()

qtd_cid_principal = 0
qtd_cid_secundario = 0
qtd_registros = 0

if(sys.argv[1]=="" or sys.argv[1]==None):
  dirOrigem='.'
else: 
  dirOrigem=sys.argv[1]

onlyfiles = [x for x in listdir(dirOrigem) if isfile(join(dirOrigem, x))]

for file in onlyfiles:
  fileLen=len(file)
  if(fileLen>=5):
    last3=file[fileLen-3:fileLen]
  else:
    last3=None
  if(last3!=None and (last3=='csv' or last3=='CSV')):
    with codecs.open(dirOrigem+'/'+file,'r',encoding='iso8859-1') as f:
    	for line in f:
    		qtd_registros += 1
    		values = line.split(',')
    		if len(values) != 36:
    			print("Erro: This .csv file is not on the pattern!")
    		if {values[32]}.issubset(CIDs):
    			qtd_cid_principal += 1
    		if {values[33]}.issubset(CIDs):
    			qtd_cid_secundario += 1
    print("*****************************************")
    print("\tArquivo "+file+" possui "+str(qtd_cid_principal)+" registros CID Principal associados a 'parto e puerpérias'")
    print("\tArquivo "+file+" possui "+str(qtd_cid_secundario)+" registros CID Secundário associados a 'parto e puerpérias'")
    print("\tA quantidade total de registros é igual a "+str(qtd_registros)+"\n")