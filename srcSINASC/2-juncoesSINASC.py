from pymongo import MongoClient

#pega a database 
projeto=MongoClient().projeto

#indexa o campo que sofrera a primeira juncao, que deve ser a juncao mais cara computacionalmente
projeto.sih.create_index("CODMUNNASC") 

#array com as juncoes a serem feitas
pipeline=[]

#projecao que seleciona campos usados
camposUsados={'APGAR1':1,'APGAR5':1,'CODANOMAL':1,'CODBAINASC ':1,'CODBAIRES':1,'CODESTAB':1,'CODMUNNASC':1,'CODMUNNATU':1,'CODMUNRES':1,'CODPAISRES':1,
				'CODUFNATU':1,'CONSULTAS':1,'DTNASC':1,'DTNASCMAE':1,'DTULTMENST':1,'ESCMAE':1,'ESCMAE2010':1,'ESCMAEAGR1':1,'ESTCIVMAE':1,'GESTACAO':1,
				'GRAVIDEZ':1,'HORANASC':1,'IDADEMAE':1,'IDADEPAI':1,'IDANOMAL':1,'LOCNASC':1,'MESPRENAT':1,'NATURALMAE':1,'NUMERODN':1,'PARTO':1,'PESO':1,
				'QTDFILMORT':1,'QTDFILVIVO':1,'QTDGESTANT':1,'QTDPARTCES':1,'QTDPARTNOR ':1,'RACACOR ':1,'RACACORMAE ':1,'SEXO':1,'SEMAGESTAC':1,
				'SERIECMAE':1,'STCESPARTO':1,'STDNEPIDEM':1,'STDNNOVA':1,'STTRABPART':1,'TPAPRESENT':1,'TPMETESTIM':1,'TPNASCCASSI':1}

project={ "$project" : camposUsados}
pipeline.append(project)

'''
	As tabelas foram organizadas com base no tamanho do csv
	rodas "ls -lS" para obter os arquivos ordenados por size
'''

#juncao de MUNICBR- codmunnasc
projeto["municbr_cnv"].create_index("codigo")
lookup={"$lookup": { "from": "municbr_cnv", "localField": "CODMUNNASC", "foreignField": "codigo", "as": "CODMUNNASC"}}
unwind1={ "$addFields": {"CODMUNNASC": { "$arrayElemAt": [ "$CODMUNNASC", 0 ] }}}
pipeline.append(lookup)
project1={ "$project" : {"CODMUNNASC._id" : 0}} #tira id, projeta codmunnasc ocm zero

#juncao de CODESTAB - campo de juncao codigo
projeto['estab06_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'estab06_cnv', "localField": "CODESTAB", "foreignField": "codigo", "as": "CODESTAB"}}
unwind2={ "$addFields": {"CODESTAB": { "$arrayElemAt": [ "$CODESTAB", 0 ] }}}
pipeline.append(lookup)
project2={ "$project" : {"CODESTAB._id" : 0}}

#juncao de PESO - campo de juncao codigo
projeto['peso_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'peso_cnv', "localField": "PESO", "foreignField": "codigo", "as": "PESO"}}
unwind3={ "$addFields": {"PESO": { "$arrayElemAt": [ "$PESO", 0 ] }}}
pipeline.append(lookup)
project3={ "$project" : {"PESO._id" : 0}}

# juncao de CODANOMAL
projeto['cid1017_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'cid1017_cnv', "localField": "CODANOMAL", "foreignField": "codigo", "as": "CODANOMAL"}}
unwind4={ "$addFields": {"CODANOMAL": { "$arrayElemAt": [ "$CODANOMAL", 0 ] }}}
pipeline.append(lookup)
project4={ "$project" : {"CODANOMAL._id" : 0}}

# juncao de IDADEMAE
projeto['idademae_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'idademae_cnv', "localField": "IDADEMAE", "foreignField": "codigo", "as": "IDADEMAE"}}
unwind5={ "$addFields": {"IDADEMAE": { "$arrayElemAt": [ "$IDADEMAE", 0 ] }}}
pipeline.append(lookup)
project5={ "$project" : {"IDADEMAE._id" : 0}}

# juncao de QTDFILMORT
projeto['filho_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'filho_cnv', "localField": "QTDFILMORT", "foreignField": "codigo", "as": "QTDFILMORT"}}
unwind6={ "$addFields": {"QTDFILMORT": { "$arrayElemAt": [ "$QTDFILMORT", 0 ] }}}
pipeline.append(lookup)
project6={ "$project" : {"QTDFILMORT._id" : 0}}

# juncao de QTDFILVIVO
projeto['filho_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'filho_cnv', "localField": "QTDFILVIVO", "foreignField": "codigo", "as": "QTDFILVIVO"}}
unwind7={ "$addFields": {"QTDFILVIVO": { "$arrayElemAt": [ "$QTDFILVIVO", 0 ] }}}
pipeline.append(lookup)
project7={ "$project" : {"QTDFILVIVO._id" : 0}}

# juncao de HORANASC
projeto['hora_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'hora_cnv', "localField": "HORANASC", "foreignField": "codigo", "as": "HORANASC"}}
unwind8={ "$addFields": {"HORANASC": { "$arrayElemAt": [ "$HORANASC", 0 ] }}}
pipeline.append(lookup)
project8={ "$project" : {"HORANASC._id" : 0}}

# juncao de SEXO
projeto['sexo_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'sexo_cnv', "localField": "SEXO", "foreignField": "codigo", "as": "SEXO"}}
unwind9={ "$addFields": {"SEXO": { "$arrayElemAt": [ "$SEXO", 0 ] }}}
pipeline.append(lookup)
project9={ "$project" : {"SEXO._id" : 0}}

# juncao de  LOCNASC
projeto['lococor_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'lococor_cnv', "localField": "LOCNASC", "foreignField": "codigo", "as": "LOCNASC"}}
unwind10={ "$addFields": {"LOCNASC": { "$arrayElemAt": [ "$LOCNASC", 0 ] }}}
pipeline.append(lookup)
project10={ "$project" : {"LOCNASC._id" : 0}}

# juncao de GRAVIDEZ
projeto['gravidez_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'gravidez_cnv', "localField": "GRAVIDEZ", "foreignField": "codigo", "as": "GRAVIDEZ"}}
unwind11={ "$addFields": {"GRAVIDEZ": { "$arrayElemAt": [ "$GRAVIDEZ", 0 ] }}}
pipeline.append(lookup)
project11={ "$project" : {"GRAVIDEZ._id" : 0}}

# juncao de PARTO
projeto['parto_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'parto_cnv', "localField": "PARTO", "foreignField": "codigo", "as": "PARTO"}}
unwind12={ "$addFields": {"PARTO": { "$arrayElemAt": [ "$PARTO", 0 ] }}}
pipeline.append(lookup)
project12={ "$project" : {"PARTO._id" : 0}}

# juncao de CONSULTAS
projeto['consult_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'consult_cnv', "localField": "CONSULTAS", "foreignField": "codigo", "as": "CONSULTAS"}}
unwind13={ "$addFields": {"CONSULTAS": { "$arrayElemAt": [ "$CONSULTAS", 0 ] }}}
pipeline.append(lookup)
project13={ "$project" : {"CONSULTAS._id" : 0}}

# juncao de ESTCIVMAE
projeto['estciv_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'estciv_cnv', "localField": "ESTCIVMAE", "foreignField": "codigo", "as": "ESTCIVMAE"}}
unwind14={ "$addFields": {"ESTCIVMAE": { "$arrayElemAt": [ "$ESTCIVMAE", 0 ] }}}
pipeline.append(lookup)
project14={ "$project" : {"ESTCIVMAE._id" : 0}}

# juncao de GESTACAO
projeto['semanas_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'semanas_cnv', "localField": "GESTACAO", "foreignField": "codigo", "as": "GESTACAO"}}
unwind15={ "$addFields": {"GESTACAO": { "$arrayElemAt": [ "$GESTACAO", 0 ] }}}
pipeline.append(lookup)
project15={ "$project" : {"GESTACAO._id" : 0}}

# juncao de APGAR1
projeto['apgar_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'apgar_cnv', "localField": "APGAR1", "foreignField": "codigo", "as": "APGAR1"}}
unwind16={ "$addFields": {"APGAR1": { "$arrayElemAt": [ "$APGAR1", 0 ] }}}
pipeline.append(lookup)
project16={ "$project" : {"APGAR1._id" : 0}}

# juncao de APGAR5
projeto['apgar_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'apgar_cnv', "localField": "APGAR5", "foreignField": "codigo", "as": "APGAR5"}}
unwind17={ "$addFields": {"APGAR5": { "$arrayElemAt": [ "$APGAR5", 0 ] }}}
pipeline.append(lookup)
project17={ "$project" : {"APGAR5._id" : 0}}

# juncao de ESCMAE
projeto['instruc_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'instruc_cnv', "localField": "ESCMAE", "foreignField": "codigo", "as": "ESCMAE"}}
unwind18={ "$addFields": {"ESCMAE": { "$arrayElemAt": [ "$ESCMAE", 0 ] }}}
pipeline.append(lookup)
project18={ "$project" : {"ESCMAE._id" : 0}}

# juncao de IDANOMAL
projeto['idanomal_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'idanomal_cnv', "localField": "IDANOMAL", "foreignField": "codigo", "as": "IDANOMAL"}}
unwind19={ "$addFields": {"IDANOMAL": { "$arrayElemAt": [ "$IDANOMAL", 0 ] }}}
pipeline.append(lookup)
project19={ "$project" : {"IDANOMAL._id" : 0}}


# juncao de RACACOR
projeto['raca_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'raca_cnv', "localField": "RACACOR", "foreignField": "codigo", "as": "RACACOR"}}
unwind20={ "$addFields": {"RACACOR": { "$arrayElemAt": [ "$RACACOR", 0 ] }}}
pipeline.append(lookup)
project20={ "$project" : {"RACACOR._id" : 0}}


#executa os unwinds para retirar dos arrays os dados
pipeline.append(unwind1)
pipeline.append(unwind2)
pipeline.append(unwind3)
pipeline.append(unwind4)
pipeline.append(unwind5)
pipeline.append(unwind6)
pipeline.append(unwind7)
pipeline.append(unwind8)
pipeline.append(unwind9)
pipeline.append(unwind10)
pipeline.append(unwind11)
pipeline.append(unwind12)
pipeline.append(unwind13)
pipeline.append(unwind14)
pipeline.append(unwind15)
pipeline.append(unwind16)
pipeline.append(unwind17)
pipeline.append(unwind18)
pipeline.append(unwind19)
pipeline.append(unwind20)


pipeline.append(project1)
pipeline.append(project2)
pipeline.append(project3)
pipeline.append(project4)
pipeline.append(project5)
pipeline.append(project6)
pipeline.append(project7)
pipeline.append(project8)
pipeline.append(project9)
pipeline.append(project10)
pipeline.append(project11)
pipeline.append(project12)
pipeline.append(project13)
pipeline.append(project14)
pipeline.append(project15)
pipeline.append(project16)
pipeline.append(project17)
pipeline.append(project18)
pipeline.append(project19)
pipeline.append(project20)
out={ "$out" : "sinasc_completa" }
pipeline.append(out)

#agregacao onde ocorrem as juncoes
projeto.sinasc15_inicial.aggregate(pipeline)

cursor=projeto.sinasc_completa.find()
if __name__ == '__main__':
	i=0
	saida=open("juncoes.out","w")
	for ele in cursor:
		#print(ele)
		if(i==100):break
		saida.write(str(ele)+"\n")
		i=i+1
	saida.close()

