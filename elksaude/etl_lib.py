from pymongo import MongoClient

class EtlPipeline:

	def __init__(self,database,collection,collectionOut):
		self.pipeline=[]
		self.database=MongoClient()[database]
		self.collection=self.database[collection]
		self.camposUsados={}
		self.terminouCamposUsados=False
		self.collectionOut=collectionOut

	def addCampoUsado(self,campo):
		if(self.terminouCamposUsados==False):
			self.camposUsados[campo]=1
		else:
			raise ValueError("Campos Usados precisam ser incluídos antes de qualquer junção")

	def addJuncao(self,campo,auxiliar,codigoAuxiliar):
		if(self.terminouCamposUsados==False):
			project={ "$project" : self.camposUsados}
			self.pipeline.append(project)
			self.terminouCamposUsados=True
			self.collection.create_index(campo)
		self.database[auxiliar].create_index(codigoAuxiliar)
		lookup={"$lookup": { "from": auxiliar, "localField": campo, "foreignField": codigoAuxiliar, "as": campo}}
		unwind={ "$addFields": {campo: { "$arrayElemAt": [ "$"+campo, 0 ] }}}
		projectCampo={ "$project" : {campo+"._id" : 0}}
		self.pipeline.append(lookup)
		self.pipeline.append(unwind)
		self.pipeline.append(projectCampo)

	def executaEtl(self):
		out={ "$out" : self.collectionOut }
		self.pipeline.append(out)
		self.collection.aggregate(self.pipeline)