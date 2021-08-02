from pymongo import MongoClient

class MongoMisc:

	def __init__(self,database):
		self.database=None#MongoClient()[database]

	def deletaCollection(self,collection):
		print('deletaCollection >> ' + collection)
		#self.database.drop_collection(collection)
