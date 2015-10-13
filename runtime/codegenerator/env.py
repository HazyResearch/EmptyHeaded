import json
from pprint import pprint

class Environment:
	config = {}
	schemas = {} 
	relations = {}
	encodings = {}

	def setSchemas(self,scheme):
		self.schemas = scheme

	def setRelations(self,relations):
		self.relations = relations

	def setEncodings(self,encodings):
		self.encodings = encodings

	def toJSON(self,outF):
		data = self.config
		data["schemas"] = self.schemas
		data["relations"] = self.relations
		data["encodings"] = self.encodings
		json.dump(data,open(outF,'w'))

	def fromJSON(self,inF):
		data = json.load(open(inF))
		self.schemas = data.pop("schemas",0)
		self.relations = data.pop("relations",0)
		self.encodings = data.pop("encodings",0)
		self.config = data

	def dump(self):
		pprint(self.config)
		pprint(self.schemas)
		pprint(self.relations)
		pprint(self.encodings)

	def setup(self,config_in):
		self.config = config_in
		if self.config["memory"] == "RAM":
			self.config["memory"] = "ParMemoryBuffer"
		else:
			self.config["memory"] = "ParMMapBuffer"