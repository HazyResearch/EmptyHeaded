import json
import os
from pprint import pprint

class Environment:
  config = {}
  schemas = {} 
  relations = {}
  encodings = {}

  liverelations = {}

  def setSchemas(self,scheme):
    self.schemas = scheme

  def setRelations(self,relations):
    self.relations.update(relations)

  def setEncodings(self,encodings):
    self.encodings = encodings

  def toJSON(self,outF):
    data = self.config
    data["schemas"] = self.schemas
    data["relations"] = self.relations
    data["encodings"] = self.encodings
    f = open(outF,'w')
    f.write(json.dumps(data,sort_keys=True,indent=4,separators=(',', ': ')))
    f.close()

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
  	self.config["resultName"] = ""
  	self.config["resultOrdering"] = []
  	self.config["database"] = os.path.expandvars(self.config["database"])
  	if self.config["memory"] == "RAM":
  		self.config["memory"] = "ParMemoryBuffer"
  	else:
  		self.config["memory"] = "ParMMapBuffer"