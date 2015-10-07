import json
from pprint import pprint

class Environment:
	config = {}
	relations = {} 
	# {R:[
		#orderings:[{
			#"01":<disk path>,
			#"10":memory,
			#"attributes":[{same as what is loaded}]
		#]}
	#]}
	encodings = {}
	#{"node":[{"refcount":0,
		#"type":"long"
	#}]}

	def dump(self):
		pprint(self.config)

	def setup(self,config_in):
		self.config = config_in