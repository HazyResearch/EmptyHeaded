## Stores the schemas for each relation.
## Enables users to define a schema. 
## Note: defining a schema does not add a 
## relation to the database. A user must define
## all schemas they wish to add to a database
## before creating the database. Once the database
## is created they can execute queries over the respective
## schemas.

## The order the types are specified in the schema must be
## the same as the order in the CSV or TSV file. The annotations
## (if specified) must come last.

import numpy as np
from sets import Set

#map from dataframe type to EH accepted types
#check in the QC occurs in Schema.scala
#we also check in DFMap when we pass the array
#difference is here we figure out the annotations
map_types = {
  "int32":np.int32,
  "int64":np.int64,
  "uint32":np.uint32,
  "uint64":np.uint64,
  "float32":np.float32,
  "float64":np.float64
}

def strip_unicode(values):
  return [str(x.name) for x in values]

def wrap_strings(values):
  return [np.dtype(map_types[str(x)]) for x in values]

def wrap_types(values):
  return [np.dtype(x) for x in values]

class Schema:
  def python2java(self,duncecap):
    return duncecap.QueryCompiler.buildSchema(
      strip_unicode(self.attributes),
      strip_unicode(self.annotations),
      self.attribute_names,
      self.annotation_names)

  @staticmethod
  def java2python(jobject):
    attribute_types = wrap_types(jobject.getAttributeTypes())
    annotation_types = wrap_types(jobject.getAnnotationTypes())
    return Schema(attribute_types,annotation_types)

  def __init__(self,attributes,annotations=[],attribute_names=[],annotation_names=[]):
    self.attributes = wrap_types(attributes)
    self.annotations = wrap_types(annotations)
    self.attribute_names = attribute_names
    self.annotation_names = annotation_names

  @staticmethod
  def fromDF(df, attribute_names=[], annotation_names=[]):
    #dataframe names for annnotations must start with "a_"
    pattern = r'^a_.*'
    typesSeries = df.dtypes
    
    found = typesSeries.index.str.contains(pattern)
    notfound = map(lambda x: not x, found)
    
    attribute_types = []
    for a in typesSeries[notfound]:
      if str(a.name) not in map_types:
        raise Exception("DataFrame attribute type error. Type " + str(a.name) + " not accepted.")
      attribute_types.append(a)

    annotation_types = []
    for a in typesSeries[found]:
      if str(a.name) not in map_types:
        raise Exception("DataFrame annotation type error. Type " + str(a.name) + " not accepted.")
      annotation_types.append(a)
    
    return Schema(attribute_types,annotation_types,attribute_names,annotation_names)

  #for printing purposes
  def __repr__(self):
    return """(%s,%s)""" % (self.attributes,self.annotations)
