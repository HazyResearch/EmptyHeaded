## Stores the relations. Each relation has:
## 1) A file (csv or tsv) the data comes from
## 2) A schema

## IMPORTANT
## The order the types are specified in the schema must be
## the same as the order in the CSV or TSV file. The annotations
## (if specified) must come last.

from schema import Schema
import pandas as pd

class Relation:
  def __init__(self,name,schema=Schema([]),filename="",dataframe=pd.DataFrame(),check=True,attribute_names=[],annotation_names=[]):

    # Replace int64 with int32. TODO: Make int64 work?
    int64_cols = [
      col for col, dtype in dataframe.dtypes.to_dict().items()
      if dtype == "int64"
      ]
    dataframe[int64_cols] = dataframe[int64_cols].astype(pd.np.int32)

    self.name = name #name of the relation
    self.schema = schema #schema of relation
    self.filename = filename #file relation comes from
    self.df = not dataframe.empty
    self.dataframe = dataframe
    if not dataframe.empty:
      self.schema = Schema.fromDF(dataframe,attribute_names,annotation_names)
    if check:
      if dataframe.empty and filename == "":
        raise Exception("Relation "+self.name+" needs a filename (either a file or dataframe).")

  def python2java(self,duncecap):
    self.javaschema = self.schema.python2java(duncecap)
    return duncecap.Relation(self.name,self.javaschema,str(self.filename),self.df)

  @staticmethod
  def java2python(jobject):
    name = jobject.getName()
    filename = jobject.getFilename()
    schema = Schema.java2python(jobject.getSchema())
    df = jobject.getDF()
    return Relation(name=name,schema=schema,filename=filename,check=False)

  #for printing purposess
  def __repr__(self):
    return """(%s,%s,%s)""" % (self.name,self.schema,self.filename)
