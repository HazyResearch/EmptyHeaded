## Maintains the interface for the intermediate representation
## This can be sent in and out of both code generators and
## the GHD optimizer.

import jpype

def strip_unicode(values):
  return [str(x) for x in values]

#Convinance class for expressing relations
class RELATION:
  def __init__(self,name,attributes,annotations=[],scaling=1):
    self.name = name
    self.attributes = attributes
    self.annotations = annotations
  
  def __repr__(self):
    return """[%s,%s,%s]""" % (self.name,\
      self.attributes,\
      self.annotations)

class IR:
  def __init__(self,rules):
    if not isinstance(rules[0],RULE):
      raise Exception("IR type incorrect (list of RULES).")
    self.rules = rules
    self.duncecap = jpype.JPackage('duncecap')

  def python2java(self):
    irbuilder = self.duncecap.IRBuilder()
    for r in self.rules:
      irbuilder.addRule(r.python2java())
    return irbuilder.build()

  @staticmethod
  def java2python(jobject):
    rules = []
    nRules = jobject.getNumRules()
    for i in range(0,nRules):
      rules.append(RULE.java2python(jobject.getRule(i)))
    return IR(rules)

#The top level class
class RULE:
  def __init__(self,
    result,
    order,
    project,
    operation,
    join,
    aggregates,
    filters,
    ):
    
    if not isinstance(result,RESULT) or \
      not isinstance(order,ORDER) or \
      not isinstance(project,PROJECT) or \
      not isinstance(operation,OPERATION) or \
      not isinstance(join,JOIN) or \
      not isinstance(aggregates,AGGREGATES) or \
      not isinstance(filters,FILTERS):
      raise Exception("Types not correct for IR.")

    self.duncecap = jpype.JPackage('duncecap')
    self.result = result
    self.order = order
    self.project = project
    self.operation = operation
    self.join = join
    self.aggregates = aggregates
    self.filters = filters

  def python2java(self):
    javaResult = self.result.python2java(self.duncecap)
    javaOrder = self.order.python2java(self.duncecap)
    javaProject = self.project.python2java(self.duncecap)
    javaOperation = self.operation.python2java(self.duncecap)
    javaJoin = self.join.python2java(self.duncecap)
    javaAgg = self.aggregates.python2java(self.duncecap)
    javaFilter = self.filters.python2java(self.duncecap)
    return self.duncecap.Rule(
      javaResult,
      javaOrder,
      javaProject,
      javaOperation,
      javaJoin,
      javaAgg,
      javaFilter)

  @staticmethod
  def java2python(jobject):
    result = RESULT.java2python(jobject.getResult())
    order = ORDER.java2python(jobject.getOrder())
    project = PROJECT.java2python(jobject.getProject())
    operation = OPERATION.java2python(jobject.getOperation())
    join = JOIN.java2python(jobject.getJoin())
    filters = FILTERS.java2python(jobject.getFilters())
    aggregates = AGGREGATES.java2python(jobject.getAggregations())
    return RULE(result,order,project,operation,join,aggregates,filters)
  
  def __repr__(self):
    return """RULE :-\t %s \n\t %s \n\t %s \n\t %s \n\t %s \n\t %s \n\t %s>""" \
      % (self.result,\
      self.order,\
      self.project,\
      self.operation,\
      self.join,\
      self.aggregates,\
      self.filters)

#Relation for the result
class RESULT:
  def __init__(self,rel):
    self.rel = rel

  def python2java(self,duncecap):
    return duncecap.Result(duncecap.IR.buildRel(self.rel.name,
      strip_unicode(self.rel.attributes),
      strip_unicode(self.rel.annotations)))

  @staticmethod
  def java2python(jobject):
    return RESULT(RELATION(
      jobject.getRel().getName(),
      strip_unicode(jobject.getRel().getAttributes()),
      strip_unicode(jobject.getRel().getAnnotations())))

  def __repr__(self):
    return """RESULT: %s """ % (self.rel)

#Order attributes are processed in NPRR
class ORDER:
  def __init__(self,attributes):
    self.attributes = attributes

  def python2java(self,duncecap):
    return duncecap.Order(
      duncecap.IR.buildAttributes(self.attributes))

  @staticmethod
  def java2python(jobject):
    return ORDER(strip_unicode(jobject.getAttributes()))

  def __repr__(self):
    return """ORDER: %s""" % (self.attributes)

#Attributes that are projected away.
class PROJECT:
  def __init__(self,attributes=[]):
    self.attributes = attributes

  def python2java(self,duncecap):
    return duncecap.Project(
      duncecap.IR.buildAttributes(self.attributes))

  @staticmethod
  def java2python(jobject):
    return PROJECT(strip_unicode(jobject.getAttributes()))

  def __repr__(self):
    return """PROJECT: %s""" % (self.attributes)

#Join operation for aggregations
class OPERATION:
  def __init__(self,operation):
    self.operation = operation

  def python2java(self,duncecap):
    return duncecap.Operation(self.operation)

  @staticmethod
  def java2python(jobject):
    return OPERATION(str(jobject.getOperation()))

  def __repr__(self):
    return """OPERATION: %s""" % (self.operation)

#Relations and attributes that are joined.
class JOIN:
  def __init__(self,relations):
    self.relations = relations

  def python2java(self,duncecap):
    joinBuilder = duncecap.JoinBuilder()
    for rel in self.relations:
      joinBuilder.addRel(rel.name,rel.attributes,rel.annotations)
    return joinBuilder.build()

  @staticmethod
  def java2python(jobject):
    nRels = jobject.getNumRels()
    relations = []
    for i in range(0,nRels):
      relations.append(RELATION(
        jobject.getRel(i).getName(),
        strip_unicode(jobject.getRel(i).getAttributes()),
        strip_unicode(jobject.getRel(i).getAnnotations())))
    return JOIN(relations)

  def __repr__(self):
    return """JOIN: %s""" % (self.relations)

class SELECT:
  def __init__(self,attribute,operation,value):
    self.attribute = attribute
    self.operation = operation
    self.value = value

  def python2java(self,filterBuilder):
    return filterBuilder.buildSelection(self.attribute,self.operation,self.value)

  @staticmethod
  def java2python(jobject):
    return SELECT(jobject.getAttr(),jobject.getOperation(),jobject.getValue())

  def __repr__(self):
    return """SELECT(%s,%s,%s)""" % (self.attribute,self.operation,self.value)

#Relations and attributes that are joined.
class FILTERS:
  def __init__(self,filters=[]):
    if filters:
      if not isinstance(filters[0],SELECT):
        raise Exception("Filters types incorrect.")
    self.filters = filters

  def python2java(self,duncecap):
    filterBuilder = duncecap.FilterBuilder()
    for f in self.filters:
      filterBuilder.addSelection(f.python2java(filterBuilder))
    return filterBuilder.build()

  @staticmethod
  def java2python(jobject):
    nFilters = jobject.getNumFilters()
    filters = []
    for i in range(0,nFilters):
      filters.append(SELECT.java2python(jobject.getSelect(i)))
    return FILTERS(filters)

  def __repr__(self):
    return """FILTERS: %s""" % (self.filters)

#Aggregations over attributes.
class AGGREGATE:
  def __init__(self,operation="",attributes=[],init="",expression=""):
    self.operation = operation
    self.attributes = attributes
    self.init = init
    self.expression = expression

  def __repr__(self):
    return """(%s,%s,%s,%s)""" % (self.operation,self.attributes,self.init,self.expression)


class AGGREGATES:
  def __init__(self,aggregates):
    if aggregates:
      if not isinstance(aggregates[0],AGGREGATE):
        raise Exception("Aggregate types incorrect.")
    self.aggregates = aggregates

  def python2java(self,duncecap):
    aggBuilder = duncecap.AggregationsBuilder()
    for agg in self.aggregates:
      aggBuilder.addAggregation(
        agg.operation,
        agg.attributes,
        agg.init,
        agg.expression)
    return aggBuilder.build()

  @staticmethod
  def java2python(jobject):
    nAggs = jobject.getNumAggregations()
    aggs = []
    for i in range(0,nAggs):
      aggs.append(AGGREGATE(
        jobject.getOperation(i),
        strip_unicode(jobject.getAttributes(i)),
        jobject.getInit(i),
        jobject.getExpression(i)))
    return AGGREGATES(aggs)

  def __repr__(self):
    return """AGGREGATES: %s""" % (self.aggregates)
