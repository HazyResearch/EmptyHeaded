## Stores the configuration of the database

class Config:
  system = "emptyheaded" #delite/spark
  num_threads = 1
  num_sockets = 4
  layout = "hybrid" #EmptyHeaded only
  memory = "RAM" #EmptyHeaded only

  @staticmethod
  def java2python(jobject):
    self = Config()
    self.num_threads = jobject.getNumThreads()
    self.num_sockets = jobject.getNumSockets()
    self.layout = jobject.getLayout()
    self.memory = jobject.getMemory()
    return self

  #for printing purposes
  def __repr__(self):
    return """(system: %s, num_threads: %s,
\t\tnum_sockets: %s, layout: %s, memory: %s)"""\
     % (self.system, \
      str(self.num_threads), \
      str(self.num_sockets), \
      self.layout,self.memory)