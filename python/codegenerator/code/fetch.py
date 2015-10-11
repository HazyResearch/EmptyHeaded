def loadRelation(path,name,annotationType,memType):
 	code = """
    Trie<%(annotationType)s,%(memType)s> *Trie_%(name)s = NULL;
    {
      auto start_time = timer::start_clock();
      // loadTrie
      Trie_%(name)s = Trie<%(annotationType)s,%(memType)s>::load("%(path)s");
      timer::stop_clock("LOADING TRIE %(name)s", start_time);
    }"""% locals()
	return code

def setResult(name):
 	return """result = (void*)Trie_%(name)s;"""% locals()