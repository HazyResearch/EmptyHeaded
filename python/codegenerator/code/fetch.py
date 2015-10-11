def loadRelation(path,name,ordering,annotationType,memType):
 	rname = name
	name += "_"+"_".join(map(str,ordering))
 	code = """
    Trie<%(annotationType)s,%(memType)s> *Trie_%(name)s = NULL;
    {
      auto start_time = debug::start_clock();
      // loadTrie
      Trie_%(name)s = Trie<%(annotationType)s,%(memType)s>::load("%(path)s");
      debug::stop_clock("LOADING TRIE %(name)s", start_time);
    }
    """% locals()
    return code

def setResult(name):
 	return """result = (void*)Trie_%(name)s"""% locals()