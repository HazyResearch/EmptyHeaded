from sets import Set

def declareColumnStore(name,types):
	return """ColumnStore<%(types)s> *ColumnStore_%(name)s = new ColumnStore<%(types)s>();"""% locals()

def declareAnnotationStore(name):
  	return """std::vector<%(name)s> *annotation_R = new std::vector<%(name)s>();"""% locals()

def declareEncoding(e):
	name,types = e
  	return """
  		SortableEncodingMap<%(types)s> *EncodingMap_%(name)s = new SortableEncodingMap<%(types)s>();
  		Encoding<%(types)s> *Encoding_%(name)s = new Encoding<%(types)s>();
	"""% locals()

def readRelationFromTSV(name,encodings,path):
	code = """{
	    auto start_time = debug::start_clock();
	    tsv_reader f_reader(
	        "%(path)s");
	    char *next = f_reader.tsv_get_first();
	    while (next != NULL) {"""% locals()
	i = 0
	for ename,types in encodings:
		code += \
		"""EncodingMap_%(ename)s->update(ColumnStore_%(name)s->append_from_string<%(i)s>(next));
		   next = f_reader.tsv_get_next();"""% locals()
		i+=1
	code += \
	"""ColumnStore_%(name)s->num_rows++;
	   }
       debug::stop_clock("READING %(name)s from disk",start_time);
    }"""% locals()
	return code

def buildAndDumpEncoding(path,encoding):
	name,types = encoding
	return """
	  {
	    auto start_time = debug::start_clock();
	    Encoding_%(name)s->build(EncodingMap_%(name)s->get_sorted());
	    delete EncodingMap_%(name)s;
	    debug::stop_clock("BUILDING ENCODINGS", start_time);
	  }
	  {
	    auto start_time = debug::start_clock();
	    Encoding_%(name)s->to_binary(
	        "%(path)s/encodings/%(name)s/");
	    debug::stop_clock("WRITING ENCODING node", start_time);
	  }
	"""% locals()

def encodeRelation(path,name,encodings,annotationType):
	code = \
"""
EncodedColumnStore<%(annotationType)s> *Encoded_%(name)s =
  new EncodedColumnStore<%(annotationType)s>(annotation_%(name)s);
{
auto start_time = debug::start_clock();
// encodeRelation
"""% locals()
	i = 0
	for e in encodings:
		ename,types = e
		code += \
"""Encoded_%(name)s->add_column(Encoding_%(ename)s->encode_column(&ColumnStore_%(name)s->get<%(i)s>()),
                      Encoding_%(ename)s->num_distinct);"""% locals()
		i += 1
	code += \
"""
Encoded_%(name)s->to_binary(
    "%(path)s/relations/%(name)s/");
debug::stop_clock("ENCODING %(name)s", start_time);
}
"""% locals()
	return code

def loadEncodedRelation(path,name):
	return \
"""
    EncodedColumnStore<void *> *Encoded_%(name)s = NULL;
    {
      auto start_time = debug::start_clock();
      Encoded_%(name)s = EncodedColumnStore<void *>::from_binary(
          "%(path)s/relations/%(name)s/");
      debug::stop_clock("LOADING ENCODED RELATION %(name)s", start_time);
    }
"""% locals()

def buildOrder(path,name,ordering,annotationType):
	rname = name
	name += "_"+"_".join(map(str,ordering))
	code = """ EncodedColumnStore<void *> *Encoded_%(name)s =
        new EncodedColumnStore<void *>(&Encoded_%(rname)s->annotation);
    {
      auto start_time = debug::start_clock();"""% locals()
	for i in ordering:
   		code += \
"""Encoded_%(name)s->add_column(Encoded_%(rname)s->column(%(i)s),
                                Encoded_%(rname)s->max_set_size.at(%(i)s));"""% locals()
	code+=\
  """debug::stop_clock("REORDERING ENCODING %(name)s", start_time);
    } 
    Trie<%(annotationType)s> *Trie_%(name)s = NULL;
    {
      auto start_time = debug::start_clock();
      // buildTrie
      Trie_%(name)s = new Trie<%(annotationType)s>( 
          &Encoded_%(name)s->max_set_size,
          &Encoded_%(name)s->data, 
          &Encoded_%(name)s->annotation);
      debug::stop_clock("BUILDING TRIE %(name)s", start_time);
    }
    {
      auto start_time = debug::start_clock();
      Trie_%(name)s->to_binary(
          "%(path)s/relations/%(rname)s/%(name)s/");
      debug::stop_clock("WRITING BINARY TRIE %(name)s", start_time);
    }
    Trie_%(name)s->memory->free();"""% locals()
 	return code
