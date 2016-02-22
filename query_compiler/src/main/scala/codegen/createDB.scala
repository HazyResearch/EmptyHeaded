///////////////////////////////////////////////////////////////////////////////
// Generates the code that loads and creates the database.
//
///////////////////////////////////////////////////////////////////////////////
package duncecap

import scala.collection.mutable.ListBuffer
import sys.process._
import java.io.{FileWriter, File, BufferedWriter}
import scala.collection.mutable.Set

object CreateDB{
  //loads the relations from disk (if nesc.)
  //and encodes them, then spills the encodings to disk
  //next builds the tries and spills to disk
  def loadAndEncode(db:DBInstance,hash:String){
    val ehhome = sys.env("EMPTYHEADED_HOME")
    val mvdir = s"""cp -rf ${ehhome}/cython/createDB ${db.folder}/libs/createDB"""
    mvdir.!
    Seq("sed","-i",
      s"s/#DFMap#/DFMap_${hash}/g",
      s"${db.folder}/libs/createDB/DFMap.pyx",
      s"${db.folder}/libs/createDB/setup.py").!
    s"""mv ${db.folder}/libs/createDB/DFMap.pyx ${db.folder}/libs/createDB/DFMap_${hash}.pyx""".!
  
    //generate code
    genLoadAndEncode(db)
    genBuild(db)
  }

  private def genBuild(db:DBInstance){
    val code = new StringBuilder()

    code.append(buildWrapper(db))

    val cppFilepath = db.folder+"/libs/createDB/build.hpp"
    val file = new File(cppFilepath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(code.toString)
    bw.close()
    s"""clang-format -style=llvm -i ${cppFilepath}""" !
  }

  private def genLoadAndEncode(db:DBInstance){
    val code = new StringBuilder()

    code.append(loadAndEncodeWrapper(db))

    val cppFilepath = db.folder+"/libs/createDB/loadAndEncode.hpp"
    val file = new File(cppFilepath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(code.toString)
    bw.close()
    s"""clang-format -style=llvm -i ${cppFilepath}""" !
  }

  private def load(db:DBInstance):String = {
    val code = new StringBuilder()
    val encodings = Set[String]()
    db.relations.foreach(r => {
      val numCols = r.schema.attributeTypes.length + r.schema.annotationTypes.length
      (r.schema.attributeTypes++r.schema.annotationTypes).foreach(atype => {
        if(!encodings.contains(atype)){
          encodings += atype
          code.append(s""" 
SortableEncodingMap<${atype}> EncodingMap_${atype};
Encoding<${atype}> Encoding_${atype};
          """)
        }
      })
      code.append(s"""
std::vector<void*> ColumnStore_${r.name};
size_t NumRows_${r.name} = 0;
{
auto start_time = timer::start_clock();""")
      if(r.df){
        code.append(s"""
mypair Pair_${r.name} = map->at("${r.name}");
NumRows_${r.name} = Pair_${r.name}.first;
ColumnStore_${r.name} = Pair_${r.name}.second;
assert(ColumnStore_${r.name}.size() == ${numCols});""")
        val atypes = r.schema.attributeTypes++r.schema.annotationTypes
        (0 until atypes.length).foreach(i => {
          code.append(s"""
const ${atypes(i)}* col_${i} = (const ${atypes(i)}*)ColumnStore_${r.name}.at(${i}); 
for(size_t i = 0; i < NumRows_${r.name}; i++){
  EncodingMap_${r.schema.attributeTypes(i)}.update(col_${i}[i]);
}
          """)
        }) 
      } else {
        code.append(s"""
tsv_reader f_reader("${r.filename}");
char *next = f_reader.tsv_get_first();""")
        val atypes = r.schema.attributeTypes++r.schema.annotationTypes
        (0 until atypes.length).foreach(i => {
          code.append(s"""
std::vector<${atypes(i)}>* v_${i} = new std::vector<${atypes(i)}>();
          """)
        })
        code.append(s"""
while (next != NULL) {
        """)
        var i = 0
        r.schema.attributeTypes.foreach(at => {
          code.append(s"""
  const ${at} value_${i} = utils::from_string<${at}>(next);
  v_${i}->push_back(value_${i});
  EncodingMap_${at}.update(value_${i});
  next = f_reader.tsv_get_next();""")
          i += 1
        })
        r.schema.annotationTypes.foreach(at => {
          code.append(s""" 
  const ${at} value_${i} = utils::from_string<${at}>(next);
  v_${i}.push_back(value_${i});
  next = f_reader.tsv_get_next();
          """)
        })
        code.append(s"""
  NumRows_${r.name}++;
}""")
        (0 until atypes.length).foreach(i => {
          code.append(s"""
ColumnStore_${r.name}.push_back((void*)v_${i}->data());""")
        })
      }
      code.append(s"""
timer::stop_clock("READING ${r.name}", start_time);
}""")
    })
    s"mkdir ${db.folder}/encodings".!
    encodings.foreach(e => {
      s"mkdir ${db.folder}/encodings/${e}".!
      code.append(s"""
{
  auto start_time = timer::start_clock();
  Encoding_${e}.build(EncodingMap_${e}.get_sorted());
  timer::stop_clock("BUILDING ENCODING ${e}", start_time);
}
{
  auto start_time = timer::start_clock();
  Encoding_${e}.to_binary("${db.folder}/encodings/${e}/");
  timer::stop_clock("WRITING ENCODING ${e}", start_time);
}""")
    })
    code.toString()
  }

  private def encode(db:DBInstance):String = {
    val code = new StringBuilder()
    s"mkdir ${db.folder}/relations".!
    db.relations.foreach(r => {
      code.append(s"""{ 
auto start_time = timer::start_clock();
EncodedColumnStore EncodedColumnStore_${r.name}(
  NumRows_${r.name},
  ${r.schema.attributeTypes.length},
  ${r.schema.annotationTypes.length});""")
      var i = 0
      r.schema.attributeTypes.foreach(at => {
        code.append(s"""
EncodedColumnStore_${r.name}.add_column(
  Encoding_${at}.encode_column((${at}*)ColumnStore_${r.name}.at(${i}),NumRows_${r.name}),
  Encoding_${at}.num_distinct);""")
        i += 1
      })
      r.schema.annotationTypes.foreach(at => {
        code.append(s"""
EncodedColumnStore_${r.name}.add_annotation(
  ColumnStore_${r.name}.at(${i}),
  sizeof(${at}));""")
        i += 1
      })
      s"mkdir ${db.folder}/relations/${r.name}".!
      code.append(s"""
EncodedColumnStore_${r.name}.to_binary("${db.folder}/relations/${r.name}/");
timer::stop_clock("ENCODING ${r.name}", start_time);}""")
    })
    code.toString()
  }

  private def loadAndEncodeWrapper(db:DBInstance):StringBuilder = {
    val code = new StringBuilder()
    code.append(s"""
#include <iostream>
#include <vector>
#include <unordered_map>
#include "utils/thread_pool.hpp"
#include "intermediate/intermediate.hpp"
#include "Encoding.hpp"
#include "utils/timer.hpp"
#include "utils/io.hpp"

typedef std::vector<void*> myvector;
typedef std::pair<size_t,myvector> mypair;

void loadAndEncode(std::unordered_map<std::string,mypair>* map){
  thread_pool::initializeThreadPool();
  ${load(db)}
  ${encode(db)}
  thread_pool::deleteThreadPool();
}""")
  code
  }

  private def build(db:DBInstance):StringBuilder = {
    val code = new StringBuilder()
    db.relations.foreach(r => {
      val orderings = (0 until r.schema.attributeTypes.length).toList.permutations.toList
      if(r.schema.annotationTypes.length > 1)
        throw new Exception("EmptyHeaded currently only supports one annotation per relation.")
      val annoType = if(r.schema.annotationTypes.length == 1) r.schema.annotationTypes(0) else "void*"
      val annoIn = if(r.schema.annotationTypes.length == 1) "annotation.at(0)" else "annotation"

      code.append(s"""{ //load encoded relation
auto load_time = timer::start_clock();
EncodedColumnStore* EncodedColumnStore_${r.name} = EncodedColumnStore::from_binary("${db.folder}/relations/${r.name}/");
timer::stop_clock("LOADING ENCODED ${r.name}", load_time);""")
      orderings.foreach(ordering => {
        s"""mkdir ${db.folder}/relations/${r.name}/${r.name}_${ordering.mkString("_")}""".!
        s"""mkdir ${db.folder}/relations/${r.name}/${r.name}_${ordering.mkString("_")}/ram""".!
        code.append(s""" 
////////////////////emitReorderEncodedColumnStore////////////////////
{
std::vector<size_t> order_${ordering.mkString("_")} = {${ordering.mkString(",")}};
EncodedColumnStore *Encoded_${r.name}_${ordering.mkString("_")} =
    new EncodedColumnStore(EncodedColumnStore_${r.name},order_${ordering.mkString("_")});
Trie<${annoType},ParMemoryBuffer> *Trie_${r.name}_${ordering.mkString("_")} = NULL;
{
  auto start_time = timer::start_clock();
  // buildTrie
  Trie_${r.name}_${ordering.mkString("_")} = new Trie<${annoType},ParMemoryBuffer>( 
      "${db.folder}/relations/${r.name}/${r.name}_${ordering.mkString("_")}",
      &Encoded_${r.name}_${ordering.mkString("_")}->max_set_size,
      &Encoded_${r.name}_${ordering.mkString("_")}->data, 
      &Encoded_${r.name}_${ordering.mkString("_")}->${annoIn});
  timer::stop_clock("BUILDING TRIE ${r.name}_${ordering.mkString("_")}", start_time);
}
////////////////////emitWriteBinaryTrie////////////////////
{
  auto start_time = timer::start_clock();
  Trie_${r.name}_${ordering.mkString("_")}->save();
  timer::stop_clock("WRITING BINARY TRIE ${r.name}_${ordering.mkString("_")}", start_time);
}
delete Trie_${r.name}_${ordering.mkString("_")};
}""")
      })
      code.append(s"""}""")
    })
    code
  }
  private def buildWrapper(db:DBInstance):StringBuilder = {
    val code = new StringBuilder()
    code.append(s"""
#include "Trie.hpp"
#include "utils/timer.hpp"
#include "intermediate/intermediate.hpp"

typedef std::vector<void*> myvector;
typedef std::pair<size_t,myvector> mypair;

void build(std::unordered_map<std::string,mypair>* map){
  thread_pool::initializeThreadPool();
  ${build(db)}
  thread_pool::deleteThreadPool();
}""")
  code
  }
}