///////////////////////////////////////////////////////////////////////////////
// Generates the code that loads and creates the database.
//
///////////////////////////////////////////////////////////////////////////////
package duncecap

import scala.collection.mutable.ListBuffer
import sys.process._
import java.io.{FileWriter, File, BufferedWriter}
import scala.collection.mutable.Set
import java.nio.file.{Paths, Files}
import util.control.Breaks._

object Trie{
  //loads the relations from disk (if nesc.)
  //and encodes them, then spills the encodings to disk
  //next builds the tries and spills to disk
  def run(db:DBInstance,rel:Relation){
    val ehhome = sys.env("EMPTYHEADED_HOME")

    if(Files.notExists(Paths.get(s"""${db.folder}/libs/trie_${rel.name}"""))){
      val mvdir = s"""cp -rf ${ehhome}/cython/trie ${db.folder}/libs/trie_${rel.name}"""
      mvdir.!
      Seq("sed","-i",
        ".bak",s"s|#PTrie#|PTrie_${rel.name}|g",
        s"${db.folder}/libs/trie_${rel.name}/PTrie.pyx",
        s"${db.folder}/libs/trie_${rel.name}/setup.py").!
      s"""mv ${db.folder}/libs/trie_${rel.name}/PTrie.pyx ${db.folder}/libs/trie_${rel.name}/PTrie_${rel.name}.pyx""".!
      
      s"""mv ${db.folder}/libs/trie_${rel.name}/PTrie.pxd ${db.folder}/libs/trie_${rel.name}/PTrie_${rel.name}.pxd""".!
     
    //some code to get the right types for the dataframe
    val pythonappendcode = new StringBuilder()
    var i = 0
    pythonappendcode.append(s"""
cdef inline convert(vector[void*] data, length):
  df = pd.DataFrame()""")
    rel.schema.attributeTypes.foreach(attr => {
      attr match {
        case "uint32_t" => pythonappendcode.append(s""" 
  col = uint32c2np(data.at(${i}),length)""")
        case "uint64_t" => pythonappendcode.append(s""" 
  col = uint64c2np(data.at(${i}),length)""")
        case "int32_t" => pythonappendcode.append(s""" 
  col = int32c2np(data.at(${i}),length)""")
        case "int64_t" => pythonappendcode.append(s""" 
  col = int64c2np(data.at(${i}),length)""")
        case "float" => pythonappendcode.append(s""" 
  col = floatc2np(data.at(${i}),length)""")
        case "double" => pythonappendcode.append(s""" 
  col = doublec2np(data.at(${i}),length)""")
        case _ => 
          throw new Exception("Could not find type to transfer to dataframe.")
      }
      pythonappendcode.append(s"""
  df[${i}] = col""")
      i+=1
    })
    pythonappendcode.append(s"""
  return df""")
    val fw = new FileWriter(s"${db.folder}/libs/trie_${rel.name}/PTrie_${rel.name}.pyx", true)
    fw.write(s"\n${pythonappendcode}") 
    fw.close()

    val code = new StringBuilder()

    //generate code
    code.append(genWrapper(db,rel))

    val cppFilepath = db.folder+s"/libs/trie_${rel.name}/TrieWrapper.hpp"
    val file = new File(cppFilepath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(code.toString)
    bw.close()
    s"""clang-format -style=llvm -i ${cppFilepath}""" !

    } else {
      println("Relation has been loaded before.")
    }
  }

  private def findOrdering(db:DBInstance,rel:Relation,orderings:List[List[Int]]):List[Int] = {
    //search all orderings, find one and pin the ordering in the trie.
    var ordering = List(-1)
    breakable {
      orderings.foreach(order => {
        if(Files.exists(
          Paths.get(s"""${db.folder}/relations/${rel.name}/${rel.name}_${order.mkString("_")}"""))){
          ordering = order
          break
        }
     })
    }
    if(ordering.length == 1 && ordering(0) == -1){
      throw new Exception(s"Relation ${rel.name} not found in database.")
    }
    ordering
  }

  private def genWrapper(db:DBInstance,rel:Relation):StringBuilder = {
    val code = new StringBuilder()
    val annoType = if(rel.schema.annotationTypes.length == 1) rel.schema.annotationTypes(0) else "void*"
    
    val orderings = (0 until rel.schema.attributeTypes.length).toList.permutations.toList
    val ordering = findOrdering(db,rel,orderings)
    val encodings = rel.schema.attributeTypes.distinct

    code.append(s""" 
#ifndef _LOAD_H_
#define _LOAD_H_

#include <iostream>
#include <vector>
#include <unordered_map>

#include "Trie.hpp"
#include "Encoding.hpp"
#include "utils/timer.hpp"

typedef std::unordered_map<std::string,void*> mymap;

/*
Loads relations from disk.
Input: Map from Name -> (void*)Trie*
Output: (void*)Trie*
*/
void* get(mymap* map){
  auto myclock = timer::start_clock();
  Trie<${annoType},ParMemoryBuffer>* mytrie = NULL;
  std::unordered_map<std::string,void*>::const_iterator got = map->find("${rel.name}_${ordering.mkString("_")}");
  if(got != map->end()){
    std::cout << "Trie in memory." << std::endl;
    return got->second;
  }""")
    encodings.foreach(enc => {
        code.append(s"""
  auto e_loading_${enc} = timer::start_clock();
  Encoding<${enc}> *Encoding_${enc} = Encoding<${enc}>::from_binary(
      "${db.folder}/encodings/${enc}/");
  timer::stop_clock("LOADING ENCODINGS ${enc}", e_loading_${enc});
""")})
    code.append(s"""mytrie = Trie<${annoType},ParMemoryBuffer>::load("${db.folder}/relations/${rel.name}/${rel.name}_${ordering.mkString("_")}");
  map->insert(std::pair<std::string,void*>("${rel.name}_${ordering.mkString("_")}",mytrie));""")
    ordering.foreach(o => {
      code.append(s"""
  mytrie->encodings.push_back((void*)Encoding_${rel.schema.attributeTypes(o)});""")
    })
    code.append(s"""
  timer::stop_clock("LOADING ${rel.name}_${ordering.mkString("_")}",myclock);
  return (void*)mytrie;
}

std::vector<void*> getDF(Trie<${annoType},ParMemoryBuffer>* mytrie){
  auto myclock = timer::start_clock();
  assert(mytrie != NULL);
  std::vector<void*> result;""")
    //allocate vector buffers
    var i = 0
    rel.schema.attributeTypes.foreach( attr => {
      code.append(s"""std::vector<${attr}>* result_${i} = new std::vector<${attr}>();
      result_${i}->resize(mytrie->num_rows);
      """)
      i += 1
    })
    //fill in those buffers
    code.append(s"""
  size_t i = 0;
  mytrie->foreach([&](std::vector<uint32_t>* v, ${annoType} a){""")
    i = 0
    rel.schema.attributeTypes.foreach(attr => {
      code.append(s"""
        Encoding<${attr}>* Encoding_${i} = (Encoding<${attr}>*)mytrie->encodings.at(${i});
        result_${i}->at(i) = Encoding_${i}->key_to_value.at(v->at(${i}));""")
      i += 1
    })
    code.append("""i++;});""")
    //pack buffers into the result
    i = 0
    rel.schema.attributeTypes.foreach( attr => {
      code.append(s"""result.push_back((void*)(result_${i}->data()));""")
      i += 1
    })
    code.append("""
    return result;
}

void save(mymap* map){
  std::cout << "Save all orderings to disk" << std::endl;
}

void load(mymap* map){""")
    encodings.foreach(enc => {
        code.append(s"""
auto e_loading_${enc} = timer::start_clock();
  Encoding<${enc}> *Encoding_${enc} = Encoding<${enc}>::from_binary(
      "${db.folder}/encodings/${enc}/");
  timer::stop_clock("LOADING ENCODINGS ${enc}", e_loading_${enc});
""")
    })
    orderings.foreach(ord => {
      code.append(s"""{
    auto myclock = timer::start_clock();
    Trie<${annoType},ParMemoryBuffer>* mytrie = NULL;
    std::unordered_map<std::string,void*>::const_iterator got = map->find("${rel.name}_${ord.mkString("_")}");
    if(got != map->end()){
      std::cout << "Trie in memory." << std::endl;
    } 
    mytrie = Trie<${annoType},ParMemoryBuffer>::load("${db.folder}/relations/${rel.name}/${rel.name}_${ord.mkString("_")}");
    map->insert(std::pair<std::string,void*>("${rel.name}_${ord.mkString("_")}",mytrie));
    timer::stop_clock("LOADING ${rel.name}_${ord.mkString("_")}",myclock);""")
      ord.foreach(o => {
        code.append(s"""
        mytrie->encodings.push_back((void*)Encoding_${rel.schema.attributeTypes(o)});""")
      })
    code.append("}")
    })
    code.append(s"""
}
#endif
""")
    code
  }
}