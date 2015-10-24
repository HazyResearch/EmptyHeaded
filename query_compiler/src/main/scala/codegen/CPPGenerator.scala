package DunceCap

import scala.util.parsing.json._
import scala.io._
import java.nio.file.{Paths, Files}
import java.io.{FileWriter, File, BufferedWriter}
import sys.process._
import scala.collection.mutable

object CPPGenerator {  
  def GHDFromJSON(filename:String):Map[String,Any] = {
    val fileContents = Source.fromFile(filename).getLines.mkString
    val ghd:Map[String,Any] = JSON.parseFull(fileContents) match {
      case Some(map: Map[String, Any]) => map
      case _ => Map()
    }
    return ghd
  }

  def run(ghd:QueryPlan) = {
    val cpp = new StringBuilder()
    
    val includeCode = getIncludes(ghd)
    
    val cppCode = emitLoadRelations(ghd.relations)
    cppCode.append(emitInitializeOutput(ghd.output))
    ghd.ghd.foreach(bag => {
      cppCode.append(emitNPRR(bag.name==ghd.output.name,bag))
    })

    cpp.append(getCode(includeCode,cppCode))

    val cppFilepath = "/Users/caberger/Documents/Research/code/eh_python/storage_engine/generated/cgen.cpp"
    val file = new File(cppFilepath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(cpp.toString)
    bw.close()
    s"""clang-format -style=llvm -i ${cppFilepath}""" !
  } 

  def getIncludes(ghd:QueryPlan) : StringBuilder = {
    val code = new StringBuilder()
    code.append("")
    return code
  }

  def getCode(includes:StringBuilder,run:StringBuilder) : String ={
    return s"""
      #include "../codegen/Query.hpp"
      ${includes.toString}

      Query::Query(){
        thread_pool::initializeThreadPool();
      }

      void Query::run(){
        ${run.toString}
        thread_pool::deleteThreadPool();
      }
    """
  }

  /////////////////////////////////////////////////////////////////////////////
  //Loads the relations and encodings needed for a query.
  /////////////////////////////////////////////////////////////////////////////
  def emitLoadRelations(relations:List[QueryPlanRelationInfo]) : StringBuilder = {
    val code = new StringBuilder()
    code.append(relations.map(r => {
      s"""Trie<${r.annotation},${Environment.config.memory}>* Trie_${r.name}_${r.ordering.mkString("_")} = NULL;
      {
        auto start_time = timer::start_clock();
        Trie_R_0_1 = Trie<void *,${Environment.config.memory}>::load( 
          "${Environment.config.database}/relations/${r.name}/${r.name}_${r.ordering.mkString("_")}"
        );
        timer::stop_clock("LOADING Trie ${r.name}_${r.ordering.mkString("_")}", start_time);      
      }
      """
    }).reduce((a,b) => a+b))

    val encodings = relations.flatMap(r => {
      val schema = Environment.config.schemas.find(schema => schema.name == r.name)
      schema match {
        case Some(s) => {
          s.attributes
        }
        case _ => 
          throw new IllegalArgumentException("Schema not found.");
      }
    }).distinct

    code.append(encodings.map(attr => {
      s"""
      auto e_loading_${attr.encoding} = timer::start_clock();
      Encoding<long> *Encoding_${attr.encoding} = Encoding<${attr.attrType}>::from_binary(
          "${Environment.config.database}/encodings/${attr.encoding}/");
      (void)Encoding_${attr.encoding};
      timer::stop_clock("LOADING ENCODINGS ${attr.encoding}", e_loading_${attr.encoding});
      """
    }).reduce((a,b) => a+b) )
    return code
  }
  /////////////////////////////////////////////////////////////////////////////
  //Initialize the output trie for the query.
  /////////////////////////////////////////////////////////////////////////////
  def emitInitializeOutput(output:QueryPlanOutputInfo) : StringBuilder = {
    val code = new StringBuilder()

    s"""mkdir -p ${Environment.config.database}/relations/${output.name}""" !

    s"""mkdir -p ${Environment.config.database}/relations/${output.name}/${output.name}_${output.ordering.mkString("_")}""" !

    code.append(s"""
      Trie<${output.annotation},${Environment.config.memory}> *Trie_${output.name} = new Trie<${output.annotation},${Environment.config.memory}>("${Environment.config.database}/relations/${output.name}/${output.name}_${output.ordering.mkString("_")}",${output.ordering.length},${output.annotation != "void*"});
    """)
    println(output)
    return code
  }

  /////////////////////////////////////////////////////////////////////////////
  //Emit NPRR code
  /////////////////////////////////////////////////////////////////////////////
  def emitIntermediateTrie(name:String,annotation:String,num:Int) : StringBuilder = {
    val code = new StringBuilder()
    assert(Environment.config.memory != "ParMMapBuffer")
    code.append(s""" 
      Trie<${annotation},${Environment.config.memory}> *Trie_${name} = new Trie<${annotation},${Environment.config.memory}>("${Environment.config.database}/relations/${name}",${num},${annotation != "void*"});
    """)
    return code
  }

  def emitParallelBuilder(name:String,annotation:String) : StringBuilder = {
    val code = new StringBuilder()
    code.append(s"""ParTrieBuilder<${annotation},${Environment.config.memory}> Builders(Trie_${name});""")

    return code
  }

  def emitParallelIterators(relations:List[QueryPlanRelationInfo]) : (StringBuilder,Map[String,List[String]]) = {
    val code = new StringBuilder()
    val dependers = mutable.ListBuffer[(String,String)]()
    relations.foreach(r => {
      val name = r.name + "_" + r.ordering.mkString("_")
      r.attributes.foreach(attr => {
        attr.foreach(a => {
          a.foreach(i => {
            dependers += ((i,r.name + "_" + a.mkString("_")))
          })
          code.append(s"""const ParTrieIterator<${r.annotation},${Environment.config.memory}> Iterators_${r.name}_${a.mkString("_")}(Trie_${name});""")
        })
      })
    })
    val iteratorAccessors = dependers.groupBy(_._1).map(m =>{
      ((m._1 -> m._2.map(_._2).toList.distinct))
    })
    return (code,iteratorAccessors)
  }
  

  def nprrRecursiveCall(head:Option[QueryPlanNPRRInfo],tail:List[QueryPlanNPRRInfo]) : StringBuilder = {
    val code = new StringBuilder()
    (head,tail) match {
      case (Some(a),List()) =>
        println("last attr")
      case (Some(a),_) =>
        //build 
        //allocate
        //foreach
        println("middle attr")
        nprrRecursiveCall(tail.headOption,tail.tail)
        //set
      case (None,_) =>
        throw new IllegalArgumentException("Should not reach this state.")
    }
    return code
  }

  def emitHeadBuildCode(head:Option[QueryPlanNPRRInfo]) : StringBuilder = {
    val code = new StringBuilder()
    head match {
      case Some(h) => {
        h.materialize match {
          case true => {
            h.accessors.length match {
              case 0 =>
                throw new IllegalArgumentException("This probably not be occuring.")
              case 1 =>
                code.append(s"""Builders.build_set(Iterators_${h.accessors(0).name}_${h.accessors(0).attrs.mkString("_")}.head);""")
              case 2 =>
                code.append(s"""Builders.build_set(Iterators_${h.accessors(0).name}_${h.accessors(0).attrs.mkString("_")}.head,Iterators_${h.accessors(1).name}_${h.accessors(1).attrs.mkString("_")}.head);""")
              case 3 =>
                throw new IllegalArgumentException("3 arguments not supported yet.")
            } 
          }
          case false => {
            h.accessors.length match {
              case 0 =>
                throw new IllegalArgumentException("This probably not be occuring.")
              case 1 =>
                code.append(s"""Builders.build_aggregated_set(Iterators_${h.accessors(0).name}_${h.accessors(0).attrs.mkString("_")}.head);""")
              case 2 =>
                code.append(s"""Builders.build_aggregated_set(Iterators_${h.accessors(0).name}_${h.accessors(0).attrs.mkString("_")}.head,Iterators_${h.accessors(1).name}_${h.accessors(1).attrs.mkString("_")}.head);""")
              case 3 =>
                throw new IllegalArgumentException("3 arguments not supported yet.")
            } 
          }
        } 
      }
      case _ =>
       throw new IllegalArgumentException("This probably not be occuring.")
    }
    return code
  }

  def emitHeadAllocations(head:QueryPlanNPRRInfo) : StringBuilder = {
    val code = new StringBuilder()
    (head.annotation,head.nextMaterialized) match {
      case (Some(annotation),None) =>
        code.append("Builders.allocate_annotation();")
      case (None,Some(nextMaterialized)) =>
        code.append("Builders.allocate_next();")
      case (Some(a),Some(b)) =>
        throw new IllegalArgumentException("Undefined behaviour.")
      case _ =>
    }
    code
  }

  def emitHeadParForeach(head:QueryPlanNPRRInfo,outputAnnotation:String,relations:List[QueryPlanRelationInfo],iteratorAccessors:Map[String,List[String]]) : StringBuilder = {
    val code = new StringBuilder()

    head.materialize match {
      case true =>
        code.append(s"""Builders.par_foreach_builder([&](const size_t tid, const uint32_t a_i, const uint32_t a_d) {""")
      case false =>
        code.append(s"""Builders.par_foreach_aggregate([&](const size_t tid, const uint32_t a_d) {""")
    }

    //get iterator and builder for each thread
    code.append(s"""TrieBuilder<${outputAnnotation},${Environment.config.memory}>* Builder = Builders.builders.at(tid);""")
    relations.foreach(r => {
      val name = r.name + "_" + r.ordering.mkString("_")
      r.attributes.foreach(attr => {
        attr.foreach(a => {
          code.append(s"""TrieIterator<${r.annotation},${Environment.config.memory}>* Iterator_${r.name}_${a.mkString("_")} = Iterators_${r.name}_${a.mkString("_")}.iterators.at(tid);""")
        })
      })
    })

    code
  }

  def emitNPRR(output:Boolean,bag:QueryPlanBagInfo) : StringBuilder = {
    val code = new StringBuilder()

    //Emit the output trie for the bag.
    output match {
      case false => {
        code.append(emitIntermediateTrie(bag.name,bag.annotation,bag.attributes.length))
      }
      case _ =>
    }

    code.append("{")
    code.append(emitParallelBuilder(bag.name,bag.annotation))
 
    //fixme Susan should add this to the query compiler.
    val (parItCode,iteratorAccessors) = emitParallelIterators(bag.relations)
    code.append(parItCode)
    code.append(emitHeadBuildCode(bag.nprr.headOption))

    println("ITERATOR ACCESSORS: " + iteratorAccessors)

    val remainingAttrs = bag.nprr.tail
    if(remainingAttrs.length > 0){
      code.append(emitHeadAllocations(bag.nprr.head))
      code.append(emitHeadParForeach(bag.nprr.head,bag.annotation,bag.relations,iteratorAccessors))
      nprrRecursiveCall(remainingAttrs.headOption,remainingAttrs.tail)
    
      code.append("});")
    }

    code.append("}")
    println(output)
    println(bag)
    return code
  }
}
