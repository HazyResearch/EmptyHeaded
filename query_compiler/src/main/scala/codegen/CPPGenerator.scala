package DunceCap

import scala.util.parsing.json._
import scala.io._
import java.io.{FileWriter, File, BufferedWriter}
import sys.process._
import scala.collection.mutable

object CPPGenerator {  
  type IteratorAccessors = Map[String,List[(String,Int,Int)]]
  type Encodings = Map[String,Attribute]

  def GHDFromJSON(filename:String):Map[String,Any] = {
    val fileContents = Source.fromFile(filename).getLines.mkString
    val ghd:Map[String,Any] = JSON.parseFull(fileContents) match {
      case Some(map: Map[String, Any]) => map
      case _ => Map()
    }
    return ghd
  }

  def run(qp:QueryPlan) = {
    var outputAttributes:List[Attribute] = List()
    val intermediateRelations:mutable.Map[String,List[Attribute]] = mutable.Map()
    val cpp = new StringBuilder()
    val includeCode = getIncludes(qp)
    val cppCode = emitLoadRelations(qp.relations)
    cppCode.append(emitInitializeOutput(qp.output))
    qp.ghd.reverse.foreach(bag => {
      val (bagCode,bagOutput) = emitNPRR(bag.name==qp.output.name,bag,intermediateRelations.toMap)
      intermediateRelations += ((bag.name -> bagOutput))
      outputAttributes = bagOutput
      cppCode.append(bagCode)
    })
    cppCode.append(emitEndQuery(qp.output))

    cpp.append(getCode(includeCode,cppCode))

    val newSchema = ((qp.output.name -> Schema(outputAttributes,List(qp.output.ordering),qp.output.annotation)))
    val newRelation = ((qp.output.name+"_"+qp.output.ordering.mkString("_"))->"disk")
    Environment.config.schemas = Environment.config.schemas+newSchema
    Environment.config.relations = Environment.config.relations+newRelation
    Environment.config.resultName = qp.output.name
    Environment.config.resultOrdering = qp.output.ordering
    Environment.toJSON()

    val cppFilepath = sys.env("EMPTYHEADED_HOME")+"/storage_engine/codegen/Query.cpp"
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
      #include "Query_HASHSTRING.hpp"
      #include "utils/thread_pool.hpp"
      #include "utils/parallel.hpp"
      #include "Trie.hpp"
      #include "TrieBuilder.hpp"
      #include "TrieIterator.hpp"
      #include "utils/timer.hpp"
      #include "utils/ParMemoryBuffer.hpp"
      #include "Encoding.hpp"
      ${includes.toString}

      void Query_HASHSTRING::run_HASHSTRING(){
        thread_pool::initializeThreadPool();
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
        Trie_${r.name}_${r.ordering.mkString("_")} = Trie<void *,${Environment.config.memory}>::load( 
          "${Environment.config.database}/relations/${r.name}/${r.name}_${r.ordering.mkString("_")}"
        );
        timer::stop_clock("LOADING Trie ${r.name}_${r.ordering.mkString("_")}", start_time);      
      }
      """
    }).reduce((a,b) => a+b))

    val encodings = relations.flatMap(r => {
      val schema = Environment.config.schemas.get(r.name)
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
      auto query_timer = timer::start_clock();
      Trie<${output.annotation},${Environment.config.memory}> *Trie_${output.name}_${output.ordering.mkString("_")} = new Trie<${output.annotation},${Environment.config.memory}>("${Environment.config.database}/relations/${output.name}/${output.name}_${output.ordering.mkString("_")}",${output.ordering.length},${output.annotation != "void*"});
      par::reducer<size_t> num_rows_reducer(0,[](size_t a, size_t b) { return a + b; });
    """)
    return code
  }

  def emitEndQuery(output:QueryPlanOutputInfo) : StringBuilder = {
    val code = new StringBuilder()

    code.append(s"""
      result_HASHSTRING = (void*) Trie_${output.name}_${output.ordering.mkString("_")};
      std::cout << "NUMBER OF ROWS: " << Trie_${output.name}_${output.ordering.mkString("_")}->num_rows << std::endl;
      timer::stop_clock("QUERY TIME", query_timer);
    """)

    return code
  }

  /////////////////////////////////////////////////////////////////////////////
  //Emit NPRR code
  /////////////////////////////////////////////////////////////////////////////
  def emitIntermediateTrie(name:String,annotation:String,num:Int) : StringBuilder = {
    val code = new StringBuilder()
    assert(Environment.config.memory != "ParMMapBuffer")
    val ordering = (0 until num).toList.mkString("_")
    code.append(s""" 
      Trie<${annotation},${Environment.config.memory}> *Trie_${name}_${ordering} = new Trie<${annotation},${Environment.config.memory}>("${Environment.config.database}/relations/${name}",${num},${annotation != "void*"});
    """)
    return code
  }

  def emitParallelBuilder(name:String,attributes:List[String],annotation:String,encodings:Encodings) : StringBuilder = {
    val code = new StringBuilder()
    val ordering = (0 until attributes.length).toList.mkString("_")
    code.append(s"""ParTrieBuilder<${annotation},${Environment.config.memory}> Builders(Trie_${name}_${ordering});""")
    attributes.foreach(attr => {
      code.append(s"""Builders.trie->encodings.push_back((void*)Encoding_${encodings(attr).encoding});""")
    })
    return code
  }

  def emitParallelIterators(relations:List[QueryPlanRelationInfo],intermediateRelations:Map[String,List[Attribute]]) : (StringBuilder,IteratorAccessors,Encodings) = {
    val code = new StringBuilder()
    val dependers = mutable.ListBuffer[(String,(String,Int,Int))]()
    val encodings = mutable.ListBuffer[(String,Attribute)]()
    relations.foreach(r => {
      val name = r.name + "_" + r.ordering.mkString("_")

      val relSchema = Environment.config.schemas.get(r.name)
      val interSchema = intermediateRelations.get(r.name)

      val enc = (relSchema,interSchema) match {
        case (Some(s),None) => {
          s.attributes
        } case (None,Some(a)) => {
          a
        } case _ => 
          throw new IllegalArgumentException("Schema not found.");
      }

      r.attributes.foreach(attr => {
        attr.foreach(a => {
          a.foreach(i => {
            encodings += ((i,enc(a.indexOf(i))))
            dependers += ((i,(r.name + "_" + a.mkString("_"),a.indexOf(i),a.length)))
          })
          code.append(s"""const ParTrieIterator<${r.annotation},${Environment.config.memory}> Iterators_${r.name}_${a.mkString("_")}(Trie_${name});""")
        })
      })
    })
    val retEncodings = encodings.groupBy(_._1).map(m => {
      val e = m._2.map(_._2).toList.distinct
      assert(e.length == 1) //one encoding per attribute
      ((m._1 -> e.head))
    })
    val iteratorAccessors = dependers.groupBy(_._1).map(m =>{
      ((m._1 -> m._2.map(_._2).toList.distinct))
    })
    return (code,iteratorAccessors,retEncodings)
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

  def emitInitHeadAnnotations(head:QueryPlanNPRRInfo,annotationType:String) : StringBuilder = {
    val code = new StringBuilder()
    (head.aggregation,head.materialize) match {
      case (Some(a),false) =>
        a.operation match {
          case "SUM" =>
            code.append(s"""par::reducer<${annotationType}> annotation_${head.name}(0,[](${annotationType} a, ${annotationType} b) { return a + b; });""")
          case _ =>
            throw new IllegalArgumentException("AGGREGATION NOT YET SUPPORTED.")
        }
      case _ =>
    }
    code
  }

  def emitSetHeadAnnotations(head:QueryPlanNPRRInfo,annotationType:String) : StringBuilder = {
    val code = new StringBuilder()
    (head.aggregation,head.materialize) match {
      case (Some(a),false) =>
        code.append(s"""Builders.trie->annotation = annotation_${head.name}.evaluate(0);""")
      case _ =>
    }
    code
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

  def emitHeadParForeach(head:QueryPlanNPRRInfo,outputAnnotation:String,relations:List[QueryPlanRelationInfo],iteratorAccessors:IteratorAccessors) : StringBuilder = {
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

    iteratorAccessors(head.name).foreach(i => {
      if(i._2 != (i._3-1)) //not for the last level
        code.append(s"""Iterator_${i._1}->get_next_block(${i._2},${head.name}_d);""")
    })

    code
  }

  def emitBuildCode(head:QueryPlanNPRRInfo,iteratorAccessors:IteratorAccessors) : StringBuilder = {
    val code = new StringBuilder()

    val relationNames = iteratorAccessors(head.name).map(_._1)
    val relationIndices = iteratorAccessors(head.name).map(_._2)
    head.materialize match {
      case true => {
        head.accessors.length match {
          case 0 =>
            throw new IllegalArgumentException("This probably not be occuring.")
          case 1 =>{
            val index1 = relationIndices(relationNames.indexOf( head.accessors(0).name + "_" + head.accessors(0).attrs.mkString("_") ) )
            code.append(s"""const size_t count_${head.name} = Builder->build_set(tid,Iterator_${head.accessors(0).name}_${head.accessors(0).attrs.mkString("_")}->get_block(${index1}));""")
          }
          case 2 =>{
            val index1 = relationIndices(relationNames.indexOf( head.accessors(0).name + "_" + head.accessors(0).attrs.mkString("_") ) )
            val index2 = relationIndices(relationNames.indexOf(head.accessors(1).name + "_" + head.accessors(1).attrs.mkString("_")))
            code.append(s"""const size_t count_${head.name} = Builder->build_set(tid,Iterator_${head.accessors(0).name}_${head.accessors(0).attrs.mkString("_")}->get_block(${index1}),Iterator_${head.accessors(1).name}_${head.accessors(1).attrs.mkString("_")}->get_block(${index2}));""")
          }
          case 3 =>
            throw new IllegalArgumentException("3 arguments not supported yet.")
        } 
      }
      case false => {
        head.accessors.length match {
          case 0 =>
            throw new IllegalArgumentException("This probably not be occuring.")
          case 1 =>{
            val index1 = relationIndices(relationNames.indexOf( head.accessors(0).name + "_" + head.accessors(0).attrs.mkString("_") ) )
            code.append(s"""const size_t count_${head.name} = Builder->build_aggregated_set(Iterator_${head.accessors(0).name}_${head.accessors(0).attrs.mkString("_")}->get_block(${index1}));""")
          }
          case 2 =>{
            val index1 = relationIndices(relationNames.indexOf( head.accessors(0).name + "_" + head.accessors(0).attrs.mkString("_") ) )
            val index2 = relationIndices(relationNames.indexOf(head.accessors(1).name + "_" + head.accessors(1).attrs.mkString("_")))
            code.append(s"""const size_t count_${head.name} = Builder->build_aggregated_set(Iterator_${head.accessors(0).name}_${head.accessors(0).attrs.mkString("_")}->get_block(${index1}),Iterator_${head.accessors(1).name}_${head.accessors(1).attrs.mkString("_")}->get_block(${index2}));""")
          }
          case 3 =>
            throw new IllegalArgumentException("3 arguments not supported yet.")
        } 
      }
    }
    code
  }

  def emitAllocateCode(head:QueryPlanNPRRInfo) : StringBuilder = {
    val code = new StringBuilder()
    (head.annotation,head.nextMaterialized) match {
      case (Some(annotation),None) =>
        code.append("Builder->allocate_annotation(tid);")
      case (None,Some(nextMaterialized)) =>
        code.append("Builder->allocate_next(tid);")
      case (Some(a),Some(b)) =>
        throw new IllegalArgumentException("Undefined behaviour.")
      case _ =>
    }
    code
  }

  def emitSetValues(head:QueryPlanNPRRInfo) : StringBuilder = {
    val code = new StringBuilder()
    (head.annotation,head.nextMaterialized) match {
      case (Some(annotation),None) =>
        code.append(s"""Builder->set_annotation(annotation_${annotation},${head.name}_i,${head.name}_d);""")
      case (None,Some(nextMaterialized)) =>
        code.append(s"""Builder->set_level(${head.name}_i,${head.name}_d);""")
      case (Some(a),Some(b)) =>
        throw new IllegalArgumentException("Undefined behaviour.")
      case _ =>
    }
    code
  }

  def emitForeach(head:QueryPlanNPRRInfo,iteratorAccessors:IteratorAccessors) : StringBuilder = {
    val code = new StringBuilder()
    head.materialize match {
      case true =>
        code.append(s"""Builder->foreach_builder([&](const uint32_t ${head.name}_i, const uint32_t ${head.name}_d) {""")
      case false =>
        code.append(s"""Builder->foreach_aggregate([&](const uint32_t ${head.name}_d) {""")
    }
    iteratorAccessors(head.name).foreach(i => {
      if(i._2 != (i._3-1)) //not for the last level
        code.append(s"""Iterator_${i._1}->get_next_block(${i._2},${head.name}_d);""")
    })
    code
  }

  def emitAnnotationAccessors(head:QueryPlanNPRRInfo,annotationType:String,iteratorAccessors:IteratorAccessors) : StringBuilder = {
    val code = new StringBuilder()
    val relationIndices = iteratorAccessors(head.name).map(_._2)
    val relationNames = iteratorAccessors(head.name).map(_._1)

    val joinType = "*" //fixme
    (head.aggregation) match {
      case Some(a) => {
        code.append(s"""const ${annotationType} intermediate_${head.name} = """)
        //starter that has no effect on join operation
        joinType match {
          case "*" => code.append(s"""(${annotationType})1""")
          case "+" => code.append(s"""(${annotationType})0""")
        }
        //now actually get annotated values (if they exist)
        head.accessors.foreach(acc => {
          if(acc.annotated){
            val index1 = relationIndices(relationNames.indexOf( acc.name + "_" + acc.attrs.mkString("_") ) )
            code.append(s"""${joinType} Iterator_${acc.name}_${acc.attrs.mkString("_")}->get_annotation(${index1},${head.name}_d)""")
          }
        })
        code.append(";")
      }
      case _ => //do nothing 
    }
    code
  }

  def emitInitializeAnnotation(head:QueryPlanNPRRInfo,annotationType:String) : StringBuilder = {
    val code = new StringBuilder()
    (head.aggregation) match {
      case Some(a) => {
        code.append(s"""${annotationType} annotation_${head.name} = (${annotationType})0;""")
      }
      case _ => //do nothing 
    }
    code
  }

  def nprrRecursiveCall(head:Option[QueryPlanNPRRInfo],tail:List[QueryPlanNPRRInfo],iteratorAccessors:IteratorAccessors,annotationType:String) : StringBuilder = {
    val code = new StringBuilder()
    (head,tail) match {
      case (Some(a),List()) => {
        code.append(emitBuildCode(a,iteratorAccessors))
        code.append(s"""num_rows_reducer.update(tid,count_${a.name});""")
        //init annotation
        code.append(emitInitializeAnnotation(a,annotationType))
        //emit compute annotation (might have to contain a foreach)
      }
      case (Some(a),_) => {
        //build 
        code.append(emitBuildCode(a,iteratorAccessors))
        //allocate
        code.append(emitAllocateCode(a))
        //init annotation
        code.append(emitInitializeAnnotation(a,annotationType))
        //foreach
        code.append(emitForeach(a,iteratorAccessors))
        //get annnotation accessors
        code.append(emitAnnotationAccessors(a,annotationType,iteratorAccessors))
        //recurse
        code.append(nprrRecursiveCall(tail.headOption,tail.tail,iteratorAccessors,annotationType))
        //set
        code.append(emitSetValues(a))
        //close out foreach
        code.append("});")
      }
      case (None,_) =>
        throw new IllegalArgumentException("Should not reach this state.")
    }
    return code
  }

  def emitNPRR(output:Boolean,bag:QueryPlanBagInfo,intermediateRelations:Map[String,List[Attribute]]) : (StringBuilder,List[Attribute]) = {
    val code = new StringBuilder()

    //Emit the output trie for the bag.
    output match {
      case false => {
        code.append(emitIntermediateTrie(bag.name,bag.annotation,bag.attributes.length))
      }
      case _ =>
    }

    code.append("{")
 
    val (parItCode,iteratorAccessors,encodings) = emitParallelIterators(bag.relations,intermediateRelations)
    code.append(emitParallelBuilder(bag.name,bag.attributes,bag.annotation,encodings))
    code.append(parItCode)
    code.append(emitHeadBuildCode(bag.nprr.headOption))

    println("ENCODINGS: " + encodings)
    println("ITERATOR ACCESSORS: " + iteratorAccessors)

    val outputAttributes:List[Attribute] = bag.attributes.map(a => encodings(a))
    val remainingAttrs = bag.nprr.tail
    if(remainingAttrs.length > 0){
      code.append(emitHeadAllocations(bag.nprr.head))
      code.append(emitInitHeadAnnotations(bag.nprr.head,bag.annotation))
      code.append(emitHeadParForeach(bag.nprr.head,bag.annotation,bag.relations,iteratorAccessors))
      code.append(emitAnnotationAccessors(bag.nprr.head,bag.annotation,iteratorAccessors))
      code.append(nprrRecursiveCall(remainingAttrs.headOption,remainingAttrs.tail,iteratorAccessors,bag.annotation))
      code.append(emitSetValues(bag.nprr.head))
      code.append("});")
      code.append(emitSetHeadAnnotations(bag.nprr.head,bag.annotation))
      code.append("Builders.trie->num_rows = num_rows_reducer.evaluate(0);")
    }
    code.append("}")
    
    return (code,outputAttributes)
  }
}
