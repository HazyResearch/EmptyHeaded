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
    val topDown = qp.topdown.length > 0
    cppCode.append(emitInitializeOutput(qp.output))
    var i = 1
    qp.ghd.foreach(bag => {
      val outputName = 
        if((i == qp.ghd.length) && (!topDown))
          Some(qp.output.name)
        else 
          None
      val (bagCode,bagOutput) = emitNPRR(outputName,bag,intermediateRelations.toMap,outputAttributes)
      intermediateRelations += ((bag.name -> bagOutput))
      outputAttributes = bagOutput
      cppCode.append(bagCode)
      i += 1
    })
    if(topDown){
      val (bagCode,bagOutput) = emitTopDown(qp.output.name,qp.output.annotation,qp.topdown,intermediateRelations.toMap)
      cppCode.append(bagCode)
      outputAttributes = bagOutput
    }
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
  def emitIntermediateTrie(name:String,annotation:String,num:Int,bagDuplicate:Option[String]) : StringBuilder = {
    val code = new StringBuilder()
    assert(Environment.config.memory != "ParMMapBuffer")
    val ordering = (0 until num).toList.mkString("_")

    bagDuplicate match {
      case Some(bd) => {
        code.append(s"""Trie<${annotation},${Environment.config.memory}> *Trie_${name}_${ordering} = Trie_${bd}_${ordering};""")
      } case None => {
        code.append(s"""Trie<${annotation},${Environment.config.memory}> *Trie_${name}_${ordering} = new Trie<${annotation},${Environment.config.memory}>("${Environment.config.database}/relations/${name}",${num},${annotation != "void*"});""")
      }
    }
    return code
  }

  def emitParallelBuilder(name:String,attributes:List[String],annotation:String,encodings:Encodings,numAttributes:Int) : StringBuilder = {
    val code = new StringBuilder()
    val ordering = (0 until attributes.length).toList.mkString("_")
    code.append(s"""ParTrieBuilder<${annotation},${Environment.config.memory}> Builders(Trie_${name}_${ordering},${numAttributes});""")
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
  
  def emitHeadBuildCode(head:Option[QueryPlanAttrInfo]) : StringBuilder = {
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
               code.append(s"""std::vector<const TrieBlock<hybrid,${Environment.config.memory}>*> ${h.name}_sets;""")
                (0 until h.accessors.length).toList.foreach(i =>{
                  code.append(s"""${h.name}_sets.push_back(Iterators_${h.accessors(i).name}_${h.accessors(i).attrs.mkString("_")}.head);""")
                })
                code.append(s"""Builders.build_set(&${h.name}_sets);""")
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
                code.append(s"""std::vector<const TrieBlock<hybrid,${Environment.config.memory}>*> ${h.name}_sets;""")
                (0 until h.accessors.length).toList.foreach(i =>{
                  code.append(s"""${h.name}_sets.push_back(Iterators_${h.accessors(i).name}_${h.accessors(i).attrs.mkString("_")}.head);""")
                })
                code.append(s"""Builders.build_aggregated_set(&${h.name}_sets);""")
            } 
          }
        } 
      }
      case _ =>
       throw new IllegalArgumentException("This probably not be occuring.")
    }
    return code
  }

  def emitInitHeadAnnotations(head:QueryPlanAttrInfo,annotationType:String) : StringBuilder = {
    val code = new StringBuilder()
    (head.aggregation,head.materialize) match {
      case (Some(a),false) =>
        a.operation match {
          case "SUM" =>
            code.append(s"""par::reducer<${annotationType}> annotation_${head.name}(0,[&](${annotationType} a, ${annotationType} b) { return a + b; });""")
          case _ =>
            throw new IllegalArgumentException("AGGREGATION NOT YET SUPPORTED.")
        }
      case _ =>
    }
    code
  }

  def emitSetHeadAnnotations(head:QueryPlanAttrInfo,annotationType:String) : StringBuilder = {
    val code = new StringBuilder()
    (head.aggregation,head.materialize) match {
      case (Some(a),false) =>
        code.append(s"""Builders.trie->annotation = annotation_${head.name}.evaluate(0);""")
      case _ =>
    }
    code
  }

  def emitHeadAllocations(head:QueryPlanAttrInfo) : StringBuilder = {
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

  def emitHeadParForeach(head:QueryPlanAttrInfo,outputAnnotation:String,relations:List[QueryPlanRelationInfo],iteratorAccessors:IteratorAccessors) : StringBuilder = {
    val code = new StringBuilder()

    head.materialize match {
      case true =>
        code.append(s"""Builders.par_foreach_builder([&](const size_t tid, const uint32_t ${head.name}_i, const uint32_t ${head.name}_d) {""")
      case false =>
        code.append(s"""Builders.par_foreach_aggregate([&](const size_t tid, const uint32_t ${head.name}_d) {""")
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

  def emitBuildCode(head:QueryPlanAttrInfo,iteratorAccessors:IteratorAccessors) : StringBuilder = {
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
            code.append(s"""std::vector<const TrieBlock<hybrid,${Environment.config.memory}>*> ${head.name}_sets;""")
            (0 until head.accessors.length).toList.foreach(i =>{
              val index1 = relationIndices(relationNames.indexOf( head.accessors(i).name + "_" + head.accessors(i).attrs.mkString("_") ) )
              code.append(s"""${head.name}_sets.push_back(Iterator_${head.accessors(i).name}_${head.accessors(i).attrs.mkString("_")}->get_block(${index1}));""")
            })
            code.append(s"""const size_t count_${head.name} = Builder->build_set(tid,&${head.name}_sets);""")
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
            code.append(s"""std::vector<const TrieBlock<hybrid,${Environment.config.memory}>*> ${head.name}_sets;""")
            (0 until head.accessors.length).toList.foreach(i =>{
              val index1 = relationIndices(relationNames.indexOf( head.accessors(i).name + "_" + head.accessors(i).attrs.mkString("_") ) )
              code.append(s"""${head.name}_sets.push_back(Iterator_${head.accessors(i).name}_${head.accessors(i).attrs.mkString("_")}->get_block(${index1}));""")
            })
            code.append(s"""const size_t count_${head.name} = Builder->build_aggregated_set(&${head.name}_sets);""")
        } 
      }
    }
    code
  }

  def emitAllocateCode(head:QueryPlanAttrInfo) : StringBuilder = {
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

  def emitSetValues(head:QueryPlanAttrInfo) : StringBuilder = {
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

  def emitForeach(head:QueryPlanAttrInfo,iteratorAccessors:IteratorAccessors) : StringBuilder = {
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

  def emitAnnotationAccessors(head:QueryPlanAttrInfo,annotationType:String,iteratorAccessors:IteratorAccessors,extra:String="") : StringBuilder = {
    val code = new StringBuilder()
    val relationIndices = iteratorAccessors(head.name).map(_._2)
    val relationNames = iteratorAccessors(head.name).map(_._1)

    val joinType = "*" //fixme
    (head.aggregation,head.materialize) match { //you always check for annotations
      case (Some(a),false) => {
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
          } else if(head.name == acc.attrs.last) {
            code.append(s"""${joinType} ${a.init}""")
          }
        })
        a.prev match {
          case Some(p) =>
            code.append(s"""${joinType} intermediate_${p}""")
          case _ =>
        }
        code.append(s"""${extra};""")
      }
      case _ => //do nothing 
    }
    code
  }

  def emitFinalAnnotation(head:QueryPlanAttrInfo,annotationType:String,iteratorAccessors:IteratorAccessors) : StringBuilder = {
    val code = new StringBuilder()
    val joinType = "*" //fixme
    (head.aggregation,head.materialize) match { //you always check for annotations
      case (Some(a),false) => {
        val loopOverSet = head.accessors.map(_.annotated).reduce((a,b) => {a || b})
        val extra = if(loopOverSet){
          code.append(emitForeach(head,iteratorAccessors))
          ""
        } else {
          s"""${joinType} count_${head.name}"""
        }
        code.append(emitAnnotationAccessors(head,annotationType,iteratorAccessors,extra))
        code.append(emitAnnotationComputation(head,annotationType,iteratorAccessors))
        if(loopOverSet){
          code.append("});")
        }
      } 
      case _ => 
    }
    code
  }
  def emitAnnotationComputation(head:QueryPlanAttrInfo,annotationType:String,iteratorAccessors:IteratorAccessors) : StringBuilder = {
    val code = new StringBuilder()
    (head.aggregation,head.materialize) match { //you always check for annotations
      case (Some(a),false) => {
        val rhs = a.next match {
          case Some(n) =>
            s"""annotation_${n}"""
          case _ =>
            s"""intermediate_${head.name}"""
        }
        a.operation match {
          case "SUM" => code.append(s"""annotation_${head.name} += ${rhs};""")
          case _ => throw new IllegalArgumentException("OPERATION NOT YET SUPPORTED")
        } 
      } 
      case _ => 
    }
    code
  }

  def emitParallelAnnotationComputation(head:QueryPlanAttrInfo) : StringBuilder = {
    val code = new StringBuilder()
    (head.aggregation,head.materialize) match { //you always check for annotations
      case (Some(a),false) => {
        val rhs = a.next match {
          case Some(n) =>
            s"""annotation_${n}"""
          case _ =>
            s"""intermediate_${head.name}"""
        }
        code.append(s"""annotation_${head.name}.update(tid,${rhs});""")
      } 
      case _ => 
    }
    code
  }

  def emitInitializeAnnotation(head:QueryPlanAttrInfo,annotationType:String) : StringBuilder = {
    val code = new StringBuilder()
    (head.aggregation) match {
      case Some(a) => {
        code.append(s"""${annotationType} annotation_${head.name} = (${annotationType})0;""")
      }
      case _ => //do nothing 
    }
    code
  }

  def nprrRecursiveCall(head:Option[QueryPlanAttrInfo],tail:List[QueryPlanAttrInfo],iteratorAccessors:IteratorAccessors,annotationType:String) : StringBuilder = {
    val code = new StringBuilder()
    (head,tail) match {
      case (Some(a),List()) => {
        code.append(emitBuildCode(a,iteratorAccessors))
        code.append(s"""num_rows_reducer.update(tid,count_${a.name});""")
        //init annotation
        code.append(emitInitializeAnnotation(a,annotationType))
        //emit compute annotation (might have to contain a foreach)
        code.append(emitFinalAnnotation(a,annotationType,iteratorAccessors))
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
        //the actual aggregation
        code.append(emitAnnotationComputation(a,annotationType,iteratorAccessors))
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

  def emitTopDownIterators(iterators:List[TopDownPassIterator],attrs:List[QueryPlanAttrInfo],iteratorLevels:mutable.Map[String,Int]) : StringBuilder = {
    val code = new StringBuilder()
    if(iterators.length == 0)
      return code
    code.append(emitTopDownAttributes(iterators,attrs,iteratorLevels))
    return code
  }

  def emitTopDownAttributes(iterators:List[TopDownPassIterator],attrs:List[QueryPlanAttrInfo],iteratorLevels:mutable.Map[String,Int]) : StringBuilder = {
    val code = new StringBuilder()
    if(attrs.length == 0 && iterators.length == 1) //done quit
      return code.append(emitTopDownIterators(List(),List(),iteratorLevels))
    else if(attrs.length == 0) //done with cur iterator
      return code.append(emitTopDownIterators(iterators.tail,iterators.tail.head.attributeInfo,iteratorLevels))

    val attrInfo = attrs.head
    val iteratorName = iterators.head.iterator
    if(iterators.length != 1){
      code.append(s"""Builder->build_set(tid,Iterator_${iteratorName}->get_block(${iteratorLevels(iteratorName)}));""")
      code.append("Builder->allocate_next(tid);")
      code.append(s"""Builder->foreach_builder([&](const uint32_t ${attrInfo.name}_i, const uint32_t ${attrInfo.name}_d) {""")
      attrInfo.accessors.foreach(acc => {
        iteratorLevels(acc.name) += 1
        val index = acc.attrs.indexOf(attrInfo.name)
        if(index != (acc.attrs.length-1)){
          if(iteratorName == acc.name){
            code.append(s"""Iterator_${acc.name}->get_next_block(${index},${attrInfo.name}_i,${attrInfo.name}_d);""")
          } else {
            code.append(s"""Iterator_${acc.name}->get_next_block(${index},${attrInfo.name}_d);""")
          }
        }
      })  
    } else {
      code.append(s"""const size_t count_${attrInfo.name} = Builder->build_set(tid,Iterator_${iteratorName}->get_block(${iteratorLevels(iteratorName)}));""")
      code.append(s"""num_rows_reducer.update(tid,count_${attrInfo.name});""")
    }

    code.append(emitTopDownIterators(iterators,attrs.tail,iteratorLevels))
    if(iterators.length != 1){
      code.append(s"""Builder->set_level(${attrInfo.name}_i,${attrInfo.name}_d);""")
      code.append("});")
    }

    return code
  }

  def emitTopDown(
    output:String,
    annotation:String,
    td:List[TopDownPassIterator],
    intermediateRelations:Map[String,List[Attribute]]) 
      : (StringBuilder,List[Attribute]) = {
    
    //val outputAttributes = bag.attributeInfo.map(a => encodings(a.name))
    val code = new StringBuilder()
    println("EMIT TOP DOWN: " + intermediateRelations)
    
    code.append("{")
    code.append("auto bag_timer = timer::start_clock();")
    code.append("num_rows_reducer.clear();")

    //emit iterators for top down pass (build encodings)
    val eBuffers = mutable.ListBuffer[(String,Attribute)]()
    val iteratorLevels = mutable.Map[String,Int]()
    td.foreach(tdi => {
      iteratorLevels += ((tdi.iterator -> 0))
      val ordering = (0 until intermediateRelations(tdi.iterator).length).toList.mkString("_")
      code.append(s"""const ParTrieIterator<${annotation},${Environment.config.memory}> Iterators_${tdi.iterator}(Trie_${tdi.iterator}_${ordering});""")
      tdi.attributeInfo.foreach(ai => {
        val acc = ai.accessors.head
        val index = acc.attrs.indexOf(ai.name)
        eBuffers += ((ai.name,intermediateRelations(acc.name)(index)))
      })
    })
    val encodings = eBuffers.toList.groupBy(eb => eb._1).map(eb => (eb._1 -> eb._2.head._2))
    val attrs = eBuffers.toList.map(eb => eb._1)

    //emit builder
    code.append(emitParallelBuilder(output,attrs,annotation,encodings,attrs.length))

    val firstIterator = td.head
    val firstAttr = firstIterator.attributeInfo.head
    code.append(s"""Builders.build_set(Iterators_${firstIterator.iterator}.head);""")
    code.append("Builders.allocate_next();")
    code.append(s"""Builders.par_foreach_builder([&](const size_t tid, const uint32_t ${firstAttr.name}_i, const uint32_t ${firstAttr.name}_d) {""")
    //get iterator and builder for each thread
    code.append(s"""TrieBuilder<${annotation},${Environment.config.memory}>* Builder = Builders.builders.at(tid);""")
    td.foreach(tdi => {
      val name = tdi.iterator
      code.append(s"""TrieIterator<${annotation},${Environment.config.memory}>* Iterator_${name} = Iterators_${name}.iterators.at(tid);""")
    })

    firstAttr.accessors.foreach(acc => {
      val index = acc.attrs.indexOf(firstAttr.name)
      iteratorLevels(acc.name) += 1
      if(firstIterator.iterator == acc.name){
        code.append(s"""Iterator_${acc.name}->get_next_block(${index},${firstAttr.name}_i,${firstAttr.name}_d);""")
      } else {
        code.append(s"""Iterator_${acc.name}->get_next_block(${index},${firstAttr.name}_d);""")
      }
    })

    code.append(emitTopDownIterators(td,firstIterator.attributeInfo.tail,iteratorLevels))
    code.append(s"""Builder->set_level(${firstAttr.name}_i,${firstAttr.name}_d);""")
    code.append("});")
    code.append("Builders.trie->num_rows = num_rows_reducer.evaluate(0);")
    code.append(s"""timer::stop_clock("TOP DOWN TIME", bag_timer);""")
    code.append("}")

    (code,List())
  }

  def emitNPRR(
    output:Option[String],
    bag:QueryPlanBagInfo,
    intermediateRelations:Map[String,List[Attribute]],
    outputAttributes:List[Attribute]) : (StringBuilder,List[Attribute]) = {
    
    val code = new StringBuilder()
    var oa = outputAttributes
    //Emit the output trie for the bag.
    output match {
      case None => 
        code.append(emitIntermediateTrie(bag.name,bag.annotation,bag.attributes.length,bag.duplicateOf))
      case _ =>
        code.append(emitIntermediateTrie(bag.name,bag.annotation,bag.attributes.length,output))
    }

    bag.duplicateOf match {
      case None => {
        code.append("{")
        code.append("auto bag_timer = timer::start_clock();")
        code.append("num_rows_reducer.clear();")
        val (parItCode,iteratorAccessors,encodings) = emitParallelIterators(bag.relations,intermediateRelations)
        code.append(emitParallelBuilder(bag.name,bag.attributes,bag.annotation,encodings,bag.nprr.length))
        code.append(parItCode)
        code.append(emitHeadBuildCode(bag.nprr.headOption))

        println("ENCODINGS: " + encodings)
        println("ITERATOR ACCESSORS: " + iteratorAccessors)

        val remainingAttrs = bag.nprr.tail
        oa = bag.attributes.map(a => encodings(a))

        if(remainingAttrs.length > 0){
          code.append(emitHeadAllocations(bag.nprr.head))
          code.append(emitInitHeadAnnotations(bag.nprr.head,bag.annotation))
          code.append(emitHeadParForeach(bag.nprr.head,bag.annotation,bag.relations,iteratorAccessors))
          code.append(emitAnnotationAccessors(bag.nprr.head,bag.annotation,iteratorAccessors))
          code.append(nprrRecursiveCall(remainingAttrs.headOption,remainingAttrs.tail,iteratorAccessors,bag.annotation))
          code.append(emitSetValues(bag.nprr.head))
          code.append(emitParallelAnnotationComputation(bag.nprr.head))
          code.append("});")
          code.append(emitSetHeadAnnotations(bag.nprr.head,bag.annotation))
          code.append("Builders.trie->num_rows = num_rows_reducer.evaluate(0);")
        }
        code.append(s"""std::cout << "NUM ROWS: " <<  Builders.trie->num_rows << " ANNOTATION: " << Builders.trie->annotation << std::endl;""")
        code.append(s"""timer::stop_clock("BAG ${bag.name} TIME", bag_timer);""")
        code.append("}")
      }
      case _ =>
    }
    return (code,oa)
  }
}
