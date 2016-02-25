package duncecap

import scala.util.parsing.json._
import scala.io._
import java.io.{FileWriter, File, BufferedWriter}
import sys.process._
import scala.collection.mutable

object EHGenerator { 
  type IteratorAccessors = Map[String,List[(String,Int,Int)]]
  type Encodings = Map[String,Attribute]
  type Attribute = String

  var db:DBInstance = null
  var memory:String = "ParMemoryBuffer"

  private def GHDFromJSON(filename:String):Map[String,Any] = {
    val fileContents = Source.fromFile(filename).getLines.mkString
    val ghd:Map[String,Any] = JSON.parseFull(fileContents) match {
      case Some(map: Map[String, Any]) => map
      case _ => Map()
    }
    return ghd
  }

  /*
  def detectTransitiveClosure(qp:QueryPlan,db:DBInstance) : Boolean = {
    //check encodings, check that base case is not annotated
    if(qp.ghd.length == 2 && qp.ghd.last.recursion.isDefined){
      val base_case = qp.ghd.head
      val encodings = 
      if(base_case.relations.length > 0 && encodings.length == 1){
        if(base_case.nprr.length == 2){
          if(base_case.nprr.head.selection.length == 1){
            if(qp.ghd.last.nprr.last.aggregation.get.operation == "MIN"){
              return true
            }
          }
        }
      }
    }
    return false
  }
  */
  
  def run(qp:QueryPlan,dbIn:DBInstance,id:String,filename:String) = {
    db = dbIn
    //get distinct relations we need to load
    //dump output at the end, rest just in a loop
    var outputAttributes:List[Attribute] = List()
    val intermediateRelations:mutable.Map[String,List[Attribute]] = mutable.Map()
    val distinctLoadRelations:mutable.Map[String,QueryPlanRelationInfo] = mutable.Map()

    val cppCode = new StringBuilder()
    val includeCode = new StringBuilder()

    //spit out output for each query in global vars
    val topDown = qp.topdown.length > 0      
    //cppCode.append(emitInitializeOutput(qp.output))
    //find all distinct relations
    val single_source_tc = false//detectTransitiveClosure(qp,db)
    qp.relations.foreach( r => {
      val loadTC = !single_source_tc || (r.ordering == (0 until r.ordering.length).toList)
      if(loadTC && !distinctLoadRelations.contains(s"""${r.name}_${r.ordering.mkString("_")}"""))
        distinctLoadRelations += ((s"""${r.name}_${r.ordering.mkString("_")}""" -> r))
    })
    cppCode.append(emitLoadRelations(distinctLoadRelations.map(e => e._2).toList))

    /*

    cppCode.append("par::reducer<size_t> num_rows_reducer(0,[](size_t a, size_t b) { return a + b; });")
    cppCode.append("\n//\n//query plan\n//\n")
    cppCode.append("{")
    cppCode.append("auto query_timer = timer::start_clock();")
    val topDown = qp.topdown.length > 0
    var i = 1
    val single_source_tc = detectTransitiveClosure(qp)
    if(!single_source_tc){
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
        val (bagCode,bagOutput) = emitTopDown(qp.output.name,qp.output.ordering,qp.output.annotation,qp.topdown,intermediateRelations.toMap)
        cppCode.append(bagCode)
        outputAttributes = bagOutput
      }
    } else{
      val base_case = qp.ghd.head
      val input = base_case.relations.head.name + "_" + base_case.relations.head.ordering.mkString("_")
      val output = qp.output.name + "_" + qp.output.ordering.mkString("_")
      val init = base_case.nprr.head.aggregation.get.init
      val source = base_case.nprr.head.selection.head.expression
      val expression = qp.ghd.last.nprr.last.aggregation.get.expression
      val encoding = Environment.config.schemas(base_case.relations.head.name).attributes.map(_.encoding).distinct.head
      outputAttributes = List(Environment.config.schemas(base_case.relations.head.name).attributes.head)

      //get encoding
      includeCode.append("""#include "TransitiveClosure.hpp" """)
      cppCode.append(s"""
        tc::unweighted_single_source<hybrid,ParMemoryBuffer,${qp.output.annotation}>(
          Encoding_${encoding}->value_to_key.at(${source}), // from encoding
          Encoding_${encoding}->num_distinct, //from encoding
          Trie_${input}, //input graph
          Trie_${output}, //output vector
          ${init},
          [&](${qp.output.annotation} a){return ${expression} a;});
        Trie_${output}->encodings.push_back((void*)Encoding_${encoding});
        """)
    }
    cppCode.append(emitEndQuery(qp.output))
    cppCode.append("}")

    val newSchema = ((qp.output.name -> Schema(outputAttributes,List(qp.output.ordering),qp.output.annotation)))
    val newRelation = ((qp.output.name+"_"+qp.output.ordering.mkString("_"))->"disk")
    Environment.config.schemas = Environment.config.schemas+newSchema
    Environment.config.relations = Environment.config.relations+newRelation
    Environment.config.resultName = qp.output.name
    Environment.config.resultOrdering = qp.output.ordering
    */
    val cpp = new StringBuilder()
    cpp.append(getCode(includeCode,cppCode,id))

    val cppFilepath = filename
    val file = new File(cppFilepath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(cpp.toString)
    bw.close()
    s"""clang-format -style=llvm -i ${cppFilepath}""" !
  } 

  private def getCode(includes:StringBuilder,run:StringBuilder,id:String) : String ={
    return s"""
      #include "utils/thread_pool.hpp"
      #include "utils/parallel.hpp"
      #include "Trie.hpp"
      #include "TrieBuilder.hpp"
      #include "TrieIterator.hpp"
      #include "utils/timer.hpp"
      #include "utils/ParMemoryBuffer.hpp"
      #include "Encoding.hpp"
      ${includes.toString}
      void run_${id}(){
        thread_pool::initializeThreadPool();
        ${run}
        thread_pool::deleteThreadPool();
      }
    """
  }

  /////////////////////////////////////////////////////////////////////////////
  //Loads the relations and encodings needed for a query.
  /////////////////////////////////////////////////////////////////////////////
  private def emitLoadRelations(relations:List[QueryPlanRelationInfo]) : StringBuilder = {
    val code = new StringBuilder()
    code.append(relations.map(r => {
      s"""Trie<${r.annotation},${memory}>* Trie_${r.name}_${r.ordering.mkString("_")} = NULL;
      {
        auto start_time = timer::start_clock();
        Trie_${r.name}_${r.ordering.mkString("_")} = Trie<${r.annotation},${memory}>::load( 
          "${db.folder}/relations/${r.name}/${r.name}_${r.ordering.mkString("_")}"
        );
        timer::stop_clock("LOADING Trie ${r.name}_${r.ordering.mkString("_")}", start_time);      
      }
      """
    }).reduce((a,b) => a+b))

    val encodings = relations.flatMap(r => {
      println(db.relationMap + " " + r.name)
      if(!db.relationMap.contains(r.name))
        throw new IllegalArgumentException("Schema not found.")
      else 
        db.relationMap(r.name).schema.attributeTypes
    }).distinct

    code.append(encodings.map(enc => {
      s"""
      auto e_loading_${enc} = timer::start_clock();
      Encoding<${enc}> *Encoding_${enc} = Encoding<${enc}>::from_binary(
          "${db.folder}/encodings/${enc}/");
      (void)Encoding_${enc};
      timer::stop_clock("LOADING ENCODINGS ${enc}", e_loading_${enc});
      """
    }).reduce((a,b) => a+b) )
    return code
  }
  /*
  /////////////////////////////////////////////////////////////////////////////
  //Initialize the output trie for the query.
  /////////////////////////////////////////////////////////////////////////////
  def emitInitializeOutput(output:QueryPlanOutputInfo) : StringBuilder = {
    val code = new StringBuilder()

    s"""mkdir -p ${db.folder}/relations/${output.name}""" !

    s"""mkdir -p ${db.folder}/relations/${output.name}/${output.name}_${output.ordering.mkString("_")}""" !

    val memFolder = if(db.config.memory == "ParMMapBuffer") "mmap"
      else "ram"

    s"""mkdir -p ${db.folder}/relations/${output.name}/${output.name}_${output.ordering.mkString("_")}/${memFolder}""" !

    code.append(s"""
      Trie<${output.annotation},${db.config.memory}> *Trie_${output.name}_${output.ordering.mkString("_")} = new Trie<${output.annotation},${db.config.memory}>("${db.folder}/relations/${output.name}/${output.name}_${output.ordering.mkString("_")}",${output.ordering.length},${output.annotation != "void*"});
    """)
    if(output.annotation != "void*" && output.ordering.length == 0)
      code.append(s"""${output.annotation} ${output.name};""")

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
    val ordering = (0 until num).toList.mkString("_")
    bagDuplicate match {
      case Some(bd) => {
        code.append(s"""Trie<${annotation},${db.config.memory}> *Trie_${name}_${ordering} = Trie_${bd}_${ordering};""")
        if(annotation != "void*" && num == 0)
          code.append(s"""${annotation} ${name};""")
      } case None => {
        code.append(s"""Trie<${annotation},${db.config.memory}> *Trie_${name}_${ordering} = new Trie<${annotation},${db.config.memory}>("${db.folder}/relations/${name}",${num},${annotation != "void*"});""")
      }
    }
    return code
  }

  def emitParallelBuilder(name:String,attributes:List[String],annotation:String,encodings:Encodings,numAttributes:Int) : StringBuilder = {
    val code = new StringBuilder()
    code.append(s"""ParTrieBuilder<${annotation},${db.config.memory}> Builders(Trie_${name},${numAttributes});""")
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
            encodings += ((i,enc(r.ordering(a.indexOf(i)))))
            dependers += ((i,(r.name + "_" + a.mkString("_"),a.indexOf(i),a.length)))
          })
          code.append(s"""ParTrieIterator<${r.annotation},${db.config.memory}> Iterators_${r.name}_${a.mkString("_")}(Trie_${name});""")
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
  
  def emitHeadBuildCode(head:QueryPlanAttrInfo) : StringBuilder = {
    val code = new StringBuilder()
    head.materialize match {
      case true => {
        head.accessors.length match {
          case 0 =>
            throw new IllegalArgumentException("This probably not be occuring.")
          case 1 =>
            code.append(s"""const size_t count_${head.name} = Builders.build_set(Iterators_${head.accessors(0).name}_${head.accessors(0).attrs.mkString("_")}.head);""")
          case 2 =>
            code.append(s"""const size_t count_${head.name} = Builders.build_set(Iterators_${head.accessors(0).name}_${head.accessors(0).attrs.mkString("_")}.head,Iterators_${head.accessors(1).name}_${head.accessors(1).attrs.mkString("_")}.head);""")
          case _ =>
           code.append(s"""std::vector<const TrieBlock<hybrid,${db.config.memory}>*> ${head.name}_sets;""")
            (0 until head.accessors.length).toList.foreach(i =>{
              code.append(s"""${head.name}_sets.push_back(Iterators_${head.accessors(i).name}_${head.accessors(i).attrs.mkString("_")}.head);""")
            })
            code.append(s"""const size_t count_${head.name} = Builders.build_set(&${head.name}_sets);""")
        } 
      }
      case false => {
        head.accessors.length match {
          case 0 =>
            throw new IllegalArgumentException("This probably not be occuring.")
          case 1 =>
            code.append(s"""const size_t count_${head.name} = Builders.build_aggregated_set(Iterators_${head.accessors(0).name}_${head.accessors(0).attrs.mkString("_")}.head);""")
          case 2 =>
            code.append(s"""const size_t count_${head.name} = Builders.build_aggregated_set(Iterators_${head.accessors(0).name}_${head.accessors(0).attrs.mkString("_")}.head,Iterators_${head.accessors(1).name}_${head.accessors(1).attrs.mkString("_")}.head);""")
          case _ =>
            code.append(s"""std::vector<const TrieBlock<hybrid,${db.config.memory}>*> ${head.name}_sets;""")
            (0 until head.accessors.length).toList.foreach(i =>{
              code.append(s"""${head.name}_sets.push_back(Iterators_${head.accessors(i).name}_${head.accessors(i).attrs.mkString("_")}.head);""")
            })
            code.append(s"""const size_t count_${head.name} = Builders.build_aggregated_set(&${head.name}_sets);""")
        } 
      }
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
          case "CONST" =>
            code.append(s"""${annotationType} annotation_${head.name} = (${annotationType})0;""")
          case _ =>
            throw new IllegalArgumentException("AGGREGATION NOT YET SUPPORTED.")
        }
      case _ =>
    }
    code
  }

  def emitSetHeadAnnotations(outputName:String,head:QueryPlanAttrInfo,annotationType:String) : StringBuilder = {
    val code = new StringBuilder()
    (head.aggregation,head.materialize) match {
      case (Some(a),false) =>
        code.append(s"""Builders.trie->annotation = annotation_${head.name}.evaluate(0);
          ${outputName} = Builders.trie->annotation;
          """)
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

  def emitContainsSelection(
    name:String,
    materialize:Boolean,
    selections:QueryPlanSelection,
    accessor:QueryPlanAccessor) : StringBuilder = {
    val accname = accessor.name + "_" + accessor.attrs.mkString("_")
    val code = new StringBuilder()
    code.append(s"""Iterators_${accname}.get_next_block(selection_${name}_0);""")
    code
  }

  def emitSelectionValues(head:List[QueryPlanAttrInfo],encodings:Map[String,Attribute]) : StringBuilder = {
    val code = new StringBuilder()
    head.foreach(attr => {
      attr.selection.foreach(s => {
        code.append(s"""const uint32_t selection_${attr.name}_${attr.selection.indexOf(s)} = 
          Encoding_${encodings(attr.name).encoding}->value_to_key.at(${s.expression});""")
      })
    })
    code
  }
  def emitHeadContainsSelections(head:List[QueryPlanAttrInfo]) : (StringBuilder,List[QueryPlanAttrInfo]) = {
    val code = new StringBuilder()
    if(head.length == 0)
      (code,head)
    val cur = head.head
    val containsSelection = (cur.accessors.length == 1) && (cur.selection.length == 1) && !cur.materialize && cur.selection.head.operation == "="
    if(containsSelection){
      code.append(emitContainsSelection(cur.name,cur.materialize,cur.selection.head,cur.accessors.head))
      val (newcode,newhead) = emitHeadContainsSelections(head.tail)
      code.append(newcode)
      (code,newhead)
    } else {
      (code,head)
    }
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
    code.append(s"""TrieBuilder<${outputAnnotation},${db.config.memory}>* Builder = Builders.builders.at(tid);""")
    relations.foreach(r => {
      val name = r.name + "_" + r.ordering.mkString("_")
      r.attributes.foreach(attr => {
        attr.foreach(a => {
          code.append(s"""TrieIterator<${r.annotation},${db.config.memory}>* Iterator_${r.name}_${a.mkString("_")} = Iterators_${r.name}_${a.mkString("_")}.iterators.at(tid);""")
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
          case _ =>
            code.append(s"""std::vector<const TrieBlock<hybrid,${db.config.memory}>*> ${head.name}_sets;""")
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
          case _ =>
            code.append(s"""std::vector<const TrieBlock<hybrid,${db.config.memory}>*> ${head.name}_sets;""")
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
        if(a.operation != "CONST"){
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
      }
      case _ => //do nothing 
    }
    code
  }

  def emitFinalAnnotation(head:QueryPlanAttrInfo,annotationType:String,iteratorAccessors:IteratorAccessors,isHead:Boolean) : StringBuilder = {
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
        code.append(emitAnnotationComputation(head,annotationType,iteratorAccessors,isHead))
        if(loopOverSet){
          code.append("});")
        }
      } 
      case _ => 
    }
    code
  }
  def emitAnnotationComputation(head:QueryPlanAttrInfo,annotationType:String,iteratorAccessors:IteratorAccessors,isHead:Boolean) : StringBuilder = {
    val code = new StringBuilder()
    (head.aggregation,head.materialize) match { //you always check for annotations
      case (Some(a),false) => {
        val rhs = a.next match {
          case Some(n) =>
            s"""annotation_${n}"""
          case _ =>
            s"""intermediate_${head.name}"""
        }
        (a.operation,isHead) match {
          case ("SUM",false) => code.append(s"""annotation_${head.name} += ${rhs};""")
          case ("SUM",true) => code.append(s"""annotation_${head.name}.update(0,${rhs});""")
          case ("CONST",_) => //do nothing
          case _ => throw new IllegalArgumentException("OPERATION NOT YET SUPPORTED")
        } 
      } 
      case _ => 
    }
    code
  }
  def emitAnnotationExpression(head:QueryPlanAttrInfo,annotationType:String) : StringBuilder = {
    val code = new StringBuilder()
    head.aggregation match {
      case Some(a) => {
        a.operation match {
          case "CONST" => {
            if(a.expression != "")
              code.append(s"""annotation_${head.name} = ${a.expression} ${a.init};""")
            else 
              throw new IllegalArgumentException("CONST annotation must have expression")
          }
          case _ => {
            if(a.expression != "")
              code.append(s"""annotation_${head.name} = (${a.expression} annotation_${head.name});""")
          }
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
        if(a.materialize)
          code.append(s"""num_rows_reducer.update(tid,count_${a.name});""")
        else 
          code.append(s"""num_rows_reducer.update(tid,1);""")

        //init annotation
        code.append(emitInitializeAnnotation(a,annotationType))
        //emit compute annotation (might have to contain a foreach)
        code.append(emitFinalAnnotation(a,annotationType,iteratorAccessors,false))
        //the expression for the aggregation
        code.append(emitAnnotationExpression(a,annotationType))
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
        code.append(emitAnnotationComputation(a,annotationType,iteratorAccessors,false))
        //set
        code.append(emitSetValues(a))
        //close out foreach
        code.append("});")
        //emit annotation expression
        code.append(emitAnnotationExpression(a,annotationType))
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
    if(iterators.length == 1 && attrs.length == 1){
      code.append(s"""const size_t count_${attrInfo.name} = Builder->build_set(tid,Iterator_${iteratorName}->get_block(${iteratorLevels(iteratorName)}));""")
      code.append(s"""num_rows_reducer.update(tid,count_${attrInfo.name});""")
    } else {
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
    }

    code.append(emitTopDownIterators(iterators,attrs.tail,iteratorLevels))
    if(!(iterators.length == 1 && attrs.length == 1)){
      code.append(s"""Builder->set_level(${attrInfo.name}_i,${attrInfo.name}_d);""")
      code.append("});")
    }

    return code
  }

  def emitTopDown(
    output:String,
    ordering:List[Int],
    annotation:String,
    td_inter:List[TopDownPassIterator],
    intermediateRelations:Map[String,List[Attribute]]) 
      : (StringBuilder,List[Attribute]) = {
    
    val td = td_inter.filter(_.attributeInfo.length != 0) //fixme
    val code = new StringBuilder()
    
    code.append("{")
    code.append("auto bag_timer = timer::start_clock();")
    code.append("num_rows_reducer.clear();")

    //emit iterators for top down pass (build encodings)
    val eBuffers = mutable.ListBuffer[(String,Attribute)]()
    val iteratorLevels = mutable.Map[String,Int]()
    td_inter.foreach(tdi => {
      iteratorLevels += ((tdi.iterator -> 0))
      val ordering = (0 until intermediateRelations(tdi.iterator).length).toList.mkString("_")
      code.append(s"""ParTrieIterator<${annotation},${db.config.memory}> Iterators_${tdi.iterator}(Trie_${tdi.iterator}_${ordering});""")
      tdi.attributeInfo.foreach(ai => {
        val acc = ai.accessors.head
        val index = acc.attrs.indexOf(ai.name)
        eBuffers += ((ai.name,intermediateRelations(acc.name)(index)))
      })
    })
    val encodings = eBuffers.toList.groupBy(eb => eb._1).map(eb => (eb._1 -> eb._2.head._2))
    val attrs = eBuffers.toList.map(eb => eb._1)
    val outputAttributes = attrs.map(encodings(_))
    //emit builder
    code.append(emitParallelBuilder(output+"_"+ordering.mkString("_"),attrs,annotation,encodings,attrs.length))

    val firstIterator = td.head
    val firstAttr = firstIterator.attributeInfo.head
    code.append(s"""Builders.build_set(Iterators_${firstIterator.iterator}.head);""")
    code.append("Builders.allocate_next();")
    code.append(s"""Builders.par_foreach_builder([&](const size_t tid, const uint32_t ${firstAttr.name}_i, const uint32_t ${firstAttr.name}_d) {""")
    //get iterator and builder for each thread
    code.append(s"""TrieBuilder<${annotation},${db.config.memory}>* Builder = Builders.builders.at(tid);""")
    td_inter.foreach(tdi => {
      val name = tdi.iterator
      code.append(s"""TrieIterator<${annotation},${db.config.memory}>* Iterator_${name} = Iterators_${name}.iterators.at(tid);""")
    })

    firstAttr.accessors.foreach(acc => {
      val index = acc.attrs.indexOf(firstAttr.name)
      iteratorLevels(acc.name) += 1
      if(index != (acc.attrs.length-1)){
        if(firstIterator.iterator == acc.name){
          code.append(s"""Iterator_${acc.name}->get_next_block(${index},${firstAttr.name}_i,${firstAttr.name}_d);""")
        } else {
          code.append(s"""Iterator_${acc.name}->get_next_block(${index},${firstAttr.name}_d);""")
        }
      }
    })

    code.append(emitTopDownIterators(td,firstIterator.attributeInfo.tail,iteratorLevels))
    code.append(s"""Builder->set_level(${firstAttr.name}_i,${firstAttr.name}_d);""")
    code.append("});")
    code.append("Builders.trie->num_rows = num_rows_reducer.evaluate(0);")
    code.append(s"""timer::stop_clock("TOP DOWN TIME", bag_timer);""")
    code.append("}")

    (code,outputAttributes)
  }

  def emitNPRR(
    output:Option[String],
    bag:QueryPlanBagInfo,
    intermediateRelations:Map[String,List[Attribute]],
    outputAttributes:List[Attribute]) : (StringBuilder,List[Attribute]) = {
    
    val code = new StringBuilder()
    var oa = outputAttributes
    //Emit the output trie for the bag.
    val outputName = output match {
      case None => {
        code.append(emitIntermediateTrie(bag.name,bag.annotation,bag.attributes.length,bag.duplicateOf))
        bag.name
      } case Some(s) => {
        code.append(emitIntermediateTrie(bag.name,bag.annotation,bag.attributes.length,output))
        s
      }
    }

    bag.duplicateOf match {
      case None => {
        code.append("{")
        
        bag.recursion match {
          case Some(rec) => {
            if(rec.criteria == "iterations"){
              code.append(s""" 
                size_t num_iterations = 0;
                while(num_iterations < ${rec.converganceValue}){
                """)
            }else 
              throw new IllegalArgumentException("CONVERGANCE CRITERIA NOT SUPPORTED")
            code.append(emitIntermediateTrie(bag.name,bag.annotation,bag.attributes.length,bag.duplicateOf))
          }
          case _ =>
        }
        code.append("auto bag_timer = timer::start_clock();")
        code.append("num_rows_reducer.clear();")
        val (parItCode,iteratorAccessors,encodings) = emitParallelIterators(bag.relations,intermediateRelations)
        val pbname = bag.name+"_"+(0 until bag.attributes.length).toList.mkString("_")
        code.append(emitParallelBuilder(pbname,bag.attributes,bag.annotation,encodings,bag.nprr.length))
        code.append(parItCode)

        oa = bag.attributes.map(a => encodings(a))

        if(bag.nprr.length > 0){
          code.append(emitSelectionValues(bag.nprr,encodings))
          val (hsCode,remainingAttrs) = emitHeadContainsSelections(bag.nprr)
          code.append(hsCode)
          if(remainingAttrs.length > 0){
            code.append(emitHeadBuildCode(remainingAttrs.head))
            code.append(emitHeadAllocations(remainingAttrs.head))
            code.append(emitInitHeadAnnotations(remainingAttrs.head,bag.annotation))
            if(remainingAttrs.length > 1){  
              code.append(emitHeadParForeach(remainingAttrs.head,bag.annotation,bag.relations,iteratorAccessors))
              code.append(emitAnnotationAccessors(remainingAttrs.head,bag.annotation,iteratorAccessors))
              code.append(nprrRecursiveCall(remainingAttrs.tail.headOption,remainingAttrs.tail.tail,iteratorAccessors,bag.annotation))
              code.append(emitSetValues(remainingAttrs.head))
              code.append(emitParallelAnnotationComputation(remainingAttrs.head))
              code.append("});")
            } else {
              code.append(s"""num_rows_reducer.update(0,count_${remainingAttrs.head.name});""")
              //code.append(emitInitializeAnnotation(remainingAttrs.head,bag.annotation))
              //emit compute annotation (might have to contain a foreach)
              code.append(emitFinalAnnotation(remainingAttrs.head,bag.annotation,iteratorAccessors,true))
            }
            code.append(emitSetHeadAnnotations(outputName,remainingAttrs.head,bag.annotation))
          }
          code.append("Builders.trie->num_rows = num_rows_reducer.evaluate(0);")
        }
        code.append(s"""std::cout << "NUM ROWS: " <<  Builders.trie->num_rows << " ANNOTATION: " << Builders.trie->annotation << std::endl;""")
        code.append(s"""timer::stop_clock("BAG ${bag.name} TIME", bag_timer);""")
        
        //copy the buffers, needed if recursive
        val recordering = (0 until bag.attributes.length).toList.mkString("_")
        output match {
          case None => 
          case Some(s) => {
            code.append(s"""Trie_${s}_${recordering}->memoryBuffers = Builders.trie->memoryBuffers;""")
            code.append(s"""Trie_${s}_${recordering}->num_rows = Builders.trie->num_rows;""")
            code.append(s"""Trie_${s}_${recordering}->encodings = Builders.trie->encodings;""")
          }
        }

        bag.recursion match {
          case Some(rec) => 
            code.append(s""" 
              Trie_${rec.input}_${recordering} = Builders.trie;
              num_iterations++;
              }
              """)
          case _ =>
        }

        code.append("}")
      }
      case _ =>
    }
    return (code,oa)
  }
  */
}