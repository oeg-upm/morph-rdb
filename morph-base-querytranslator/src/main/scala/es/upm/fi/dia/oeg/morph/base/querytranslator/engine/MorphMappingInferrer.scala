package es.upm.fi.dia.oeg.morph.base.querytranslator.engine

import scala.collection.JavaConversions._
import org.apache.jena.graph.Node
import org.apache.jena.query.Query
import org.apache.jena.sparql.algebra.Algebra
import org.apache.jena.sparql.algebra.Op
import org.apache.jena.vocabulary.RDF
import org.apache.jena.sparql.algebra.op._
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.expr.ExprList
import org.apache.jena.sparql.expr.Expr
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import org.slf4j.LoggerFactory

class MorphMappingInferrer(mappingDocument:MorphBaseMappingDocument ) {
	val logger = LoggerFactory.getLogger(this.getClass());
	val mapInferredTypes : Map[Node, Set[MorphBaseClassMapping]] = Map.empty
	//var query:Query  = null;

	def addToInferredTypes(mapNodeTypes:Map[Node, Set[MorphBaseClassMapping]]
												 , node:Node , cms:Set[MorphBaseClassMapping] )
	: Map[Node, Set[MorphBaseClassMapping]] = {
		val newVal = mapNodeTypes.get(node).map(_.intersect(cms)).getOrElse(cms)
		mapNodeTypes + (node -> newVal)
	}

	//	def infer2(query:Query) : java.util.Map[Node, java.util.Set[MorphBaseClassMapping]] = {
	//		val resultAux = this.infer(query);
	//		val result = resultAux.keys.flatMap(key => {
	//		  val valueInScala = resultAux(key);
	//		  val valueInJava = valueInScala.asJava;
	//		  Some((key -> valueInJava))
	//		})
	//
	//		val result2 = result.toMap;
	//		result2
	//	}

	//	def infer() : Map[Node, Set[MorphBaseClassMapping]] = {
	//		if(this.mapInferredTypes == null) {
	//			this.mapInferredTypes = this.infer(query);
	//		}
	//
	//		this.mapInferredTypes;
	//	}

	def genericInferBGP(bgpFunc: (OpBGP) => Map[Node, Set[MorphBaseClassMapping]])(op:Op)
	: Map[Node, Set[MorphBaseClassMapping]] = {
		op match {
			case bgp:OpBGP => bgpFunc(bgp)
			case opLeftJoin:OpLeftJoin => {
				val mapNodeTypesLeft = this.genericInferBGP(bgpFunc)(opLeftJoin.getLeft())
				val mapNodeTypesRight = this.genericInferBGP(bgpFunc)(opLeftJoin.getRight())
				MorphQueryTranslatorUtility.mapsIntersection(mapNodeTypesLeft, mapNodeTypesRight)
			}
			case opUnion:OpUnion => {
				val mapNodeTypesLeft = this.genericInferBGP(bgpFunc)(opUnion.getLeft())
				val mapNodeTypesRight = this.genericInferBGP(bgpFunc)(opUnion.getRight())
				MorphQueryTranslatorUtility.mapsIntersection(mapNodeTypesLeft, mapNodeTypesRight)
			}
			case opJoin:OpJoin => {
				val mapNodeTypesLeft = this.genericInferBGP(bgpFunc)(opJoin.getLeft())
				val mapNodeTypesRight = this.genericInferBGP(bgpFunc)(opJoin.getRight())
				MorphQueryTranslatorUtility.mapsIntersection(mapNodeTypesLeft, mapNodeTypesRight)
			}
			case opFilter:OpFilter => {
				val mapFilterSubOp = this.genericInferBGP(bgpFunc)(opFilter.getSubOp())
				//val mapFilterExprs = this.inferExprs(bgpFunc)(opFilter.getExprs())
				mapFilterSubOp
			}
			case opDistinct:OpDistinct => this.genericInferBGP(bgpFunc)(opDistinct.getSubOp())
			case opProject:OpProject => this.genericInferBGP(bgpFunc)(opProject.getSubOp())
			case opSlice:OpSlice => this.genericInferBGP(bgpFunc)(opSlice.getSubOp())
			case opExtend:OpExtend => this.genericInferBGP(bgpFunc)(opExtend.getSubOp())
			case opGroup:OpGroup => this.genericInferBGP(bgpFunc)(opGroup.getSubOp())
			case opOrder:OpOrder => this.genericInferBGP(bgpFunc)(opOrder.getSubOp())
		}
	}

	/*
  def inferExprs(exprList:ExprList) : Map[Node, Set[MorphBaseClassMapping]] = {
    this.inferExprList(exprList.getList.toList)
  }

  def inferExprList(exprList:List[Expr]) : Map[Node, Set[MorphBaseClassMapping]] = {
  }

  def interExpr(expr:Expr) : Map[Node, Set[MorphBaseClassMapping]] = {

  }
  */

	def genericInfer(tripleFunc: (Triple) => Option[Pair[Node, Set[MorphBaseClassMapping]]])(op:Op)
	: Map[Node, Set[MorphBaseClassMapping]] = {
		def bgpHelper(bgp:OpBGP): Map[Node, Set[MorphBaseClassMapping]] = {
			val bpTriples = bgp.getPattern().getList()
			val newMappings = bpTriples.flatMap(tripleFunc(_))
			newMappings.foldLeft(Map.empty[Node, Set[MorphBaseClassMapping]])(
				(mapNodeTypes, tpNodeCms) => tpNodeCms match {
					case (tpNode, cms) => this.addToInferredTypes(mapNodeTypes, tpNode, cms.toSet)
				})
		}
		genericInferBGP(bgpHelper _)(op)
	}

	//	def infer(query:Query ) : Map[Node, Set[MorphBaseClassMapping]] = {
	//		if (this.mapInferredTypes.isEmpty) {
	//			val queryPattern = query.getQueryPattern();
	//			val opQueryPattern = Algebra.compile(queryPattern);
	//			this.mapInferredTypes = this.infer(opQueryPattern);
	//		}
	//
	//		this.mapInferredTypes;
	//	}

	def infer(query:Query ) : Map[Node, Set[MorphBaseClassMapping]] = {
		val queryPattern = query.getQueryPattern();
		val opQueryPattern = Algebra.compile(queryPattern);
		this.infer(opQueryPattern);
	}

	//	def infer(opQueryPattern:Op ) : Map[Node, Set[MorphBaseClassMapping]] = {
	//		if (this.mapInferredTypes.isEmpty) {
	//			val mapSubjectTypesByRdfType = this.inferByRDFType(opQueryPattern);
	//			val mapSubjectTypesByPredicatesURIs = this.inferSubjectTypesByPredicatesURIs(opQueryPattern);
	//			val mapSubjectTypesBySubjectUri = this.inferSubjectTypesBySubjectURI(opQueryPattern);
	//
	//			val listSubjectMapNodes = List(mapSubjectTypesByRdfType,
	//										   mapSubjectTypesByPredicatesURIs,
	//										   mapSubjectTypesBySubjectUri)
	//			val mapSubjectTypes = MorphQueryTranslatorUtility.mapsIntersection(listSubjectMapNodes);
	//
	//			val mapObjectTypesByObjectsURIs = this.inferObjectTypesByObjectURI(opQueryPattern)
	//			val mapObjectTypesWithConceptMappingsWithClassURI =
	//				mapObjectTypesByObjectsURIs.mapValues(_.filter(_.getMappedClassURIs.nonEmpty))
	//			val mapObjectTypesByPredicatesURIs = this.inferObjectTypesByPredicateURI(opQueryPattern, mapSubjectTypes)
	//
	//			val listMapNodes = List(mapSubjectTypesByRdfType,
	//									mapSubjectTypesBySubjectUri,
	//									mapSubjectTypesByPredicatesURIs,
	//									mapObjectTypesWithConceptMappingsWithClassURI,
	//									mapObjectTypesByPredicatesURIs)
	//			this.mapInferredTypes = MorphQueryTranslatorUtility.mapsIntersection(listMapNodes)
	//		}
	//
	//		this.mapInferredTypes;
	//	}

	def infer(opQueryPattern:Op ) : Map[Node, Set[MorphBaseClassMapping]] = {
		val mapSubjectTypesByRdfType = this.inferByRDFType(opQueryPattern);
		val mapSubjectTypesByPredicatesURIs = this.inferSubjectTypesByPredicatesURIs(opQueryPattern);
		val mapSubjectTypesBySubjectUri = this.inferSubjectTypesBySubjectURI(opQueryPattern);

		val listSubjectMapNodes = List(mapSubjectTypesByRdfType,
			mapSubjectTypesByPredicatesURIs,
			mapSubjectTypesBySubjectUri)
		val mapSubjectTypes = MorphQueryTranslatorUtility.mapsIntersection(listSubjectMapNodes);

		val mapObjectTypesByObjectsURIs = this.inferObjectTypesByObjectURI(opQueryPattern)
		val mapObjectTypesWithConceptMappingsWithClassURI =
			mapObjectTypesByObjectsURIs.mapValues(_.filter(_.getMappedClassURIs.nonEmpty))
		val mapObjectTypesByPredicatesURIs = this.inferObjectTypesByPredicateURI(opQueryPattern, mapSubjectTypes)

		val listMapNodes = List(mapSubjectTypesByRdfType,
			mapSubjectTypesBySubjectUri,
			mapSubjectTypesByPredicatesURIs,
			mapObjectTypesWithConceptMappingsWithClassURI,
			mapObjectTypesByPredicatesURIs)
		MorphQueryTranslatorUtility.mapsIntersection(listMapNodes)

	}

	def inferByRDFType(op:Op) : Map[Node, Set[MorphBaseClassMapping]] = {
		def helper(tp: Triple) : Option[Pair[Node, Set[MorphBaseClassMapping]]] = {
			val tpPredicate = tp.getPredicate()
			if (tpPredicate.isURI()) {
				val predicateURI = tpPredicate.getURI()
				val tpObject = tp.getObject()

				if(RDF.`type`.getURI().equalsIgnoreCase(predicateURI) && tpObject.isURI()) {
					val subjectType = tpObject.getURI()
					val cms = this.mappingDocument.getClassMappingsByClassURI(subjectType);

					if (cms.nonEmpty) {
						val tpSubject = tp.getSubject()
						Some(tpSubject -> cms.toSet)
						//this.mapNodeConceptMapping.put(subject, cm);
					} else {
						val errorMessage = "No rdf:type mapping for: " + subjectType
						logger.debug(errorMessage)
						None
					}
				} else None
			} else None
		}
		genericInfer(helper _)(op)
	}

	def inferSubjectsTypesByPredicateURIs(mapSubjectSTGs:Map[Node, Set[Triple]] )
	: Map[Node, Set[MorphBaseClassMapping]] =
		mapSubjectSTGs.mapValues(stg =>
			this.mappingDocument.getClassMappingByPropertyURIs(
				stg.map(_.getPredicate())
					.filter(_.isURI())
					.map(_.getURI())
					.filter(!RDF.`type`.getURI().equalsIgnoreCase(_))
			).toSet)

	def bgpToSTGs(triples:List[Triple] ) : Map[Node, Set[Triple]] =
		triples.groupBy(_.getSubject()).mapValues(_.toSet)

	def inferSubjectTypesBySubjectURI(op:Op ) : Map[Node, Set[MorphBaseClassMapping]] = {
		def helper(tp: Triple) : Option[Pair[Node, Set[MorphBaseClassMapping]]] = {
			val tpSubject = tp.getSubject()
			if (tpSubject.isURI()) {
				val subjectURI = tpSubject.getURI()
				val subjectTypes = this.inferByURI(subjectURI)
				if (subjectTypes.nonEmpty) {
					Some(tpSubject, subjectTypes)
				} else None
			} else None
		}
		genericInfer(helper _)(op)
	}

	def inferObjectTypesByExprList(exprList:ExprList ) : Map[Node, Set[MorphBaseClassMapping]] = {
		val listOfMaps = exprList.getList().map(this.inferObjectTypesByExpr(_))
		MorphQueryTranslatorUtility.mapsIntersection(listOfMaps.toList)
	}

	def inferObjectTypesByExpr(expr:Expr ) : Map[Node, Set[MorphBaseClassMapping]] = {
		val mapNodeTypesExprs : Map[Node, Set[MorphBaseClassMapping]] = {
			if(expr.isConstant()) {
				val nodeValue = expr.getConstant();
				if(nodeValue.isIRI()) {
					val nodeURI = nodeValue.getNode();
					val uri = nodeURI.getURI().toString();
					val possibleTypes = this.inferByURI(uri);
					if (possibleTypes.nonEmpty) {
						Map(nodeURI -> possibleTypes);
					} else Map.empty
				} else Map.empty
			} else if(expr.isFunction()) {
				val exprFunction = expr.getFunction();
				val args = exprFunction.getArgs();
				val listOfMaps = args.map(this.inferObjectTypesByExpr(_))
				MorphQueryTranslatorUtility.mapsIntersection(listOfMaps.toList);
			} else Map.empty
		}

		mapNodeTypesExprs;
	}

	def inferObjectTypesByObjectURI(op:Op ) : Map[Node, Set[MorphBaseClassMapping]] = {
		def helper(tp: Triple) : Option[Pair[Node, Set[MorphBaseClassMapping]]] = {
			val tpObject = tp.getObject()
			if (tpObject.isURI()) {
				val objectURI = tpObject.getURI()
				val nodeTypes = this.inferByURI(objectURI)
				if (nodeTypes.nonEmpty) {
					Some(tpObject -> nodeTypes)
				} else None
			} else None
		}
		genericInfer(helper _)(op)
	}


	def  inferObjectTypesByPredicateURI(op:Op , mapSubjectTypes:Map[Node, Set[MorphBaseClassMapping]] )
	: Map[Node, Set[MorphBaseClassMapping]] = {
		def createHelper(mapSubjectTypes: Map[Node, Set[MorphBaseClassMapping]])(tp: Triple) :
		Option[Pair[Node, Set[MorphBaseClassMapping]]] = {
			val tpPredicate = tp.getPredicate()

			if (tpPredicate.isURI()) {
				val predicateURI = tpPredicate.getURI()
				if (!RDF.`type`.getURI().equalsIgnoreCase(predicateURI)) {
					val tpSubject = tp.getSubject()
					val subjectTypes = mapSubjectTypes.get(tpSubject)
					val arbitraryCm = subjectTypes.flatMap(_.headOption)
					val objectNodeTypes = arbitraryCm match {
						case Some(cm) => this.mappingDocument.getPossibleRange(predicateURI, cm)
						case None => this.mappingDocument.getPossibleRange(predicateURI)
					}
					if (objectNodeTypes.nonEmpty) {
						val tpObject = tp.getObject()
						Some(tpObject -> objectNodeTypes.toSet)
					} else None
				} else None
			} else None
		}

		val helper = createHelper(mapSubjectTypes) _;
		val result = genericInfer(helper)(op)
		result;
	}

	def inferSubjectTypesByPredicatesURIs(op:Op ) : Map[Node, Set[MorphBaseClassMapping]] = {
		def helper(bgp: OpBGP) : Map[Node, Set[MorphBaseClassMapping]] = {
			val bpTriples = bgp.getPattern().getList()
			val mapSubjectSTGs = this.bgpToSTGs(bpTriples.toList)

			//get subject types by all the predicate URIs of the STGs
			val subjectsTypesByPredicateURIs = this.inferSubjectsTypesByPredicateURIs(mapSubjectSTGs)
			subjectsTypesByPredicateURIs.foldLeft(Map.empty[Node, Set[MorphBaseClassMapping]])(
				(mapNodeTypes, subjectAndTypes) => subjectAndTypes match {
					case (subject, subjectTypes) => this.addToInferredTypes(mapNodeTypes, subject, subjectTypes)
				}
			)
		}
		genericInferBGP(helper _)(op)
	}

	def getTypes(node:Node ) : Set[MorphBaseClassMapping] =
		this.mapInferredTypes.getOrElse(node, Set.empty).toSet

	def inferByURI(uri:String ) : Set[MorphBaseClassMapping] =
		this.mappingDocument.classMappings.filter(_.isPossibleInstance(uri)).toSet

	def printInferredTypes() : String  = {
		var result = new StringBuffer();
		for(key <- this.mapInferredTypes.keySet) {
			result.append(key + " : " + this.mapInferredTypes.get(key) + "\n");
		}
		result.toString();
	}
}