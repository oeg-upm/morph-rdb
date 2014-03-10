package es.upm.fi.dia.oeg.morph.base.querytranslator.engine

import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.algebra.Algebra
import com.hp.hpl.jena.sparql.algebra.Op
import com.hp.hpl.jena.sparql.algebra.op.OpBGP
import com.hp.hpl.jena.vocabulary.RDF
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin
import com.hp.hpl.jena.sparql.algebra.op.OpUnion
import com.hp.hpl.jena.sparql.algebra.op.OpJoin
import com.hp.hpl.jena.sparql.algebra.op.OpFilter
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.sparql.expr.ExprList
import com.hp.hpl.jena.sparql.expr.Expr
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping

class MorphMappingInferrer(mappingDocument:MorphBaseMappingDocument ) {
	val logger = Logger.getLogger("MorphMappingInferrer");
	var mapInferredTypes : Map[Node, Set[MorphBaseClassMapping]] = null;
	var query:Query  = null;

	def addToInferredTypes(mapNodeTypes:Map[Node, Set[MorphBaseClassMapping]] 
	, node:Node , cms:Set[MorphBaseClassMapping] ) 
	: Map[Node, Set[MorphBaseClassMapping]] = {
		val types = mapNodeTypes.get(node);
		var result = mapNodeTypes;
		
		if(types.isDefined) { 
			val intersection = types.get.intersect(cms);
			result = result + (node -> intersection);
		} else {
		  result = result + (node -> cms);
		}
		
		result
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

	def infer() : Map[Node, Set[MorphBaseClassMapping]] = {
		if(this.mapInferredTypes == null) {
			this.mapInferredTypes = this.infer(query); 
		}

		this.mapInferredTypes;
	}

	def infer(query:Query ) : Map[Node, Set[MorphBaseClassMapping]] = {
		if(this.mapInferredTypes == null || this.mapInferredTypes.isEmpty) {
			val queryPattern = query.getQueryPattern();
			val opQueryPattern = Algebra.compile(queryPattern);
			this.mapInferredTypes = this.infer(opQueryPattern);			
		}

		this.mapInferredTypes;
	}

	def infer(opQueryPattern:Op ) : Map[Node, Set[MorphBaseClassMapping]] = {
		if(this.mapInferredTypes == null) {
			val mapSubjectTypesByRdfType = this.inferByRDFType(opQueryPattern);
			val mapSubjectTypesByPredicatesURIs = this.inferSubjectTypesByPredicatesURIs(opQueryPattern);
			val mapSubjectTypesBySubjectUri = this.inferSubjectTypesBySubjectURI(opQueryPattern);
			
			val listSubjectMapNodes = List(mapSubjectTypesByRdfType, mapSubjectTypesByPredicatesURIs, mapSubjectTypesBySubjectUri) 
			val mapSubjectTypes = MorphQueryTranslatorUtility.mapsIntersection(listSubjectMapNodes);
			
			var mapObjectTypesByObjectsURIs = this.inferObjectTypesByObjectURI(opQueryPattern);
			for(mapNodeTypesByUriKey <- mapObjectTypesByObjectsURIs.keySet) {
				val mapNodeTypesByUriValue = mapObjectTypesByObjectsURIs.get(mapNodeTypesByUriKey);
				
				if(mapNodeTypesByUriValue.isDefined) {
					val mapNodeTypesByUriValueSet = mapNodeTypesByUriValue.get;
					val setConceptMappingsWithoutClassURI = mapNodeTypesByUriValueSet.flatMap(cm => {
						val mappedClassURIs = cm.getMappedClassURIs(); 
						if(mappedClassURIs == null || mappedClassURIs.isEmpty()) {
							Some(cm);
						} else {
						  None
						}
					})
					
					mapNodeTypesByUriValueSet.removeAll(setConceptMappingsWithoutClassURI);
					mapObjectTypesByObjectsURIs += (mapNodeTypesByUriKey -> mapNodeTypesByUriValueSet);
				}
			}
			val mapObjectTypesByPredicatesURIs = this.inferObjectTypesByPredicateURI(opQueryPattern, mapSubjectTypes); 

			val listMapNodes = List(mapSubjectTypesByRdfType, mapSubjectTypesBySubjectUri
			    , mapSubjectTypesByPredicatesURIs, mapObjectTypesByObjectsURIs, mapObjectTypesByPredicatesURIs)

			val mapNodeTypes = MorphQueryTranslatorUtility.mapsIntersection(listMapNodes);
			this.mapInferredTypes = mapNodeTypes;			
		}

		this.mapInferredTypes;
	}
	
	def inferByRDFType(op:Op ) : Map[Node, Set[MorphBaseClassMapping]] = {
		var mapNodeTypes:Map[Node, Set[MorphBaseClassMapping]] = Map.empty;

		op match {
		case bgp:OpBGP => {
			val bp = bgp.getPattern();
			val bpTriples = bp.getList();
			for(tp <- bpTriples) {
				val tpSubject = tp.getSubject();
				val tpPredicate = tp.getPredicate();
				if(tpPredicate.isURI()) {
					val predicateURI = tpPredicate.getURI();
					val tpObject = tp.getObject();

					if(RDF.`type`.getURI().equalsIgnoreCase(predicateURI) && tpObject.isURI()) {
						val subjectType = tpObject.getURI();
						val cms = this.mappingDocument.getConceptMappingsByConceptName(subjectType);

						if(cms != null && cms.size() > 0) {
							mapNodeTypes = this.addToInferredTypes(mapNodeTypes, tpSubject, cms.toSet);
							//this.mapNodeConceptMapping.put(subject, cm);
						} else {
							val errorMessage = "No rdf:type mapping for: " + subjectType;
							logger.debug(errorMessage);
						}
					}					
				}

			}
		}
		case opLeftJoin:OpLeftJoin => {
			val mapNodeTypesLeft = this.inferByRDFType(opLeftJoin.getLeft());
			val mapNodeTypesRight = this.inferByRDFType(opLeftJoin.getRight());
			mapNodeTypes = MorphQueryTranslatorUtility.mapsIntersection(mapNodeTypesLeft, mapNodeTypesRight);
		}
		case opUnion:OpUnion => {
			val mapNodeTypesLeft = this.inferByRDFType(opUnion.getLeft());
			val mapNodeTypesRight = this.inferByRDFType(opUnion.getRight());
			mapNodeTypes = MorphQueryTranslatorUtility.mapsIntersection(mapNodeTypesLeft, mapNodeTypesRight);
		}
		case opJoin:OpJoin => {
			val opJoinLeft = opJoin.getLeft();
			val opJoinRight = opJoin.getRight();
			val mapNodeTypesLeft = this.inferByRDFType(opJoinLeft);
			val mapNodeTypesRight = this.inferByRDFType(opJoinRight);
			mapNodeTypes = MorphQueryTranslatorUtility.mapsIntersection(mapNodeTypesLeft, mapNodeTypesRight);
		}
		case opFilter:OpFilter => {
			mapNodeTypes = this.inferByRDFType(opFilter.getSubOp());
		}
		}

		return mapNodeTypes;
	}

	def inferSubjectsTypesByPredicateURIs(mapSubjectSTGs:Map[Node, Set[Triple]] ) 
	: Map[Node, Set[MorphBaseClassMapping]] = {
		val result = mapSubjectSTGs.keys.flatMap(subject => {
			val stg = mapSubjectSTGs.get(subject);
			if(stg.isDefined) {
				val predicateURIs = stg.get.flatMap(tp => {
					val tpPredicate = tp.getPredicate();
					if(tpPredicate.isURI()) {
						val predicateURI = tpPredicate.getURI();
						if(!RDF.`type`.getURI().equalsIgnoreCase(predicateURI)) {
							Some(predicateURI);
						} else {
						  None
						}
					} else {
					  None
					}			  
				})
			
				val subjectTypes = this.mappingDocument.getConceptMappingByPropertyURIs(predicateURIs);
				Some((subject -> subjectTypes.toSet));			  
			} else {
			  None
			}
		  
		})

		result.toMap;
	}

	def bgpToSTGs(triples:List[Triple] ) : Map[Node, Set[Triple]]  = {
		val groupedTriples = triples.groupBy(_.getSubject());
		val result = groupedTriples.keys.map(node => (node -> Set(groupedTriples(node)).flatten))
		val result2 = result.toMap;
		result2
	}
	
	def  inferSubjectTypesBySubjectURI(op:Op ) : Map[Node, Set[MorphBaseClassMapping]] = {
		var mapNodeTypes:Map[Node, Set[MorphBaseClassMapping]] = Map.empty;

		op match {
		  case bgp:OpBGP => {
			val bp = bgp.getPattern();
			val bpTriples = bp.getList();
			this.bgpToSTGs(bpTriples.toList);

			for(tp <- bpTriples) {
				val tpSubject = tp.getSubject();
				if(tpSubject.isURI()) {
					val subjectURI = tpSubject.getURI();
					val subjectTypes = this.inferByURI(subjectURI);
					if(subjectTypes != null && subjectTypes.size() > 0) {
						mapNodeTypes = this.addToInferredTypes(mapNodeTypes, tpSubject, subjectTypes);
					}
				}
			}		    
		  }
		  case opLeftJoin:OpLeftJoin => {
			val mapNodeTypesLeft = this.inferSubjectTypesBySubjectURI(opLeftJoin.getLeft());
			val mapNodeTypesRight = this.inferSubjectTypesBySubjectURI(opLeftJoin.getRight());
			mapNodeTypes = MorphQueryTranslatorUtility.mapsIntersection(mapNodeTypesLeft, mapNodeTypesRight);		    
		  }
		  case opUnion:OpUnion => {
			val mapNodeTypesLeft = this.inferSubjectTypesBySubjectURI(opUnion.getLeft());
			val mapNodeTypesRight = this.inferSubjectTypesBySubjectURI(opUnion.getRight());
			mapNodeTypes = MorphQueryTranslatorUtility.mapsIntersection(mapNodeTypesLeft, mapNodeTypesRight);		    
		  }
		  case opJoin:OpJoin => {
			val mapNodeTypesLeft = this.inferSubjectTypesBySubjectURI(opJoin.getLeft());
			val mapNodeTypesRight = this.inferSubjectTypesBySubjectURI(opJoin.getRight());
			mapNodeTypes = MorphQueryTranslatorUtility.mapsIntersection(mapNodeTypesLeft, mapNodeTypesRight);		    
		  }
		  case opFilter:OpFilter => {
		    val opFilterSubOp = opFilter.getSubOp() ;
			val exprList = opFilter.getExprs();
			val mapNodeTypesExprs = this.inferObjectTypesByExprList(exprList);
			val mapNodeTypesSubOp = this.inferSubjectTypesBySubjectURI(opFilterSubOp);
			mapNodeTypes = MorphQueryTranslatorUtility.mapsIntersection(mapNodeTypesSubOp, mapNodeTypesExprs);		    
		  }
		  
		}
		
		return mapNodeTypes;
	}

	def inferObjectTypesByExprList(exprList:ExprList ) : Map[Node, Set[MorphBaseClassMapping]] = {
		val listOfMaps = exprList.getList().map(expr => {
			val map = this.inferObjectTypesByExpr(expr);
			map
		})
		
		val mapNodeTypesExprs = MorphQueryTranslatorUtility.mapsIntersection(listOfMaps.toList);
		mapNodeTypesExprs;
	}	
	
	def inferObjectTypesByExpr(expr:Expr ) : Map[Node, Set[MorphBaseClassMapping]] = {
		val mapNodeTypesExprs : Map[Node, Set[MorphBaseClassMapping]] = {
			if(expr.isConstant()) {
				val nodeValue = expr.getConstant();
				if(nodeValue.isIRI()) {
					val nodeURI = nodeValue.getNode();
					val uri = nodeURI.getURI().toString();
					val possibleTypes = this.inferByURI(uri);
					if(possibleTypes != null && possibleTypes.size() > 0) {
						Map(nodeURI -> possibleTypes);
					} else {
					  Map.empty
					}
				} else {
				  Map.empty
				}
			} else if(expr.isFunction()) {
				val exprFunction = expr.getFunction();
				val args = exprFunction.getArgs();
				val listOfMaps = args.map(arg => {
					val map = this.inferObjectTypesByExpr(arg);
					map;
				})
				MorphQueryTranslatorUtility.mapsIntersection(listOfMaps.toList);
			} else {
			  Map.empty
			}
		}

		mapNodeTypesExprs;
	}

	def  inferObjectTypesByObjectURI(op:Op ) : Map[Node, Set[MorphBaseClassMapping]] = {
		var mapNodeTypes : Map[Node, Set[MorphBaseClassMapping]] = Map.empty

		op match {
		  case bgp:OpBGP => {
			for(tp <- bgp.getPattern().getList()) {
				val tpObject = tp.getObject();
				if(tpObject.isURI()) {
					val objectURI = tpObject.getURI();
					val nodeTypes = this.inferByURI(objectURI);
					if(nodeTypes != null && nodeTypes.size() > 0) {
						mapNodeTypes = this.addToInferredTypes(mapNodeTypes, tpObject, nodeTypes);
					}
				}
			}		    
		  }
		  case opLeftJoin:OpLeftJoin => {
			val mapNodeTypesLeft = this.inferObjectTypesByObjectURI(opLeftJoin.getLeft());
			val mapNodeTypesRight = this.inferObjectTypesByObjectURI(opLeftJoin.getRight());
			mapNodeTypes = MorphQueryTranslatorUtility.mapsIntersection(mapNodeTypesLeft, mapNodeTypesRight);		    
		  }
		  case opUnion:OpUnion => {
			val mapNodeTypesLeft = this.inferObjectTypesByObjectURI(opUnion.getLeft());
			val mapNodeTypesRight = this.inferObjectTypesByObjectURI(opUnion.getRight());
			mapNodeTypes = MorphQueryTranslatorUtility.mapsIntersection(mapNodeTypesLeft, mapNodeTypesRight);		    
		  }
		  case opJoin:OpJoin => {
			val mapNodeTypesLeft = this.inferObjectTypesByObjectURI(opJoin.getLeft());
			val mapNodeTypesRight = this.inferObjectTypesByObjectURI(opJoin.getRight());
			mapNodeTypes = MorphQueryTranslatorUtility.mapsIntersection(mapNodeTypesLeft, mapNodeTypesRight);
		  }
		  case opFilter:OpFilter => {
			  mapNodeTypes = this.inferObjectTypesByObjectURI(opFilter.getSubOp());		    
		  }
		}
		
		mapNodeTypes;
	}
	
	def  inferObjectTypesByPredicateURI(op:Op , mapSubjectTypes:Map[Node, Set[MorphBaseClassMapping]] ) 
	: Map[Node, Set[MorphBaseClassMapping]] = {
		var mapNodeTypes : Map[Node, Set[MorphBaseClassMapping]] = Map.empty;

		op match {
		  case bgp:OpBGP => {
			for(tp <- bgp.getPattern().getList()) {
				val tpSubject = tp.getSubject();
				val tpPredicate = tp.getPredicate();
				val tpObject = tp.getObject();
				
				val subjectTypes = mapSubjectTypes.get(tpSubject);
				
				if(tpPredicate.isURI()) {
					val predicateURI = tpPredicate.getURI();
					if(!RDF.`type`.getURI().equalsIgnoreCase(predicateURI)) {
						val nodeTypes = {
							if(subjectTypes.isDefined) {
								val subjectTypesSet = subjectTypes.get;
								if(subjectTypesSet != null && !subjectTypesSet.isEmpty) {
									val cm = subjectTypes.get.iterator.next();
									this.mappingDocument.getPossibleRange(predicateURI, cm);								  
								} else {
								  this.mappingDocument.getPossibleRange(predicateURI);
								}
							} else {
							  this.mappingDocument.getPossibleRange(predicateURI);
							}
						}
						
						if(nodeTypes != null && nodeTypes.size() > 0) {
							mapNodeTypes += (tpObject -> nodeTypes.toSet);	
						}

					}
				}
			}		    
		  }
		  case opLeftJoin:OpLeftJoin => {
			val mapNodeTypesLeft = this.inferObjectTypesByPredicateURI(opLeftJoin.getLeft(), mapSubjectTypes);
			val mapNodeTypesRight = this.inferObjectTypesByPredicateURI(opLeftJoin.getRight(), mapSubjectTypes);
			mapNodeTypes = MorphQueryTranslatorUtility.mapsIntersection(mapNodeTypesLeft, mapNodeTypesRight);		    
		  }
		  case opUnion:OpUnion => {
			val mapNodeTypesLeft = this.inferObjectTypesByPredicateURI(opUnion.getLeft(), mapSubjectTypes);
			val mapNodeTypesRight = this.inferObjectTypesByPredicateURI(opUnion.getRight(), mapSubjectTypes);
			mapNodeTypes = MorphQueryTranslatorUtility.mapsIntersection(mapNodeTypesLeft, mapNodeTypesRight);		    
		  }
		  case opJoin:OpJoin => {
			val mapNodeTypesLeft = this.inferObjectTypesByPredicateURI(opJoin.getLeft(), mapSubjectTypes);
			val mapNodeTypesRight = this.inferObjectTypesByPredicateURI(opJoin.getRight(), mapSubjectTypes);
			mapNodeTypes = MorphQueryTranslatorUtility.mapsIntersection(mapNodeTypesLeft, mapNodeTypesRight);		    
		  }
		  case opFilter:OpFilter => {
			mapNodeTypes = this.inferObjectTypesByPredicateURI(opFilter.getSubOp(), mapSubjectTypes);
		    
		  }
		  
		}
		
		mapNodeTypes;
	}	
	
	def inferSubjectTypesByPredicatesURIs(op:Op ) : Map[Node, Set[MorphBaseClassMapping]] = {
		var mapNodeTypes : Map[Node, Set[MorphBaseClassMapping]] = Map.empty;

		op match {
		  case bgp:OpBGP => {
			val bp = bgp.getPattern();
			val bpTriples = bp.getList();
			val mapSubjectSTGs = this.bgpToSTGs(bpTriples.toList);
			
			//get subject types by all the predicate URIs of the STGs
			val subjectsTypesByPredicateURIs = this.inferSubjectsTypesByPredicateURIs(mapSubjectSTGs);
			for(subject <- subjectsTypesByPredicateURIs.keySet) {
				val subjectTypes = subjectsTypesByPredicateURIs.get(subject);
				if(subjectTypes.isDefined) {
				  mapNodeTypes = this.addToInferredTypes(mapNodeTypes, subject, subjectTypes.get);
				}
			}		    
		  }
		  case opLeftJoin:OpLeftJoin => {
			val mapNodeTypesLeft = this.inferSubjectTypesByPredicatesURIs(opLeftJoin.getLeft());
			val mapNodeTypesRight = this.inferSubjectTypesByPredicatesURIs(opLeftJoin.getRight());
			mapNodeTypes = MorphQueryTranslatorUtility.mapsIntersection(mapNodeTypesLeft, mapNodeTypesRight);		    
		  }
		  case opUnion:OpUnion => {
			val mapNodeTypesLeft = this.inferSubjectTypesByPredicatesURIs(opUnion.getLeft());
			val mapNodeTypesRight = this.inferSubjectTypesByPredicatesURIs(opUnion.getRight());
			mapNodeTypes = MorphQueryTranslatorUtility.mapsIntersection(mapNodeTypesLeft, mapNodeTypesRight);		    
		  }
		  case opJoin:OpJoin => {
			val mapNodeTypesLeft = this.inferSubjectTypesByPredicatesURIs(opJoin.getLeft());
			val mapNodeTypesRight = this.inferSubjectTypesByPredicatesURIs(opJoin.getRight());
			mapNodeTypes = MorphQueryTranslatorUtility.mapsIntersection(mapNodeTypesLeft, mapNodeTypesRight);		    
		  }
		  case opFilter:OpFilter => {
			  mapNodeTypes = this.inferSubjectTypesByPredicatesURIs(opFilter.getSubOp());		    
		  }
		}
		
		mapNodeTypes;
	}

	def  getTypes(node:Node ) : Set[MorphBaseClassMapping] = {
	  val result = {
		val resultAux = this.mapInferredTypes.get(node); 
	    if(resultAux.isDefined) {resultAux.get}
	    else {Set.empty}
	  }
	  result.toSet
	}
	
	def inferByURI(uri:String ) : Set[MorphBaseClassMapping] = {
	  val cms = this.mappingDocument.classMappings
	  val result = cms.flatMap(cm => {
			val possibleInstance = cm.isPossibleInstance(uri);
			if(possibleInstance) {
				Some(cm);
			} else {
			  None
			}		  
		})
				
		result.toSet;
	}

	def printInferredTypes() : String  = {
		var result = new StringBuffer();
		for(key <- this.mapInferredTypes.keySet) {
			result.append(key + " : " + this.mapInferredTypes.get(key) + "\n");
		}
		result.toString();
	}	
}