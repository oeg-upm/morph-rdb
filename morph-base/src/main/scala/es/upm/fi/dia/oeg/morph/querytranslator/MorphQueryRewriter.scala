package es.upm.fi.dia.oeg.morph.querytranslator

import com.hp.hpl.jena.graph.query.Rewrite
import org.apache.log4j.Logger
import com.hp.hpl.jena.graph.Node
import scala.collection.JavaConversions._
import com.hp.hpl.jena.sparql.algebra.Op
import com.hp.hpl.jena.sparql.algebra.op.OpBGP
import es.upm.fi.dia.oeg.morph.base.SPARQLUtility
import com.hp.hpl.jena.sparql.core.BasicPattern
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.sparql.algebra.op.OpJoin
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin
import es.upm.fi.dia.oeg.morph.base.MorphTriple
import com.hp.hpl.jena.sparql.algebra.op.OpUnion
import com.hp.hpl.jena.sparql.algebra.op.OpFilter
import com.hp.hpl.jena.sparql.algebra.optimize.TransformFilterConjunction
import com.hp.hpl.jena.sparql.algebra.optimize.Optimize
import com.hp.hpl.jena.sparql.algebra.op.OpProject
import com.hp.hpl.jena.sparql.algebra.op.OpSlice
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct
import com.hp.hpl.jena.sparql.algebra.op.OpOrder
import com.hp.hpl.jena.vocabulary.RDFS
import com.hp.hpl.jena.rdf.model.ResourceFactory
import com.hp.hpl.jena.vocabulary.RDF


class MorphQueryRewriter(mapNodeLogicalTableSize:java.util.Map[Node, java.lang.Long], reorderSTG:Boolean)
extends Rewrite {
	def logger = Logger.getLogger("MorphQueryRewriter");
	//private Map<Node, Set<AbstractConceptMapping>> mapInferredTypes;
//	var reorderSTG = false;
	

	def rewrite(op:Op) : Op = {
		var result : Op = null;
		
		op match {
		  	case bgp:OpBGP => {
		  		result = this.rewriteBGP(bgp);
			} 
			case opJoin:OpJoin => { // AND pattern
			  result = this.rewriteJoin(opJoin);
			} 
			case opLeftJoin:OpLeftJoin => { //OPT pattern
				result = this.rewriteLeftJoin(opLeftJoin);
			} 
			case opUnion:OpUnion => { //UNION pattern
				val leftChild = opUnion.getLeft();
				val rightChild = opUnion.getRight();
				val leftChildRewritten = this.rewrite(leftChild);
				val rightChildRewritten = this.rewrite(rightChild);
				result = new OpUnion(leftChildRewritten, rightChildRewritten);
			} 
			case opFilter:OpFilter => { //FILTER pattern
				val exprs = opFilter.getExprs();
				val subOp = opFilter.getSubOp();
				
				val tfc = new TransformFilterConjunction();
				Optimize.apply("test", tfc , opFilter);
				
				
	//			Op op2 = null;
	//			if(subOp instanceof OpBGP) {
	//				BasicPattern basicPattern = ((OpBGP) subOp).getPattern();
	//				op2 = TransformFilterPlacement.transform(exprs, basicPattern);
	//			}
				val subOpRewritten = this.rewrite(subOp);
				result = OpFilter.filter(exprs, subOpRewritten);
			} 
			case opProject:OpProject => {
				val subOp = opProject.getSubOp();
				val subOpRewritten = this.rewrite(subOp);
				result = new OpProject(subOpRewritten, opProject.getVars());
			} 
			case opSlice:OpSlice => {
				val subOp = opSlice.getSubOp();
				val subOpRewritten = this.rewrite(subOp);
				result = new OpSlice(subOpRewritten, opSlice.getStart(), opSlice.getLength());
			} 
			case opDistinct:OpDistinct => {
				val subOp = opDistinct.getSubOp();
				val subOpRewritten = this.rewrite(subOp);
				result = new OpDistinct(subOpRewritten);
			} 
			case opOrder:OpOrder => {
				val subOp = opOrder.getSubOp();
				val subOpRewritten = this.rewrite(subOp);
				result = new OpOrder(subOpRewritten, opOrder.getConditions());
			} 
			case _ => {
				result = op;
			}
		}
		


		result;
	}

//	public void setMapInferredTypes(
//			Map<Node, Set<AbstractConceptMapping>> mapInferredTypes) {
//		this.mapInferredTypes = mapInferredTypes;
//	}
	
	def reorderSTGs(triples:List[Triple]) : List[Triple] = {
		
		var result : List[Triple] = Nil;
		
		if(triples == null) {
			result = null;
		} else if(triples.size() == 1) {
			result = triples;
		} else {
			var mapNodeTriples:Map[Node, List[Triple]] = Map.empty;
			var mapNodeTableSize:Map[Node, Long]  = Map.empty;
			
			for(tp <- triples) {
				val tpSubject = tp.getSubject();
				val logicalTableSize = {
					if(tpSubject.isURI()) {
						this.mapNodeLogicalTableSize.get(tpSubject).longValue() - 1;
					} else {
					  this.mapNodeLogicalTableSize.get(tpSubject).longValue();
					}
				}
						
				if(mapNodeTriples.contains(tpSubject)) {
					var mappedTriples = mapNodeTriples(tpSubject);
					mappedTriples = mappedTriples ::: List(tp);
					mapNodeTriples += (tpSubject -> mappedTriples);
				} else {
					val mappedTriples = List(tp);
					mapNodeTriples += (tpSubject -> mappedTriples);
				}
				
				val mappedTableSize = {
					if(mapNodeTableSize.contains(tpSubject)) {
					  mapNodeTableSize(tpSubject);
					} else {
					  mapNodeTableSize += (tpSubject -> logicalTableSize);
					  logicalTableSize
					}
				}
				
			}
			
			val triplesSubjects = SPARQLUtility.getSubjects(triples).distinct;
			if(mapNodeTableSize != null && mapNodeTableSize.size() == triplesSubjects.size()) {
			  
				//val mapNodeTableSizeResorted = QueryTranslatorUtility.sortByValue(mapNodeTableSize);
//			  	val nodeTableSizeResorted = mapNodeTableSizeResorted.keySet();
//				for(node <- nodeTableSizeResorted) {
//					result = result ::: mapNodeTriples.get(node);
//				}
				
				val mapNodeTableSizeResorted = mapNodeTableSize.toList sortBy {_._2};
				for(tuple <- mapNodeTableSizeResorted) {
					result = result ::: mapNodeTriples(tuple._1)
				}
			  
				
			} else {
				result = triples;
			}
		}
		
		result;
	}

	def rewriteLeftJoin(opLeftJoin:OpLeftJoin) : Op = {
		
	  
		val exprList = opLeftJoin.getExprs();
		val leftChild = opLeftJoin.getLeft();
		val rightChild = opLeftJoin.getRight();
		val leftChildRewritten = this.rewrite(leftChild);
		val rightChildRewritten = this.rewrite(rightChild);
		
		val result : Op = {
			if(leftChildRewritten.isInstanceOf[OpBGP] && rightChildRewritten.isInstanceOf[OpBGP]) {
				val leftChildRewrittenBGP = leftChildRewritten.asInstanceOf[OpBGP];
				val rightChildRewrittenBGP = rightChildRewritten.asInstanceOf[OpBGP];
			  	val rightBasicPattern = rightChildRewrittenBGP.getPattern();
				val rightBasicPatternSize = rightBasicPattern.size();

				if(rightBasicPatternSize == 1) {
					logger.debug("Optional pattern with only one triple pattern.");
					val leftChildRewrittenTPList = leftChildRewrittenBGP.getPattern().getList().toList;
					
					val rightTp = rightChildRewrittenBGP.getPattern().get(0);
					val rightTpSubject = rightTp.getSubject();
					val rightTpPredicate = rightTp.getPredicate();
					val rightTpObject = rightTp.getObject();
					
					val leftChildTriples = leftChildRewrittenBGP.getPattern().getList().toList;
					val leftChildSubjects = SPARQLUtility.getSubjects(leftChildTriples);
					val leftChildObjects = SPARQLUtility.getObjects(leftChildTriples);
					val needsPhantomTP = leftChildObjects.contains(rightTpSubject) && 
							!leftChildSubjects.contains(rightTpSubject) ;
					
					if(needsPhantomTP) {
					  val phantomSubject = rightTpSubject;
					  val phantomPredicate = RDF.`type`.asNode();
					  val phantomObject = RDFS.Class.asNode();
					  
					  val phantomTriple = new Triple(phantomSubject, phantomPredicate, phantomObject);
					  val newLeftChildRewrittenTPList = leftChildRewrittenTPList ::: List(phantomTriple);
					  val bgpGrouped = SPARQLUtility.groupTriplesBySubject(newLeftChildRewrittenTPList);
					  val triplesGrouped = bgpGrouped.toList;
						
					  val bgpWithPhantomTP = {
							  try {
								  val triplesReordered = this.reorderSTGs(triplesGrouped);
								  val basicPattern = BasicPattern.wrap(triplesReordered);
								  new OpBGP(basicPattern);
							  } catch {
							  	case e:Exception => {
							  		val errorMesssage = "error occured while reodering STG.";
							  		logger.warn(errorMesssage);
							  		//result = bgpGrouped;
							  		val basicPattern = BasicPattern.wrap(triplesGrouped);
							  		new OpBGP(basicPattern);
							  	}
							  }						  
					  }
						
					  val newOpLeftJoin = OpLeftJoin.create(bgpWithPhantomTP, rightChild, exprList);
					  this.rewriteLeftJoin(newOpLeftJoin.asInstanceOf[OpLeftJoin]);
					} else {
					  	if(leftChildSubjects.contains(rightTpSubject) 
					  			&& !leftChildObjects.contains(rightTpObject)) {
						
					  		val rightEtp = new MorphTriple(rightTpSubject, rightTpPredicate
					  				, rightTpObject, true);
					  		val newLeftChildRewrittenTPList = leftChildRewrittenTPList ::: List(rightEtp);
						
					  		//val bgpGrouped = SPARQLUtility.groupBGPBySubject(leftChildRewrittenBGP);
					  		//val triplesGrouped = bgpGrouped.getPattern().getList().toList;
					  		val bgpGrouped = SPARQLUtility.groupTriplesBySubject(newLeftChildRewrittenTPList);
					  		val triplesGrouped = bgpGrouped.toList;
						
					  		try {
					  			val triplesReordered = this.reorderSTGs(triplesGrouped);
					  			val basicPattern = BasicPattern.wrap(triplesReordered);
					  			new OpBGP(basicPattern);
					  		} catch {
					  			case e:Exception => {
					  				val errorMesssage = "error occured while reodering STG.";
					  				logger.warn(errorMesssage);
					  				//result = bgpGrouped;
					  				val basicPattern = BasicPattern.wrap(triplesGrouped);
					  				new OpBGP(basicPattern);
					  			}
					  		}
					  	} else {
					  		OpLeftJoin.create(leftChildRewritten, rightChildRewritten, exprList);
					  	}
					  	//List<Triple> leftChildTriplesList = leftChildRewrittenBGP.getPattern().getList();
					  	//SortedSet<Triple> leftChildTriplesListSorted = new TreeSet<Triple>(leftChildTriplesList);
					}
				} else {
					OpLeftJoin.create(leftChildRewritten, rightChildRewritten, exprList);
				}
			} else {
				OpLeftJoin.create(leftChildRewritten, rightChildRewritten, exprList);				
			}
		  
		}
				
		result
	}
	
	def rewriteBGP(bgp:OpBGP) : Op = {
		val bgpTriples = bgp.getPattern().getList().toList;
		val triplesGrouped = SPARQLUtility.groupTriplesBySubject(bgpTriples);
		
		val basicPattern = {
			if(this.reorderSTG) {
				val triplesReordered = this.reorderSTGs(triplesGrouped.toList);
				BasicPattern.wrap(triplesReordered);
			} else {
				BasicPattern.wrap(triplesGrouped);
			}				  
		}

		val result = new OpBGP(basicPattern);
		result;
	}
	
	def rewriteJoin(opJoin:OpJoin) : Op = {
		val leftChild = opJoin.getLeft();
		val rightChild = opJoin.getRight();
		val leftChildRewritten = this.rewrite(leftChild);
		val rightChildRewritten = this.rewrite(rightChild);
		val result = {
			if(leftChildRewritten.isInstanceOf[OpBGP] && rightChildRewritten.isInstanceOf[OpBGP]) {
				val leftChildRewrittenBGP = leftChildRewritten.asInstanceOf[OpBGP];
				val rightChildRewrittenBGP = rightChildRewritten.asInstanceOf[OpBGP];
//					leftChildRewrittenBGP.getPattern().addAll(rightChildRewrittenBGP.getPattern());
//					result = leftChildRewrittenBGP;
				
				val leftChildRewrittenBGPTriplesList = leftChildRewrittenBGP.getPattern().getList().toList;
				val rightChildRewrittenBGPTriplesList = rightChildRewrittenBGP.getPattern().getList().toList;
				val newTriplesList = leftChildRewrittenBGPTriplesList ::: rightChildRewrittenBGPTriplesList; 
				val newBasicPattern = BasicPattern.wrap(newTriplesList);
				new OpBGP(newBasicPattern);
			} else {
				OpJoin.create(leftChildRewritten, rightChildRewritten);
			}				  
		}
		result;
	  
	}
}