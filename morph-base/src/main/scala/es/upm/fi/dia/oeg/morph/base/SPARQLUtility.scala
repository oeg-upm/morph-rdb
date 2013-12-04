package es.upm.fi.dia.oeg.morph.base

import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.sparql.algebra.Op
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.sparql.algebra.op.OpBGP
import scala.collection.JavaConversions._
import com.hp.hpl.jena.sparql.algebra.op.OpJoin
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin
import com.hp.hpl.jena.sparql.algebra.op.OpUnion
import com.hp.hpl.jena.sparql.algebra.op.OpFilter
import com.hp.hpl.jena.sparql.algebra.op.OpProject
import com.hp.hpl.jena.sparql.algebra.op.OpSlice
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct
import com.hp.hpl.jena.sparql.algebra.op.OpOrder
import scala.collection.mutable.LinkedHashMap
import com.hp.hpl.jena.sparql.core.BasicPattern
import org.apache.log4j.Logger



object SPARQLUtility {
	val logger = Logger.getLogger("SPARQLUtility");
	
	def groupTriplesBySubject(triples:java.util.List[Triple]) : java.util.List[Triple] = {
		//val triples = basicPattern.getList().toList;
		var result:List[Triple] = Nil;
		if(triples == null || triples.size() == 0) {
			result = Nil;
		} else {
			if(triples.size() == 1) {
				result = triples.toList;
			} else {
				var resultAux : Map[Node, List[Triple]] = Map.empty;
				
				var subjectSet:Set[Node] = Set.empty;

				for(tp <- triples) {
					val tpSubject = tp.getSubject();
					
					if(resultAux.contains(tpSubject)) {
						var triplesBySubject = resultAux(tpSubject);
						triplesBySubject = triplesBySubject ::: List(tp)
						resultAux += (tpSubject -> triplesBySubject); 
					} else {
					  	val triplesBySubject:List[Triple] = List(tp);
					  	resultAux += (tpSubject -> triplesBySubject);
					}
				}
				
				for(triplesBySubject <- resultAux.values()) {
					result = result ::: triplesBySubject;
				}
			}
		}
		
		result;
	}  
	
	def groupBGPBySubject(bgp:OpBGP ) : OpBGP  ={
		try {
			val basicPattern = bgp.getPattern();
			var mapTripleHashCode:Map[Integer, List[Triple]] = Map.empty

			for(tp <- basicPattern) {
				val tpSubject = tp.getSubject();

				val tripleSubjectHashCode = new Integer(tpSubject.hashCode());
				
				if(mapTripleHashCode.containsKey(tripleSubjectHashCode)) {
					var triplesByHashCode = mapTripleHashCode(tripleSubjectHashCode);
					triplesByHashCode = triplesByHashCode ::: List(tp);
					mapTripleHashCode += (tripleSubjectHashCode -> triplesByHashCode);
				} else {
					val triplesByHashCode : List[Triple] = List(tp);
					mapTripleHashCode += (tripleSubjectHashCode -> triplesByHashCode);
				}
				
			}
			var triplesReordered:List[Triple] = Nil;
			for(key <- mapTripleHashCode.keySet) {
				val triplesByHashCode = mapTripleHashCode(key);
				triplesReordered = triplesReordered ::: triplesByHashCode;
			}
			
			val basicPattern2 = BasicPattern.wrap(triplesReordered);
			val bgp2 = new OpBGP(basicPattern2);
			return bgp2;			 
		} catch {
		  case e:Exception => {
			val errorMessage = "Error while grouping triples, original triples will be returned.";
			logger.warn(errorMessage);
			return bgp;		    
		  }
		}
	}
		
	def  getSubjects(triples : List[Triple]) : List[Node] = {
		var result:List[Node] = Nil;
		
		if(triples != null) {
			for(triple <- triples) {
			  result = result ::: List(triple.getSubject());
			}
		}

		result;
	}

	def  getObjects(triples : List[Triple]) : List[Node] = {
		var result:List[Node] = Nil;
		
		if(triples != null) {
			for(triple <- triples) {
			  result = result ::: List(triple.getObject());
			}
		}

		result;
	}
	
	def isBlankNode(node:Node) = {
	  val result = {
		  if(node.isBlank()) {
		    true
		  } else {
			  if(node.isVariable()) {
			    val varName = node.getName();
			    if(varName.startsWith("?")) {
			      true
			    } else {
			      false
			    }
			  } else {
			    false
			  }		    
		  } 
	    
	  }
	  result;
	}

	def isNodeInSubjectTriple(node : Node, tp: Triple) : Boolean = {
	  tp.getSubject() == node
	}
	
	def isNodeInSubjectGraph(node : Node, op : Op) : Boolean = {
	  val found = op match {
	    case tp: Triple => {
	      this.isNodeInSubjectTriple(node, tp)
	    }
	    case bgp: OpBGP => {
	      this.isNodeInSubjectBGP(node, bgp.getPattern().toList);
	    }
	    case join: OpJoin => {
	      this.isNodeInSubjectGraphs(node,join.getLeft(), join.getRight());
	    }
	    case leftJoin: OpLeftJoin => {
	      this.isNodeInSubjectGraphs(node,leftJoin.getLeft(), leftJoin.getRight());
	    }	    
	    case union: OpUnion => {
	      this.isNodeInSubjectGraphs(node,union.getLeft(), union.getRight());
	    }
	    case filter: OpFilter=> {
	      this.isNodeInSubjectGraph(node,filter.getSubOp());
	    }
	    case project: OpProject=> {
	      this.isNodeInSubjectGraph(node,project.getSubOp());
	    }	    
	    case slice: OpSlice=> {
	      this.isNodeInSubjectGraph(node,slice.getSubOp());
	    }
	    case distinct: OpDistinct=> {
	      this.isNodeInSubjectGraph(node,distinct.getSubOp());
	    }	    
	    case order: OpOrder=> {
	      this.isNodeInSubjectGraph(node,order.getSubOp());
	    }
	    case _ => false
	  }
	  
	  found;
	}
	
	def isNodeInSubjectBGP(node : Node, bgpList : List[Triple]) : Boolean = {
	  val isInHead = isNodeInSubjectTriple(node, bgpList.head);
	  var found = isInHead;
	  if(!found && !bgpList.tail.isEmpty) {
	    found = isNodeInSubjectBGP(node, bgpList.tail);  
	  }
	  found;
	}
	
	def isNodeInSubjectGraphs(node : Node, opLeft: Op, opRight: Op) : Boolean = {
	  val isInLeft = isNodeInSubjectGraph(node, opLeft);
	  var found = isInLeft;
	  if(!found) {
	    found = isNodeInSubjectGraph(node, opRight);
	  }
	  found;
	}		
}