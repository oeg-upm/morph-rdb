package es.upm.fi.dia.oeg.morph.base.querytranslator.engine

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.hp.hpl.jena.sparql.algebra.op.OpBGP
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.sparql.algebra.Op
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin
import com.hp.hpl.jena.sparql.algebra.op.OpJoin
import com.hp.hpl.jena.sparql.algebra.op.OpFilter
import com.hp.hpl.jena.sparql.algebra.op.OpUnion
import Zql.ZSelectItem
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLSelectItem
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLUtility
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping


class MorphQueryTranslatorUtility {

}

object MorphQueryTranslatorUtility {

  def mapsIntersection(map1:Map[Node, Set[MorphBaseClassMapping]] , map2:Map[Node, Set[MorphBaseClassMapping]]) 
  : Map[Node, Set[MorphBaseClassMapping]] = {
		  val map1KeySet = map1.keySet;
		  val map2KeySet = map2.keySet;
		  val mapKeySetsIntersection = map1KeySet.intersect(map2KeySet);
		  val map1OnlyKeySet = map1.keySet.diff(mapKeySetsIntersection);
		  val map2OnlyKeySet = map2.keySet.diff(mapKeySetsIntersection);
		  val result1=map1OnlyKeySet.map(map1Key => {
		    (map1Key -> map1(map1Key));
		  })
		  val result2=map2OnlyKeySet.map(map2Key => {
		    (map2Key -> map2(map2Key));
		  })		  
		  val resultIntersection = mapKeySetsIntersection.map(key => {
		    val map1Values = map1(key);
		    val map2Values = map2(key);
		    val mapValuesIntersection = map1Values.intersect(map2Values); 
		    (key -> mapValuesIntersection);
		  })
		  
		  val resultFinal = result1 ++ resultIntersection ++ result2;
		  resultFinal.toMap;
	}
  
  def mapsIntersection(maps:List[Map[Node, Set[MorphBaseClassMapping]]]) 
  : Map[Node, Set[MorphBaseClassMapping]] = {
		  val result : Map[Node, Set[MorphBaseClassMapping]] = {
			  if(maps == null || maps.isEmpty) {
			    Map.empty;
			  } else if(maps.size == 1) {
			    maps(0);
			  } else if(maps.size == 2) {
			    this.mapsIntersection(maps(0), maps(1));
			  } else {
			    val head = maps.head;
			    val tail = maps.tail;
			    val tailIntersection = this.mapsIntersection(tail);
			    this.mapsIntersection(head, tailIntersection);
			  }		    
		  }

		  result
  }
  
//  def mergeTwoMaps(map1:Map[Node, Set[AbstractConceptMapping]] , map2:Map[Node, Set[AbstractConceptMapping]]) 
//  : Map[Node, Set[AbstractConceptMapping]] = {
//	  val maps = List(map1) ::: List(map2);
//	  val result = this.mergeMaps(maps);
//	  result;
//	}
//	
//	def mergeMaps(maps:List[Map[Node, Set[AbstractConceptMapping]]]) 
//	: Map[Node, Set[AbstractConceptMapping]] = {
//		// convert maps to seq, to keep duplicate keys and concat
//		//val merged = Map(A -> Set(2,3)).toSeq ++ Map(A -> Set(4,5)).toSeq
//		// merged: Seq[(Int, Int)] = ArrayBuffer((A,Set(2,3)), (A,Set(4,5)))
//		val merged = maps.flatMap(_.toSeq);
//		
//		// group by key
//		val grouped = merged.groupBy(_._1)
//		// grouped: scala.collection.immutable.Map[Int,Seq[(Int, Int)]] = Map(A -> ArrayBuffer((A,Set(2,3)), (A,Set(4,5))))
//		
//		
//		// remove key from value set and convert to list
//		val cleaned = grouped.mapValues(_.map(_._2).toList.flatten)
//		// cleaned: scala.collection.immutable.Map[Int,List[Int]] = Map(A -> List(2,3,4,5))
//		
//		val cleaned2 = cleaned.map{case (key, value) => (key -> value.toSet)}
//		cleaned2
//		
//	}

	def isTriplePattern(opBGP:OpBGP ) : Boolean = {
	  val triplesSize = opBGP.getPattern().getList().size();
	  val result = { 
	    if(triplesSize == 1) { true; } 
	    else { false; } 
	  }
	  result
	}
	
	def isSTG(triples:List[Triple]) : Boolean = {
	  val result = {
	    if(triples.size() <= 1) { false }
	    else {
	    	val groupedTriples = triples.groupBy(triple => triple.getSubject());
	    	groupedTriples.size == 1;	      
	    }
	  }
	  result
	}
	
	def isSTG(opBGP : OpBGP) : Boolean = {
	  val triples = opBGP.getPattern().getList();
	  this.isSTG(triples.toList);
	}
	
	def getFirstTBEndIndex(triples:java.util.List[Triple] ) = {
		var result = 1;
		for(i <- 1 until triples.size()+1) {
			val sublist = triples.subList(0, i);
			if(this.isSTG(sublist.toList)) {
				result = i;
			}
		}

		result;
	}
	
	def terms(op:Op) : java.util.Collection[Node] = {
	  val result = this.getTerms(op).asJavaCollection;
	  result
	}

	def getTerms(op:Op) : Set[Node] = {
		val result : Set[Node] = {
			op match {
				case bgp:OpBGP => {
					val triples = bgp.getPattern().getList();
					var resultAux : Set[Node] = Set.empty
					for(triple <- triples) {
						val tpSubject = triple.getSubject();
						if(tpSubject.isURI() || tpSubject.isBlank() || tpSubject.isLiteral() || tpSubject.isVariable()) {
							resultAux = resultAux ++ Set(tpSubject);
						}
				
						val tpPredicate = triple.getPredicate();
						if(tpPredicate.isURI() || tpPredicate.isBlank() || tpPredicate.isLiteral() || tpPredicate.isVariable()) {
							resultAux = resultAux ++ Set(tpPredicate);
						}
				
						val tpObject = triple.getObject();
						if(tpObject.isURI() || tpObject.isBlank() || tpObject.isLiteral() || tpObject.isVariable()) {
							resultAux = resultAux ++ Set(tpObject);
						}
					}
					resultAux
				}
				case leftJoin:OpLeftJoin  => {
					val resultLeft = this.getTerms(leftJoin.getLeft());
					val resultRight = this.getTerms(leftJoin.getRight());
					resultLeft ++ resultRight 
				} 
				case opJoin:OpJoin => {
					val resultLeft = this.getTerms(opJoin.getLeft());
					val resultRight = this.getTerms(opJoin.getRight());
					resultLeft ++ resultRight
				} 
				case filter:OpFilter => {
					this.getTerms(filter.getSubOp());
				} 
				case opUnion:OpUnion => {
					val resultLeft = this.getTerms(opUnion.getLeft());
					val resultRight = this.getTerms(opUnion.getRight());
					resultLeft ++ resultRight
				}
				case _ => Set.empty;
			}
		}

		result;
	}

	def generateMappingIdSelectItems(nodes:List[Node], selectItems:List[ZSelectItem]
	, pPrefix:String , dbType:String) : List[ZSelectItem] = {
		val prefix = {
			if(pPrefix == null) {
				"";
			} else {
				if(!pPrefix.endsWith(".")) {
					pPrefix + ".";
				} else {
				  pPrefix
				}	
			}		  
		} 
		
		val result = nodes.map(term => {
			if(term.isVariable()) {
				val mappingsSelectItemsAux = MorphSQLUtility.getSelectItemsMapPrefix(
				    selectItems, term, prefix, dbType);
				mappingsSelectItemsAux.map(mappingsSelectItemAux => {
					val mappingSelectItemAuxAlias = mappingsSelectItemAux.getAlias();
					val newSelectItem = MorphSQLSelectItem.apply(
							mappingSelectItemAuxAlias, prefix, dbType, null);
					newSelectItem;				  
				})
			} else {
			  Nil
			}
		})
		
		result.flatten;
	}
	
	
}