package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions._
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.vocabulary.RDF
import es.upm.fi.dia.oeg.morph.base.sql.SQLLogicalTable
import es.upm.fi.dia.oeg.morph.base.sql.SQLJoinTable

class MorphAlphaResult(val alphaSubject:SQLLogicalTable
    , var alphaPredicateObjects:List[MorphAlphaResultPredicateObject]) {
	//alpha result = SQLLogicalTable
	//alpha alphaPredicateObjects = (parent table, predicate URI)
  
	override def toString = {
			(alphaSubject, alphaPredicateObjects).toString
	}  
}

//class MorphAlphaResultSubject(val alphaSubject:SQLLogicalTable)

class MorphAlphaResultPredicateObject(val parentTable:SQLJoinTable, val predicateURI:String)