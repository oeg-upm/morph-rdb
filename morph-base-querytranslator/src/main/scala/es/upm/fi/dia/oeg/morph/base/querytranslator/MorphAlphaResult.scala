package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions._
import java.util.Collection
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.vocabulary.RDF
import es.upm.fi.dia.oeg.morph.base.sql.SQLLogicalTable
import es.upm.fi.dia.oeg.morph.base.sql.SQLJoinTable

class MorphAlphaResult(val alphaSubject:SQLLogicalTable
    , var alphaPredicateObjects:List[SQLJoinTable] , val predicateURI:String ) {
  
	override def toString = {
			(alphaSubject, alphaPredicateObjects, predicateURI).toString
	}  
}