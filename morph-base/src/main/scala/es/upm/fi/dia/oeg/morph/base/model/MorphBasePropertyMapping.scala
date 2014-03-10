package es.upm.fi.dia.oeg.morph.base.model

import com.hp.hpl.jena.rdf.model.Resource

abstract class MorphBasePropertyMapping {
	def getMappedPredicateName(i:Int):String;
	def getMappedPredicateNames():Iterable[String] ;
	var parent:MorphBaseClassMapping=null; 
	var resource:Resource=null;
}