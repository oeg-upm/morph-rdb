package es.upm.fi.dia.oeg.morph.base.model

import es.upm.fi.dia.oeg.morph.base.sql.MorphTableMetaData
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData
import com.hp.hpl.jena.rdf.model.Resource

abstract class MorphBaseClassMapping(val propertyMappings:Iterable[MorphBasePropertyMapping]) {
  	var id:String =null;
  	var name:String =null;
	var resource:Resource = null;
  	
	def getConceptName():String;
	def getPropertyMappings(propertyURI:String ):Iterable[MorphBasePropertyMapping] ;
	def getPropertyMappings():Iterable[MorphBasePropertyMapping] ;
	def isPossibleInstance(uri:String):Boolean ;
	def getLogicalTable():MorphBaseLogicalTable ;
	def getLogicalTableSize():Long ;
	def getTableMetaData():MorphTableMetaData ;
	def getMappedClassURIs():Iterable[String] ;
	def getSubjectReferencedColumns():List[String] ;
	def buildMetaData(dbMetaData:MorphDatabaseMetaData );
	
	def setId(id:String) = { this.id = id }
	
}