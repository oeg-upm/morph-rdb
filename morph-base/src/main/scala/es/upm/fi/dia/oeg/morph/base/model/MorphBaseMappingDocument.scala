package es.upm.fi.dia.oeg.morph.base.model

import scala.collection.JavaConversions._
import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData
import org.slf4j.LoggerFactory

//import org.apache.log4j.Logger

abstract class MorphBaseMappingDocument(val classMappings:Iterable[MorphBaseClassMapping]) {
	//val logger = LogManager.getLogger(this.getClass);
	val logger = LoggerFactory.getLogger(this.getClass());
	
	
	var mappingDocumentPrefixMap:Map[String, String] = Map.empty ;
	var id:String =null;
	var name:String =null;
	var purpose:String =null;
	//var configurationProperties:ConfigurationProperties =null;
	var dbMetaData:Option[MorphDatabaseMetaData] = None;
	var mappingDocumentPath:String = null;
	
	def buildMetaData(connection:Connection, databaseName:String
	    , databaseType:String);
	
	def getMappedClasses() : Iterable[String] =
		this.classMappings.map(_.getConceptName)
	
	def getClassMappingsByClassURI(classURI:String) =
		this.classMappings.filter(_.getMappedClassURIs.exists(_.equals(classURI)))
	
	def getMappedProperties() : Iterable[String];
	
	def getClassMappingByPropertyUri(propertyUri:String) : Iterable[MorphBaseClassMapping] =
	  this.classMappings.filter(_.getPropertyMappings(propertyUri).nonEmpty)

	def getClassMappingByPropertyURIs(propertyURIs:Iterable[String]) 
	: Iterable[MorphBaseClassMapping] =
		this.classMappings.filter(cm =>
			propertyURIs.toSet.subsetOf(cm.propertyMappings.flatMap(_.getMappedPredicateNames).toSet)
		)
	
	def getPropertyMappingsByPropertyURI(propertyURI:String ) 
	: Iterable[MorphBasePropertyMapping] =
	  this.classMappings.flatMap(_.getPropertyMappings(propertyURI))
	
	def getPossibleRange(predicateURI:String , cm:MorphBaseClassMapping ):Iterable[MorphBaseClassMapping];
	
	def getPossibleRange(predicateURI:String ):Iterable[MorphBaseClassMapping] ;
	
	def getPossibleRange(pm:MorphBasePropertyMapping ):Iterable[MorphBaseClassMapping] ;
	
	def getClassMappingsByInstanceTemplate(templateValue:String) : Iterable[MorphBaseClassMapping];
	
	def getClassMappingsByInstanceURI(instanceURI:String):Iterable[MorphBaseClassMapping];

}