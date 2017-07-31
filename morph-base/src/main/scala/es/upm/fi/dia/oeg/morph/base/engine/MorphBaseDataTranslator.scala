package es.upm.fi.dia.oeg.morph.base.engine

import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
//import com.hp.hpl.jena.rdf.model.Property
import org.apache.jena.rdf.model.Property;
//import com.hp.hpl.jena.rdf.model.Resource
import org.apache.jena.rdf.model.Resource;
import org.slf4j.LoggerFactory

abstract class MorphBaseDataTranslator(val md:MorphBaseMappingDocument
    , val materializer:MorphBaseMaterializer, unfolder:MorphBaseUnfolder
    , val dataSourceReader:MorphBaseDataSourceReader, connection:Connection
    , properties:MorphProperties ) {
  
	//val logger = LogManager.getLogger(this.getClass);
	val logger = LoggerFactory.getLogger(this.getClass());

	  		
	//var properties:ConfigurationProperties=null;
//	var connection:Connection = null;
	
	def processCustomFunctionTransformationExpression(argument:Object ) : Object; 
	
	def translateData(mappingDocument:MorphBaseMappingDocument ) ;
	
	def translateData(triplesMaps:Iterable[MorphBaseClassMapping] );
	
	def translateData(triplesMap:MorphBaseClassMapping);
	
	def generateRDFTriples(cm:MorphBaseClassMapping , iQuery:IQuery );
	
	def generateSubjects(cm:MorphBaseClassMapping , iQuery:IQuery) ;
	
	def getDataSourceReader = this.dataSourceReader;
	
	def createResource(originalIRI:String) : Resource = {
	  val resourceIRI = this.createIRI(originalIRI);
	  this.materializer.model.createResource(resourceIRI);
	}
	
	def createProperty(originalIRI:String) : Property = {
	  val propertyIRI = this.createIRI(originalIRI);
	  this.materializer.model.createProperty(propertyIRI);
	}
		
	private def createIRI(originalIRI:String) : String = {
	    var resultIRI = originalIRI;
	    try {
			resultIRI = GeneralUtility.encodeURI(resultIRI
			    , properties.mapURIEncodingChars, properties.uriTransformationOperation);
			if(this.properties != null) {
				if(this.properties.encodeUnsafeChars) {
				  resultIRI = GeneralUtility.encodeUnsafeChars(resultIRI);
				}
				
				if(this.properties.encodeReservedChars) {
					resultIRI = GeneralUtility.encodeReservedChars(resultIRI);
				}
			}
			resultIRI;
			//this.materializer.model.createResource(resultIRI);
		} catch {
			case e:Exception => {
				logger.warn("Error translating object uri value : " + resultIRI);
				throw e
			}
		}
	}
	
//	def postTranslation() = {this.materializer.postMaterialize}
}