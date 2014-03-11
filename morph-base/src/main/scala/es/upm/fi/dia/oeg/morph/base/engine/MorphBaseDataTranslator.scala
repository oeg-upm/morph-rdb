package es.upm.fi.dia.oeg.morph.base.engine

import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.base.MorphProperties

abstract class MorphBaseDataTranslator(val md:MorphBaseMappingDocument
    , val materializer:MorphBaseMaterializer, unfolder:MorphBaseUnfolder
    , dataSourceReader:MorphBaseDataSourceReader, connection:Connection
    , properties:MorphProperties ) {
  
	val logger = Logger.getLogger(this.getClass().getName());
	//var properties:ConfigurationProperties=null;
//	var connection:Connection = null;
	
	def processCustomFunctionTransformationExpression(argument:Object ) : Object; 
	
	def translateData(mappingDocument:MorphBaseMappingDocument ) ;
	
	def generateRDFTriples(cm:MorphBaseClassMapping , iQuery:IQuery );
	
	def generateSubjects(cm:MorphBaseClassMapping , iQuery:IQuery) ;
	
	def translateData(triplesMaps:Iterable[MorphBaseClassMapping] );
	
//	def postTranslation() = {this.materializer.postMaterialize}
}