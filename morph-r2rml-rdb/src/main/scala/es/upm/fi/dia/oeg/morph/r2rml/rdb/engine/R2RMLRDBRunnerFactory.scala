package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunnerFactory
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.AbstractQueryResultTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator

class R2RMLRDBRunnerFactory extends MorphBaseRunnerFactory{
  
	override def makeRunner(mappingDocument:MorphBaseMappingDocument
    , dataSourceReader:MorphBaseDataSourceReader
    , unfolder:MorphBaseUnfolder
    , dataTranslator :MorphBaseDataTranslator
    , materializer : MorphBaseMaterializer
    , queryTranslator:Option[IQueryTranslator]
    , resultProcessor:Option[AbstractQueryResultTranslator]
    ) : R2RMLRunner = { 
	  new R2RMLRunner(mappingDocument.asInstanceOf[R2RMLMappingDocument]
    , dataSourceReader
    , unfolder.asInstanceOf[R2RMLUnfolder]
    , dataTranslator.asInstanceOf[R2RMLDataTranslator]
    , materializer
    , queryTranslator
    , resultProcessor)
	}
	
	override def readMappingDocumentFile(mappingDocumentFile:String
	    ,props:MorphProperties, connection:Connection ) 
	: MorphBaseMappingDocument = {
		val mappingDocument = R2RMLMappingDocument(mappingDocumentFile, props
		    , connection);
		mappingDocument
	}
	
	override def createUnfolder(md:MorphBaseMappingDocument, dbType:String):R2RMLUnfolder = {
		val unfolder = new R2RMLUnfolder(md.asInstanceOf[R2RMLMappingDocument]);
		unfolder.dbType = dbType
		unfolder;	  
	}

	override def createDataTranslator(mappingDocument:MorphBaseMappingDocument
	    , materializer:MorphBaseMaterializer, unfolder:MorphBaseUnfolder
	    , dataSourceReader:MorphBaseDataSourceReader
	    , connection:Connection, properties:MorphProperties)
	:MorphBaseDataTranslator = {
			new R2RMLDataTranslator(mappingDocument.asInstanceOf[R2RMLMappingDocument]
			, materializer , unfolder.asInstanceOf[R2RMLUnfolder]
			, dataSourceReader.asInstanceOf[RDBReader] , connection, properties);	  
	}
}