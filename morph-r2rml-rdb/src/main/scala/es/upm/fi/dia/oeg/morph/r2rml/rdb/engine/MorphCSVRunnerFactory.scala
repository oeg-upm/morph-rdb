package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

//import java.util.Properties

import es.upm.fi.dia.oeg.morph.base.{MorphBenchmarking, MorphProperties}
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.AbstractQueryResultTranslator
import java.io.Writer
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument


/**
 * Created by freddy on 14/04/16.
 */
class MorphCSVRunnerFactory extends MorphRDBRunnerFactory {

	override def createRunner(mappingDocument:MorphBaseMappingDocument
    , unfolder:MorphBaseUnfolder
    , dataTranslator:Option[MorphBaseDataTranslator]
    , queryTranslator:Option[IQueryTranslator]
    , resultProcessor:Option[AbstractQueryResultTranslator]
    , outputStream:Writer
    , benchmark: MorphBenchmarking
  ) : MorphBaseRunner = { 
    val morphCSVRunner = new MorphCSVRunner(
      mappingDocument.asInstanceOf[R2RMLMappingDocument]
      , unfolder.asInstanceOf[MorphRDBUnfolder]
      , dataTranslator.asInstanceOf[Option[MorphRDBDataTranslator]]
      , queryTranslator
      , resultProcessor
      , outputStream
      , benchmark)

    morphCSVRunner;
	}

  override def createRunner(configurationDirectory:String , configurationFile:String) 
	: MorphBaseRunner = {
    
		val configurationProperties = MorphCSVProperties.apply(configurationDirectory, configurationFile);
		this.createRunner(configurationProperties);
	}
		
  override def createRunner(properties:MorphProperties):MorphBaseRunner = {
    val runner = super.createRunner(properties);
    val morphProperties = properties.asInstanceOf[MorphCSVProperties];
    if(morphProperties.csvFiles != null) {
      morphProperties.csvFiles.map(csvFile => {
        MorphRDBUtility.loadCSVFile(runner.connection, csvFile, morphProperties.fieldSeparator);
      });
    }
    runner;
  }

}

object MorphCSVRunnerFactory {

}