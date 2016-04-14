package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import java.util.Properties

import es.upm.fi.dia.oeg.morph.base.MorphProperties
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
class MorphCVSRunnerFactory extends MorphRDBRunnerFactory {

	override def createRunner(mappingDocument:MorphBaseMappingDocument
    , unfolder:MorphBaseUnfolder
    , dataTranslator:Option[MorphBaseDataTranslator]
    , queryTranslator:Option[IQueryTranslator]
    , resultProcessor:Option[AbstractQueryResultTranslator]
    , outputStream:Writer
  ) : MorphBaseRunner = { 
    val morphCVSRunner = new MorphCVSRunner(
      mappingDocument.asInstanceOf[R2RMLMappingDocument]
      , unfolder.asInstanceOf[MorphRDBUnfolder]
      , dataTranslator.asInstanceOf[Option[MorphRDBDataTranslator]]
      , queryTranslator
      , resultProcessor
      , outputStream)

    morphCVSRunner;
	}

  override def createRunner(properties:Properties):MorphBaseRunner = {
    val runner = super.createRunner(properties);
    val morphProperties = properties.asInstanceOf[MorphProperties];
    if(morphProperties.csvFiles.isDefined) {
      morphProperties.csvFiles.get.map(csvFile => {
        MorphRDBUtility.loadCSVFile(runner.connection, csvFile);
      });
    }
    runner;
  }

}

object MorphCVSRunnerFactory {

}