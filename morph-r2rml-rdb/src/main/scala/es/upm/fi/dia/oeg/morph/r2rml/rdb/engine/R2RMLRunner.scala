package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.AbstractQueryResultTranslator
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer


class R2RMLRunner(mappingDocument:R2RMLMappingDocument
    , dataSourceReader:MorphBaseDataSourceReader
    , unfolder:R2RMLUnfolder
    , dataTranslator:R2RMLDataTranslator
    , materializer:MorphBaseMaterializer
    , queryTranslator:Option[IQueryTranslator]
    , resultProcessor:Option[AbstractQueryResultTranslator]
    ) extends MorphBaseRunner(mappingDocument
    , dataSourceReader
    , unfolder
    , dataTranslator
    , materializer
    , queryTranslator
    , resultProcessor        
        ) {
  

    
	override val logger = Logger.getLogger(this.getClass());
	

//	override def buildQueryTranslator() = {
//		val r2rmlMD = this.mappingDocument.asInstanceOf[R2RMLMappingDocument];
//		val r2rmlUnfolder = this.unfolder.asInstanceOf[R2RMLUnfolder];
//		
//		this.queryTranslator = MorphRDBQueryTranslator(r2rmlMD, this.conn, r2rmlUnfolder);
//		
//		if(configurationProperties != null) {
//			this.queryTranslator.setConfigurationProperties(configurationProperties);
//			val databaseType = configurationProperties.databaseType;
//			if(databaseType != null && !databaseType.equals("")) {
//				this.queryTranslator.setDatabaseType(databaseType);
//			}			
//		}
//
//		//query translation optimizer
//		val queryTranslationOptimizer = this.buildQueryTranslationOptimizer();
//
//		val eliminateSelfJoin = this.isSelfJoinElimination();
//		queryTranslationOptimizer.setSelfJoinElimination(eliminateSelfJoin);
//
//		val eliminateSubQuery = this.isSubQueryElimination();
//		queryTranslationOptimizer.setSubQueryElimination(eliminateSubQuery);
//
//		val transJoinEliminateSubQuery = this.isTransJoinSubQueryElimination();
//		queryTranslationOptimizer.setTransJoinSubQueryElimination(transJoinEliminateSubQuery);
//
//		val transSTGEliminateSubQuery = this.isTransSTGSubQueryElimination();
//		queryTranslationOptimizer.setTransSTGSubQueryElimination(transSTGEliminateSubQuery);
//
//		val subQueryAsView = this.isSubQueryAsView();
//		queryTranslationOptimizer.setSubQueryAsView(subQueryAsView);
//
//		this.queryTranslator.setOptimizer(queryTranslationOptimizer);
//		logger.debug("query translator = " + this.queryTranslator);
//		
//		//sparql query
//		val queryFilePath = this.configurationProperties.queryFilePath;
//		this.queryTranslator.setSPARQLQueryByFile(queryFilePath);
//	}	
}

object R2RMLRunner {
	val logger = Logger.getLogger("R2RMLRunner");
	
	def apply(properties:MorphProperties ) : R2RMLRunner = {
		val runnerFactory = new R2RMLRDBRunnerFactory();
		//val runner = new R2RMLRunner();
//		runner.loadConfigurationProperties(properties);
		val runner = runnerFactory.createRunner(properties);
		runner.asInstanceOf[R2RMLRunner];
	}
	
	def apply(configurationDirectory:String , configurationFile:String ) : R2RMLRunner = {
		val configurationProperties = 
				MorphProperties.apply(configurationDirectory, configurationFile);
		val runner = R2RMLRunner(configurationProperties)
		runner
	}
	
	def main(args:Array[String]) {
		try {
			if(args == null || args.length == 0 || args.length != 2) {
				logger.info("usage R2RMLRunner propertiesDirectory propertiesFile");
				System.exit(-1);
			}
			
			val configurationDirectory = args(0);
			logger.debug("propertiesDirectory = " + configurationDirectory);
			
			val configurationFile = args(1);
			logger.debug("propertiesFile = " + configurationFile);
			
			val runner = R2RMLRunner(configurationDirectory, configurationFile);
			runner.run();
		} catch {
		  case e:Exception => {
			//e.printStackTrace();
			logger.error("Exception occured: " + e.getMessage());
			throw e;		    
		  }

		}
	}
  
}