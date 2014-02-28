package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractRunner
import es.upm.fi.dia.oeg.morph.base.ConfigurationProperties
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractUnfolder
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLMappingDocument


class R2RMLRunner extends AbstractRunner {
	val logger = Logger.getLogger("R2RMLRunner");
	
	override def createDataTranslator(configurationProperties:ConfigurationProperties ) = {
		this.dataTranslator = new R2RMLDataTranslator(configurationProperties);
		if(this.unfolder != null) {
			this.dataTranslator.setUnfolder(unfolder);
		}
	}

	override def createUnfolder() : AbstractUnfolder  = {
		val unfolder = new R2RMLUnfolder(this.mappingDocument.asInstanceOf[R2RMLMappingDocument]);
		unfolder.setDbType(this.configurationProperties.databaseType);
		return unfolder;
	}

	override def readMappingDocumentFile(mappingDocumentFile:String ) = {
		this.mappingDocument = new R2RMLMappingDocument(mappingDocumentFile, configurationProperties);
		this.mappingDocument.buildMetaData(this.conn);
	}

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
			
			val runner = new R2RMLRunner();
			runner.loadConfigurationfile(configurationDirectory, configurationFile);
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