package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import es.upm.fi.dia.oeg.morph.base.ConfigurationProperties
import es.upm.fi.dia.oeg.obdi.core.engine.QueryTranslationOptimizerFactory

class RunnerFactory {

}

object RunnerFactory {
	def createR2RMLRunnerC(configurationDirectory:String , configurationFile:String) : R2RMLRunner = {
		val configurationProperties = 
				ConfigurationProperties.apply(configurationDirectory, configurationFile);
		val r2rmlRunner = RunnerFactory.createR2RMLRunnerC(configurationProperties);
		return r2rmlRunner;
	}

	def createR2RMLRunnerC(configurationProperties:ConfigurationProperties) : R2RMLRunner = {
		val r2rmlRunner = new R2RMLRunner();
		r2rmlRunner.loadConfigurationProperties(configurationProperties);
//		r2rmlRunner.buildQueryTranslator();
		val queryTranslator = r2rmlRunner.getQueryTranslator();
		val queryTranslationOptimizerC = 
				QueryTranslationOptimizerFactory.createQueryTranslationOptimizerC();
		queryTranslator.setOptimizer(queryTranslationOptimizerC);
		r2rmlRunner;
	}

	def createR2RMLRunnerE(configurationDirectory:String, configurationFile:String) : R2RMLRunner = {
		val configurationProperties = 
				ConfigurationProperties.apply(configurationDirectory, configurationFile);
		val r2rmlRunner = RunnerFactory.createR2RMLRunnerE(configurationProperties);
		r2rmlRunner;
	}
	
	def  createR2RMLRunnerE(configurationProperties:ConfigurationProperties) : R2RMLRunner = {
		val r2rmlRunner = new R2RMLRunner();
		r2rmlRunner.loadConfigurationProperties(configurationProperties);
//		r2rmlRunner.buildQueryTranslator();
		val queryTranslator = r2rmlRunner.getQueryTranslator();
		val queryTranslationOptimizerE = 
				QueryTranslationOptimizerFactory.createQueryTranslationOptimizerE();
		queryTranslator.setOptimizer(queryTranslationOptimizerE);
		return r2rmlRunner;
	}

	def createR2RMLRunnerFC(configurationDirectory:String, configurationFile:String) : R2RMLRunner = {
		val configurationProperties = 
				ConfigurationProperties.apply(configurationDirectory, configurationFile);
		val r2rmlRunner = RunnerFactory.createR2RMLRunnerFC(configurationProperties);
		r2rmlRunner;
	}

	def createR2RMLRunnerFC(configurationProperties:ConfigurationProperties ) : R2RMLRunner = {
		val r2rmlRunner = new R2RMLRunner();
		r2rmlRunner.loadConfigurationProperties(configurationProperties);
//		r2rmlRunner.buildQueryTranslator();
		val queryTranslator = r2rmlRunner.getQueryTranslator();
		val queryTranslationOptimizerFC = 
				QueryTranslationOptimizerFactory.createQueryTranslationOptimizerFC();
		queryTranslator.setOptimizer(queryTranslationOptimizerFC);
		r2rmlRunner;
	}
	
	def createR2RMLRunnerFE(configurationDirectory:String, configurationFile:String) : R2RMLRunner = {
		val configurationProperties = 
				ConfigurationProperties.apply(configurationDirectory, configurationFile);
		val r2rmlRunner = RunnerFactory.createR2RMLRunnerFE(configurationProperties);
		r2rmlRunner;
	}

	def createR2RMLRunnerFE(properties:ConfigurationProperties ) : R2RMLRunner = {
		val r2rmlRunner = new R2RMLRunner();
		r2rmlRunner.loadConfigurationProperties(properties);
//		r2rmlRunner.buildQueryTranslator();
		val queryTranslator = r2rmlRunner.getQueryTranslator();
		val queryTranslationOptimizerFE = 
				QueryTranslationOptimizerFactory.createQueryTranslationOptimizerFE();
		queryTranslator.setOptimizer(queryTranslationOptimizerFE);
		return r2rmlRunner;
	}
  
}