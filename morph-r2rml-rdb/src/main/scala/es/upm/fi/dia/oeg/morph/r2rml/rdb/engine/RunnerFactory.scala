package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.engine.QueryTranslationOptimizerFactory
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner

class RunnerFactory {

}

object RunnerFactory {
	def createR2RMLRunnerC(configurationDirectory:String , configurationFile:String) 
	: MorphBaseRunner = {
		val properties = MorphProperties.apply(configurationDirectory, configurationFile);
		val r2rmlRunner = RunnerFactory.createR2RMLRunnerC(properties);
		return r2rmlRunner;
	}

	def createR2RMLRunnerC(properties:MorphProperties) 
	: MorphBaseRunner = {
		val runnerFactory = new R2RMLRDBRunnerFactory();
		val r2rmlRunner = runnerFactory.createRunner(properties);
		val queryTranslator = r2rmlRunner.queryTranslator
		if(queryTranslator.isDefined) {
			val queryTranslationOptimizerC = 
					QueryTranslationOptimizerFactory.createQueryTranslationOptimizerC();
			queryTranslator.get.optimizer = queryTranslationOptimizerC;		  
		}

		r2rmlRunner;
	}

	def createR2RMLRunnerE(configurationDirectory:String, configurationFile:String) : MorphBaseRunner = {
		val properties = 
				MorphProperties.apply(configurationDirectory, configurationFile);
		val r2rmlRunner = RunnerFactory.createR2RMLRunnerE(properties);
		r2rmlRunner;
	}
	
	def  createR2RMLRunnerE(properties:MorphProperties) : MorphBaseRunner = {
		val runnerFactory = new R2RMLRDBRunnerFactory();
		val r2rmlRunner = runnerFactory.createRunner(properties);
//		r2rmlRunner.buildQueryTranslator();
		val queryTranslator = r2rmlRunner.queryTranslator;
		if(queryTranslator.isDefined) {
			val queryTranslationOptimizerE = 
					QueryTranslationOptimizerFactory.createQueryTranslationOptimizerE();
			queryTranslator.get.optimizer = queryTranslationOptimizerE;  
		}

		return r2rmlRunner;
	}

	def createR2RMLRunnerFC(configurationDirectory:String, configurationFile:String) : MorphBaseRunner = {
		val properties = 
				MorphProperties.apply(configurationDirectory, configurationFile);
		val r2rmlRunner = RunnerFactory.createR2RMLRunnerFC(properties);
		r2rmlRunner;
	}

	def createR2RMLRunnerFC(properties:MorphProperties ) : MorphBaseRunner = {
		val runnerFactory = new R2RMLRDBRunnerFactory();
		val r2rmlRunner = runnerFactory.createRunner(properties);
//		r2rmlRunner.buildQueryTranslator();
		val queryTranslator = r2rmlRunner.queryTranslator;
		if(queryTranslator.isDefined) {
			val queryTranslationOptimizerFC = 
					QueryTranslationOptimizerFactory.createQueryTranslationOptimizerFC();
			queryTranslator.get.optimizer = queryTranslationOptimizerFC;		  
		}

		r2rmlRunner;
	}
	
	def createR2RMLRunnerFE(configurationDirectory:String, configurationFile:String) : MorphBaseRunner = {
		val properties = 
				MorphProperties.apply(configurationDirectory, configurationFile);
		val r2rmlRunner = RunnerFactory.createR2RMLRunnerFE(properties);
		r2rmlRunner;
	}

	def createR2RMLRunnerFE(properties:MorphProperties ) : MorphBaseRunner = {
		val runnerFactory = new R2RMLRDBRunnerFactory();
		val r2rmlRunner = runnerFactory.createRunner(properties);
//		r2rmlRunner.buildQueryTranslator();
		val queryTranslator = r2rmlRunner.queryTranslator;
		if(queryTranslator.isDefined) {
			val queryTranslationOptimizerFE = 
					QueryTranslationOptimizerFactory.createQueryTranslationOptimizerFE();
			queryTranslator.get.optimizer = queryTranslationOptimizerFE;		  
		}

		r2rmlRunner;
	}
  
}