package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.querytranslator.engine;

import es.upm.fi.dia.oeg.obdi.core.ConfigurationProperties;
import es.upm.fi.dia.oeg.obdi.core.engine.IQueryTranslationOptimizer;
import es.upm.fi.dia.oeg.obdi.core.engine.IQueryTranslator;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.QueryTranslationOptimizerFactory;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLRunner;

public class RunnerFactory {
	public static R2RMLRunner createR2RMLRunnerC(String configurationDirectory, String configurationFile)
			throws Exception {
		ConfigurationProperties configurationProperties = 
				new ConfigurationProperties(configurationDirectory, configurationFile);
		R2RMLRunner r2rmlRunner = RunnerFactory.createR2RMLRunnerC(configurationProperties);
		return r2rmlRunner;
	}

	public static R2RMLRunner createR2RMLRunnerC(ConfigurationProperties configurationProperties)
			throws Exception {
		R2RMLRunner r2rmlRunner = new R2RMLRunner(configurationProperties);
		r2rmlRunner.buildQueryTranslator();
		IQueryTranslator queryTranslator = r2rmlRunner.getQueryTranslator();
		IQueryTranslationOptimizer queryTranslationOptimizerC = 
				QueryTranslationOptimizerFactory.createQueryTranslationOptimizerC();
		queryTranslator.setOptimizer(queryTranslationOptimizerC);
		return r2rmlRunner;
	}

	public static R2RMLRunner createR2RMLRunnerE(String configurationDirectory, String configurationFile)
			throws Exception {
		ConfigurationProperties configurationProperties = 
				new ConfigurationProperties(configurationDirectory, configurationFile);
		R2RMLRunner r2rmlRunner = RunnerFactory.createR2RMLRunnerE(configurationProperties);
		return r2rmlRunner;
	}
	
	public static R2RMLRunner createR2RMLRunnerE(ConfigurationProperties configurationProperties)
			throws Exception {
		R2RMLRunner r2rmlRunner = new R2RMLRunner(configurationProperties);
		r2rmlRunner.buildQueryTranslator();
		IQueryTranslator queryTranslator = r2rmlRunner.getQueryTranslator();
		IQueryTranslationOptimizer queryTranslationOptimizerE = 
				QueryTranslationOptimizerFactory.createQueryTranslationOptimizerE();
		queryTranslator.setOptimizer(queryTranslationOptimizerE);
		return r2rmlRunner;
	}

	public static R2RMLRunner createR2RMLRunnerFC(String configurationDirectory, String configurationFile)
			throws Exception {
		ConfigurationProperties configurationProperties = 
				new ConfigurationProperties(configurationDirectory, configurationFile);
		R2RMLRunner r2rmlRunner = RunnerFactory.createR2RMLRunnerFC(configurationProperties);
		return r2rmlRunner;
	}

	public static R2RMLRunner createR2RMLRunnerFC(ConfigurationProperties configurationProperties)
			throws Exception {
		R2RMLRunner r2rmlRunner = new R2RMLRunner(configurationProperties);
		r2rmlRunner.buildQueryTranslator();
		IQueryTranslator queryTranslator = r2rmlRunner.getQueryTranslator();
		IQueryTranslationOptimizer queryTranslationOptimizerFC = 
				QueryTranslationOptimizerFactory.createQueryTranslationOptimizerFC();
		queryTranslator.setOptimizer(queryTranslationOptimizerFC);
		return r2rmlRunner;
	}
	
	public static R2RMLRunner createR2RMLRunnerFE(String configurationDirectory, String configurationFile)
			throws Exception {
		ConfigurationProperties configurationProperties = 
				new ConfigurationProperties(configurationDirectory, configurationFile);
		R2RMLRunner r2rmlRunner = RunnerFactory.createR2RMLRunnerFE(configurationProperties);
		return r2rmlRunner;
	}

	public static R2RMLRunner createR2RMLRunnerFE(ConfigurationProperties properties)
			throws Exception {
		R2RMLRunner r2rmlRunner = new R2RMLRunner(properties);
		r2rmlRunner.buildQueryTranslator();
		IQueryTranslator queryTranslator = r2rmlRunner.getQueryTranslator();
		IQueryTranslationOptimizer queryTranslationOptimizerFE = 
				QueryTranslationOptimizerFactory.createQueryTranslationOptimizerFE();
		queryTranslator.setOptimizer(queryTranslationOptimizerFE);
		return r2rmlRunner;
	}

}
