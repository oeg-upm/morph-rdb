package es.upm.fi.dia.oeg.morph.querytranslator.test;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.obdi.core.ConfigurationProperties;
import es.upm.fi.dia.oeg.obdi.core.engine.IQueryTranslationOptimizer;
import es.upm.fi.dia.oeg.obdi.core.engine.IQueryTranslator;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.QueryTranslationOptimizerFactory;
import es.upm.fi.dia.oeg.obdi.core.sql.IQuery;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLRunner;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLMappingDocument;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.querytranslator.R2RMLQueryTranslator;

public class ExampleWithPropertiesFile {
	private static Logger logger = Logger.getLogger(ExampleWithPropertiesFile.class);
	static { PropertyConfigurator.configure("log4j.properties"); }
	
	public void batch() {
		String configurationDirectory = System.getProperty("user.dir") + "/example";
		String configurationFile = "batch.r2rml.properties";
		try {
			R2RMLRunner runner = new R2RMLRunner(configurationDirectory, configurationFile);
			runner.run();
			logger.info("Batch process DONE------\n\n");
		} catch (Exception e) {
			//e.printStackTrace();
			logger.error("Error : " + e.getMessage());
			logger.info("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

	@Test
	public void sparql01() {
		String configurationDirectory = System.getProperty("user.dir") + "/example";
		String configurationFile = "query01.r2rml.properties";
		try {
			String[] args = {configurationDirectory, configurationFile};
			R2RMLRunner.main(args);
			logger.info("Query process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error : " + e.getMessage());
			logger.info("Query process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}
	
	@Test
	public void sparql02() {
		String configurationDirectory = System.getProperty("user.dir") + "/example";
		String configurationFile = "query02.r2rml.properties";
		try {
			String[] args = {configurationDirectory, configurationFile};
			R2RMLRunner.main(args);
			logger.info("Query process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error : " + e.getMessage());
			logger.info("Query process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

	@Test
	public void sparql03() {
		String configurationDirectory = System.getProperty("user.dir") + "/example";
		String configurationFile = "query03.r2rml.properties";
		try {
			String[] args = {configurationDirectory, configurationFile};
			R2RMLRunner.main(args);
			logger.info("Query process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error : " + e.getMessage());
			logger.info("Query process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}	
	
	@Test
	public void sparql04() {
		String configurationDirectory = System.getProperty("user.dir") + "/example";
		String configurationFile = "query04.r2rml.properties";
		try {
			String[] args = {configurationDirectory, configurationFile};
			R2RMLRunner.main(args);
			logger.info("Query process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error : " + e.getMessage());
			logger.info("Query process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}	
	
	@Test
	public void sparql05() {
		String configurationDirectory = System.getProperty("user.dir") + "/example";
		String configurationFile = "query05.r2rml.properties";
		try {
			String[] args = {configurationDirectory, configurationFile};
			R2RMLRunner.main(args);
			logger.info("Query process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error : " + e.getMessage());
			logger.info("Query process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}
	
}
