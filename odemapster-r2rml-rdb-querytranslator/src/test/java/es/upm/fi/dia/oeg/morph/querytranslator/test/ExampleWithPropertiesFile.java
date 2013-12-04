package es.upm.fi.dia.oeg.morph.querytranslator.test;

import static org.junit.Assert.assertTrue;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLRunner;

public class ExampleWithPropertiesFile {
	private static Logger logger = Logger.getLogger(ExampleWithPropertiesFile.class);
	static { PropertyConfigurator.configure("log4j.properties"); }
	
	@Test
	public void testBatchMySQL() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
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
	public void testBatchPostgreSQL() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "batch-postgresql.r2rml.properties";
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
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
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
	public void testSparql01PostgreSQL() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "query01postgresql.r2rml.properties";
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
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
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
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
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
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
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
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
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

	@Test
	public void sparql06() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "query06.r2rml.properties";
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
	public void sparql08() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "query08.r2rml.properties";
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
	public void sparql09() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "query09.r2rml.properties";
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
	public void sparql10() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "query10.r2rml.properties";
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
