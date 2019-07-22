package es.upm.fi.dia.oeg.morph.example;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunnerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExampleWithPropertiesFilePostgreSQL {
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	//static { PropertyConfigurator.configure("log4j.properties"); }

	@Test
	public void testBatchPostgreSQL() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples-postgresql";
		String configurationFile = "batch-postgresql.morph.properties";
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			logger.info("Batch process DONE------\n\n");
		} catch (Exception e) {
			//e.printStackTrace();
			logger.info("Error : " + e.getMessage());
			logger.info("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

	@Test
	public void testSparql01PostgreSQL() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples-postgresql";
		String configurationFile = "query01postgresql.morph.properties";
		try {
			String[] args = {configurationDirectory, configurationFile};
			MorphRDBRunner.main(args);
			System.out.println("Query process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error : " + e.getMessage());
			System.out.println("Query process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}


	@Test
	public void testSparql02PostgreSQL() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples-postgresql";
		String configurationFile = "query02postgresql.morph.properties";
		try {
			String[] args = {configurationDirectory, configurationFile};
			MorphRDBRunner.main(args);
			System.out.println("Query process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error : " + e.getMessage());
			System.out.println("Query process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}
	
	@Test
	public void testSparql03PostgreSQL() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples-postgresql";
		String configurationFile = "query03postgresql.morph.properties";
		try {
			String[] args = {configurationDirectory, configurationFile};
			MorphRDBRunner.main(args);
			System.out.println("Query process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error : " + e.getMessage());
			System.out.println("Query process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

	@Test
	public void testSparql04PostgreSQL() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples-postgresql";
		String configurationFile = "query04postgresql.morph.properties";
		try {
			String[] args = {configurationDirectory, configurationFile};
			MorphRDBRunner.main(args);
			System.out.println("Query process DONE------\n\n");
		} catch (Exception e) {
			//e.printStackTrace();
			System.out.println("Error : " + e.getMessage());
			System.out.println("Query process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}
	
	@Test
	public void testSparql05PostgreSQL() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples-postgresql";
		String configurationFile = "query05postgresql.morph.properties";
		try {
			String[] args = {configurationDirectory, configurationFile};
			MorphRDBRunner.main(args);
			System.out.println("Query process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error : " + e.getMessage());
			System.out.println("Query process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

	@Test
	public void testSparql06PostgreSQL() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples-postgresql";
		String configurationFile = "query06postgresql.morph.properties";
		try {
			String[] args = {configurationDirectory, configurationFile};
			MorphRDBRunner.main(args);
			System.out.println("Query process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error : " + e.getMessage());
			System.out.println("Query process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

	@Test
	public void testAACTSparql01() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples-postgresql";
		String configurationFile = "aact-query1.r2rml.properties.txt";
		try {
			String[] args = {configurationDirectory, configurationFile};
			MorphRDBRunner.main(args);
			System.out.println("Query process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error : " + e.getMessage());
			System.out.println("Query process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}
}
