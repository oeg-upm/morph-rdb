package es.upm.fi.dia.oeg.morph.example;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunnerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExampleWithPropertiesFileMySQL {
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	//static { PropertyConfigurator.configure("log4j.properties"); }
	String configurationDirectory = System.getProperty("user.dir") + File.separator + "examples-mysql";

	@Test
	public void testExample1BatchMySQL() {
		String configurationFile = "example1-batch-mysql.morph.properties";
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			logger.info("Batch process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			logger.info("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

	public void testMonumentsBatchMySQL() {
		String configurationFile = "monuments-batch-mysql.morph.properties";
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			logger.info("Batch process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			logger.info("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

	public void testHistoricalBuildingsBatchMySQL() {
		String configurationFile = "edificiohistoricos-batch-mysql.morph.properties";
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			logger.info("Batch process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			logger.info("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

	@Test
	public void testExample1Sparql01MySQL() {
		//2 instances
		String configurationFile = "example1-query01-mysql.morph.properties";
		try {
			String[] args = {configurationDirectory, configurationFile};
			MorphRDBRunner.main(args);
			System.out.println("Query process DONE------");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error : " + e.getMessage());
			System.out.println("Query process FAILED------");
			assertTrue(e.getMessage(), false);
		}
	}


	@Test
	public void testExample1Sparql02MySQL() {
		//4 instances
		String configurationFile = "example1-query02-mysql.morph.properties";
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
	public void testExample1Sparql03MySQL() {
		//4 instances
		String configurationFile = "example1-query03-mysql.morph.properties";
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
	public void testExample1Sparql04MySQL() {
		//4 instances
		String configurationFile = "example1-query04-mysql.morph.properties";
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
	public void testExample1Sparql05MySQL() {
		//1 instance
		String configurationFile = "example1-query05-mysql.morph.properties";
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

	public void testExample1Sparql06MySQL() {
		String configurationFile = "example1-query06-mysql.morph.properties";
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
