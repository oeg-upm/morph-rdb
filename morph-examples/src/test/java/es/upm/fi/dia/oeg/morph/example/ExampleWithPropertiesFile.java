package es.upm.fi.dia.oeg.morph.example;

import static org.junit.Assert.*;

import org.junit.Test;

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunnerFactory;

public class ExampleWithPropertiesFile {

	@Test
	public void testBatchOracle() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "batch-oracle.r2rml.properties";
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			System.out.println("Batch process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}



	@Test
	public void testBatchPostgreSQL() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "batch-postgresql.r2rml.properties";
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			System.out.println("Batch process DONE------\n\n");
		} catch (Exception e) {
			//e.printStackTrace();
			System.out.println("Error : " + e.getMessage());
			System.out.println("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

	@Test
	public void testSparql01PostgreSQL() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "query01postgresql.r2rml.properties";
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
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "query02postgresql.r2rml.properties";
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
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "query03postgresql.r2rml.properties";
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
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "query04postgresql.r2rml.properties";
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
	public void testSparql05PostgreSQL() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "query05postgresql.r2rml.properties";
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
	public void sparql06() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "query06.r2rml.properties";
		try {
			String[] args = {configurationDirectory, configurationFile};
			MorphRDBRunner.main(args);
			System.out.println("Query process DONE------");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error : " + e.getMessage());
			System.out.println("Query process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}	

	@Test
	public void sparql07() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "query07.r2rml.properties";
		try {
			String[] args = {configurationDirectory, configurationFile};
			MorphRDBRunner.main(args);
			System.out.println("Query process DONE------");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error : " + e.getMessage());
			System.out.println("Query process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}	

	@Test
	public void sparql08() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "query08.r2rml.properties";
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
	public void sparql09() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "query09.r2rml.properties";
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

	public void sparql10() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "query10.r2rml.properties";
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
	public void testExampleShopping() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "example_shopping.r2rml.properties";
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			System.out.println("Batch process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

	@Test
	public void testExampleUniversity() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "example_university.r2rml.properties";
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			System.out.println("Batch process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

	@Test
	public void testDODGAWithPropertiesFile() {
		//String configurationDirectory = "C:/Users/fpriyatna/Documents/dodga/iasoft/morph-files";
		String configurationDirectory = "/home/fpriyatna/Documentos/dodga/iasoft/morph-files";
		String configurationFile = "pproc.r2rml.properties";
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			System.out.println("Batch process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

	@Test
	public void testExamplePatientTP() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "example_patientTP.r2rml.properties";
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			System.out.println("Batch process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}
	
	@Test
	public void testExamplePatientSTG() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "example_patientSTG.r2rml.properties";
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			System.out.println("Batch process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

	@Test
	public void testExamplePatientOSTG() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "example_patientOSTG.r2rml.properties";
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			System.out.println("Batch process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}
	
	@Test
	public void testExamplePatientBGP() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "example_patientBGP.r2rml.properties";
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			System.out.println("Batch process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}
	
	@Test
	public void testExamplePatientPT() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "example_patientPT.r2rml.properties";
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			System.out.println("Batch process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

	@Test
	public void testBatchPostgreSQL2() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "batch-postgresql2.r2rml.properties";
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			System.out.println("Batch process DONE------\n\n");
		} catch (Exception e) {
			//e.printStackTrace();
			System.out.println("Error : " + e.getMessage());
			System.out.println("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

	@Test
	public void sparql11PostgreSQL() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "query11postgresql2.r2rml.properties";
		try {
			String[] args = {configurationDirectory, configurationFile};
			MorphRDBRunner.main(args);
			System.out.println("Query process DONE------");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error : " + e.getMessage());
			System.out.println("Query process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

	@Test
	public void sparql12PostgreSQL() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples";
		String configurationFile = "query12postgresql2.r2rml.properties";
		try {
			String[] args = {configurationDirectory, configurationFile};
			MorphRDBRunner.main(args);
			System.out.println("Query process DONE------");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error : " + e.getMessage());
			System.out.println("Query process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

	@Test
	public void testNandana() {
		String configurationDirectory = System.getProperty("user.dir") + "/nandana";
		String configurationFile = "config.properties";
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			System.out.println("Batch process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}
	
	@Test
	public void testNandana2() {
		String configurationDirectory = System.getProperty("user.dir") + "/nandana";
		String configurationFile = "config.properties";
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			System.out.println("Batch process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}
	
	@Test
	public void issue467() {
		String configurationDirectory = System.getProperty("user.dir") + "/issue467";
		String configurationFile = "config.properties.txt";
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			System.out.println("Batch process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

}
