package es.upm.fi.dia.oeg.morph.example;

import static org.junit.Assert.*;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner;
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunnerFactory;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVRunnerFactory;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBProperties;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunnerFactory;
import scala.Option;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVProperties;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVRunner;

public class ExampleCSV {
    private Logger logger = LogManager.getLogger(this.getClass());
    //static { PropertyConfigurator.configure("log4j.properties"); }
    String configurationDirectory = System.getProperty("user.dir") + File.separator + "examples-csv";

	@Test
	public void testEdificioHistorico() {
		MorphCSVProperties properties = new MorphCSVProperties();
		properties.setMappingDocumentFilePath(
				"https://raw.githubusercontent.com/oeg-upm/mappingpedia-contents/master/mobileage/0c8ff8c8-f55d-4e80-aeda-1dcd00879714/edificio-historico.r2rml.ttl"				
		);
		
		properties.setOutputFilePath("edificio-historico-batch-result-csv.nt");
		properties.addCSVFile("http://www.zaragoza.es/api/recurso/turismo/edificio-historico.csv");
		try {
			MorphCSVRunnerFactory runnerFactory = new MorphCSVRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(properties);
			runner.run();
			assertTrue("testBatch done", true);
		} catch(Exception e) {
			e.printStackTrace();
			String errorMessage = "Error occured: " + e.getMessage();
			assertTrue(errorMessage, false);
		}
	}

	@Test
	public void testMonumento() {
		MorphCSVProperties properties = new MorphCSVProperties();
		//properties.setMappingDocumentFilePath("https://raw.githubusercontent.com/oeg-upm/mappingpedia-contents/master/mobileage/3e28ec0f-cc1b-4d59-b173-bff1fc31157d/monumento.r2rml.ttl");
		properties.setMappingDocumentFilePath("https://raw.githubusercontent.com/oeg-upm/mappingpedia-contents/master/zaragoza_test/308f028b-c6e6-4c29-b465-de5b72bf0714/monumento.r2rml.ttl");
		
		
		properties.setOutputFilePath("monumento-batch-result-csv.nt");
		properties.addCSVFile("https://www.zaragoza.es/api/recurso/turismo/monumento.csv");
		try {
			MorphCSVRunnerFactory runnerFactory = new MorphCSVRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(properties);
			runner.run();
			logger.info("bye");
			assertTrue("testBatch done", true);
		} catch(Exception e) {
			e.printStackTrace();
			String errorMessage = "Error occured: " + e.getMessage();
			assertTrue(errorMessage, false);
		}
	}
	
	@Test
	public void testFarmacia() {
		MorphCSVProperties properties = new MorphCSVProperties();
		properties.setMappingDocumentFilePath(
				"https://raw.githubusercontent.com/oeg-upm/mappingpedia-contents/master/mobileage/baf61ea0-390f-4db6-8645-a58135574dd0/farmacia.r2rml.ttl"
		);
		
		properties.setOutputFilePath("farmacia-batch-result-csv.nt");
		properties.addCSVFile("https://www.zaragoza.es/sede/servicio/farmacia.csv");
		properties.setFieldSeparator(";");
		try {
			MorphCSVRunnerFactory runnerFactory = new MorphCSVRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(properties);
			runner.run();
			assertTrue("testBatch done", true);
		} catch(Exception e) {
			e.printStackTrace();
			String errorMessage = "Error occured: " + e.getMessage();
			assertTrue(errorMessage, false);
		}
	}
	
	@Test
	public void testExample1BatchCSVWithoutPropertiesFile() {
		MorphCSVProperties properties = new MorphCSVProperties();
		properties.setMappingDocumentFilePath("https://github.com/oeg-upm/morph-rdb/blob/master/morph-examples/examples-csv/example1-mapping-csv.ttl");
		properties.setOutputFilePath("example1-batch-result-csv.nt");
		properties.addCSVFile("https://github.com/oeg-upm/mappingpedia-contents/blob/master/mappingpedia-testuser/1839e06a-c0a4-4bd0-ab4e-bb7d805ebb42/Sport.csv");
		properties.addCSVFile("https://github.com/oeg-upm/mappingpedia-contents/blob/master/mappingpedia-testuser/1839e06a-c0a4-4bd0-ab4e-bb7d805ebb42/Student.csv");
		try {
			MorphCSVRunnerFactory runnerFactory = new MorphCSVRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(properties);
			runner.run();
			assertTrue("testBatch done", true);
		} catch(Exception e) {
			e.printStackTrace();
			String errorMessage = "Error occured: " + e.getMessage();
			assertTrue(errorMessage, false);
		}
	}
	
	@Test
	public void testQuery1CSVWithoutPropertiesFile() {
		MorphCSVProperties properties = new MorphCSVProperties();
		properties.setMappingDocumentFilePath("https://raw.githubusercontent.com/oeg-upm/mappingpedia-contents/master/mappingpedia-testuser/1839e06a-c0a4-4bd0-ab4e-bb7d805ebb42/example1-mapping-csv.ttl");
		properties.setOutputFilePath("query1result.xml");
		properties.addCSVFile("https://raw.githubusercontent.com/oeg-upm/mappingpedia-contents/master/mappingpedia-testuser/1839e06a-c0a4-4bd0-ab4e-bb7d805ebb42/Sport.csv");
		properties.addCSVFile("https://raw.githubusercontent.com/oeg-upm/mappingpedia-contents/master/mappingpedia-testuser/1839e06a-c0a4-4bd0-ab4e-bb7d805ebb42/Student.csv");
		//properties.setQueryFilePath("https://github.com/oeg-upm/mappingpedia-contents/blob/master/mappingpedia-testuser/1839e06a-c0a4-4bd0-ab4e-bb7d805ebb42/example1-query01.rq");
		properties.setQueryFilePath("https://raw.githubusercontent.com/oeg-upm/mappingpedia-contents/master/mappingpedia-testuser/1839e06a-c0a4-4bd0-ab4e-bb7d805ebb42/example1-query01.rq");
		
		try {
			MorphCSVRunnerFactory runnerFactory = new MorphCSVRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(properties);
			runner.run();
			assertTrue("test query 1 done", true);
		} catch(Exception e) {
			e.printStackTrace();
			String errorMessage = "Error occured: " + e.getMessage();
			assertTrue(errorMessage, false);
		}
	}
	
    @Test
    public void testExample1BatchCSVWithPropertiesFile() {
        String configurationFile = "example1-batch-csv.morph.properties";
        try {
        	MorphBaseRunnerFactory runnerFactory = new MorphCSVRunnerFactory();
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
    public void testExample1Sparql01CSV() {
        String configurationFile = "example1-query01-csv.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphCSVRunner.main(args);
            System.out.println("Query process DONE------\n\n");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------\n\n");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testExample1Sparql02CSV() {
        String configurationFile = "example1-query02-csv.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphCSVRunner.main(args);
            System.out.println("Query process DONE------\n\n");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------\n\n");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testExample1Sparql03CSV() {
        String configurationFile = "example1-query03-csv.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphCSVRunner.main(args);
            System.out.println("Query process DONE------\n\n");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------\n\n");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testExample1Sparql04CSV() {
        String configurationFile = "example1-query04-csv.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphCSVRunner.main(args);
            System.out.println("Query process DONE------\n\n");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------\n\n");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testExample1Sparql05CSV() {
        String configurationFile = "example1-query05-csv.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphCSVRunner.main(args);
            System.out.println("Query process DONE------\n\n");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------\n\n");
            assertTrue(e.getMessage(), false);
        }
    }
    
    @Test
    public void testExample1Sparql06CSV() {
        String configurationFile = "example1-query06-csv.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphCSVRunner.main(args);
            System.out.println("Query process DONE------\n\n");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------\n\n");
            assertTrue(e.getMessage(), false);
        }
    }
    
}
