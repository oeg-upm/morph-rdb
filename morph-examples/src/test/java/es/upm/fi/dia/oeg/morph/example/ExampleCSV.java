package es.upm.fi.dia.oeg.morph.example;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner;
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunnerFactory;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVRunnerFactory;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBProperties;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunnerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVProperties;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVRunner;

public class ExampleCSV {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    //static { PropertyConfigurator.configure("log4j.properties"); }
    String configurationDirectory = System.getProperty("user.dir") + File.separator + "examples-csv";

	@Test
	public void testEdificioHistoricoWeb() {
		MorphCSVProperties properties = new MorphCSVProperties();
		properties.setMappingDocumentFilePath(
				"https://raw.githubusercontent.com/oeg-upm/mappingpedia-engine/master/examples/edificio-historico.r2rml.ttl"
		);
		properties.setOutputFilePath(configurationDirectory + File.separator + "edificio-historico-batch-result-csv-web.nt");
		properties.addCSVFile("https://raw.githubusercontent.com/oeg-upm/mappingpedia-engine/master/examples/edificio-historico.csv");
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
    public void testBarcelonaGTFSWithPropertiesFile() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-gtfs-bcn";
        String configurationFile = "barnaTram-batch-csv.morph.properties";
        try {
            MorphCSVRunnerFactory runnerFactory = new MorphCSVRunnerFactory();
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
    public void testMadridGTFSWithPropertiesFile() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-gtfs";
        String configurationFile = "gtfsmadrid-batch-csv.morph.properties";
        try {
            MorphCSVRunnerFactory runnerFactory = new MorphCSVRunnerFactory();
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
	public void testLinkedFiestasBatchWithPropertiesFile() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples-csv";
		String configurationFile = "linkedfiestas-batch-csv.morph.properties";
		try {
			MorphCSVRunnerFactory runnerFactory = new MorphCSVRunnerFactory();
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
	public void testLinkedFiestasQuery1() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples-csv";
		String configurationFile = "linkedfiestas-q1.morph.properties";
		try {
			MorphCSVRunnerFactory runnerFactory = new MorphCSVRunnerFactory();
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
	public void testLinkedFiestasWithoutPropertiesFile() {
		MorphCSVProperties properties = new MorphCSVProperties();
		properties.setMappingDocumentFilePath(
				"https://raw.githubusercontent.com/oeg-upm/mappingpedia-contents/master/test-mobileage-upm/766351de-9aed-4c20-b8b9-f2c06452de81/linkedfiestas1b.r2rml.ttl"				
		);
		properties.setOutputFilePath(configurationDirectory + File.separator + "linkedfiestas1b.nt");
		properties.addCSVFile("https://raw.githubusercontent.com/fpriyatna/linked-fiestas/master/datasets/fiestas/linkedfiestas1.csv");
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
	public void testSpainNationalWithoutPropertiesFile() {
		MorphCSVProperties properties = new MorphCSVProperties();
		properties.setMappingDocumentFilePath("https://raw.githubusercontent.com/fpriyatna/linked-fiestas/master/datasets/wikipedia/spain-national1.r2rml.ttl");
		properties.setOutputFilePath(configurationDirectory + File.separator + "spain-national1.nt");
		properties.addCSVFile("https://raw.githubusercontent.com/fpriyatna/linked-fiestas/master/datasets/wikipedia/spain-national1.csv");
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
	public void testSpainNationalWithPropertiesFile() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples-csv";
		String configurationFile = "spain-national-batch-csv.morph.properties";
		try {
			MorphCSVRunnerFactory runnerFactory = new MorphCSVRunnerFactory();
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
	public void testSpainInternationalWithoutPropertiesFile() {
		MorphCSVProperties properties = new MorphCSVProperties();
		properties.setMappingDocumentFilePath("https://raw.githubusercontent.com/fpriyatna/linked-fiestas/master/datasets/wikipedia/spain-international1.r2rml.ttl");
		properties.setOutputFilePath(configurationDirectory + File.separator + "spain-international1.nt");
		properties.addCSVFile("https://raw.githubusercontent.com/fpriyatna/linked-fiestas/master/datasets/wikipedia/spain-international1.csv");
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
	public void testSpainInternationalWithPropertiesFile() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples-csv";
		String configurationFile = "spain-international-batch-csv.morph.properties";
		try {
			MorphCSVRunnerFactory runnerFactory = new MorphCSVRunnerFactory();
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
	public void testMadridFestivalsWithoutPropertieSFile() {
		MorphCSVProperties properties = new MorphCSVProperties();
		properties.setMappingDocumentFilePath("https://raw.githubusercontent.com/fpriyatna/linked-fiestas/master/datasets/wikipedia/madrid1.r2rml.ttl");
		properties.setOutputFilePath(configurationDirectory + File.separator + "madrid1.nt");
		properties.addCSVFile("https://raw.githubusercontent.com/fpriyatna/linked-fiestas/master/datasets/wikipedia/madrid1.csv");
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
	public void testMadridFestivalsWithPropertiesFile() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples-csv";
		String configurationFile = "madrid-batch-csv.morph.properties";
		try {
			MorphCSVRunnerFactory runnerFactory = new MorphCSVRunnerFactory();
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
	public void testWikidataFestivalsWithPropertiesFile() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples-csv";
		String configurationFile = "wikidata-batch-csv.morph.properties";
		try {
			MorphCSVRunnerFactory runnerFactory = new MorphCSVRunnerFactory();
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
	public void testEdificioHistoricoLocal() {
		MorphCSVProperties properties = new MorphCSVProperties();
		properties.setMappingDocumentFilePath(configurationDirectory + File.separator + "edificio-historico.r2rml.ttl");
		properties.setOutputFilePath(configurationDirectory + File.separator + "edificio-historico-batch-result-csv-local.nt");
		properties.addCSVFile(configurationDirectory + File.separator + "edificio-historico.csv");
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
	public void testEdificioHistoricoLocalQ1() {
		MorphCSVProperties properties = new MorphCSVProperties();
		properties.setMappingDocumentFilePath(configurationDirectory + File.separator + "edificio-historico.r2rml.ttl");
		properties.setOutputFilePath(configurationDirectory + File.separator + "edificio-historico-batch-result-csv-local-q1.xml");
		properties.addCSVFile(configurationDirectory + File.separator + "edificio-historico.csv");
		properties.setQueryFilePath(configurationDirectory + File.separator + "edificio-historico-q1.rq");
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
	public void testParisParks() {
		MorphCSVProperties properties = new MorphCSVProperties();
		properties.setMappingDocumentFilePath(configurationDirectory + File.separator + "paris-park.r2rml.ttl");
		properties.setOutputFilePath(configurationDirectory + File.separator + "paris-park-result-csv.nt");
		properties.addCSVFile(configurationDirectory + File.separator + "paris-park.csv");
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
	public void testMonumentsWeb() {
		MorphCSVProperties properties = new MorphCSVProperties();
		properties.setMappingDocumentFilePath("https://raw.githubusercontent.com/oeg-upm/morph-rdb/master/morph-examples/examples-csv/monumento.r2rml.ttl");
		properties.setOutputFilePath(configurationDirectory + File.separator + "monumento-batch-result-csv-web.nt");
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
	public void testMonumentsLocal() {
		MorphCSVProperties properties = new MorphCSVProperties();
		properties.setMappingDocumentFilePath(configurationDirectory + File.separator + "monumento.r2rml.ttl");
		properties.setOutputFilePath(configurationDirectory + File.separator + "monumento-batch-result-csv-local.nt");
		properties.addCSVFile(configurationDirectory + File.separator + "monumento.csv");
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
		
		properties.setOutputFilePath(configurationDirectory + File.separator + "farmacia-batch-result-csv.nt");
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
		//properties.setMappingDocumentFilePath("https://raw.githubusercontent.com/oeg-upm/morph-rdb/master/morph-examples/examples-csv/example1-mapping-csv.ttl");
		properties.setMappingDocumentFilePath(configurationDirectory + File.separator + "example1-mapping-csv.ttl");
		 
		
		properties.setOutputFilePath(configurationDirectory + File.separator + "example1-batch-result-csv.nt");
		properties.addCSVFile("https://raw.githubusercontent.com/oeg-upm/morph-rdb/master/morph-examples/examples-csv/Sport.csv");
		properties.addCSVFile("https://raw.githubusercontent.com/oeg-upm/morph-rdb/master/morph-examples/examples-csv/Student.csv");
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
		properties.setMappingDocumentFilePath("https://github.com/oeg-upm/morph-rdb/blob/master/morph-examples/examples-csv/example1-mapping-csv.ttl");
		properties.setOutputFilePath(configurationDirectory + File.separator + "query1result.xml");
		properties.addCSVFile("https://raw.githubusercontent.com/oeg-upm/morph-rdb/master/morph-examples/examples-csv/Sport.csv");
		properties.addCSVFile("https://raw.githubusercontent.com/oeg-upm/morph-rdb/master/morph-examples/examples-csv/Student.csv");
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
    public void testHistoricalBuildingsWithPropertiesFile() {
        String configurationFile = "edificio-historico-batch-csv.morph.properties";
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
    public void testMonumentsWithPropertiesFile() {
        String configurationFile = "monumento-batch-csv.morph.properties";
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

	@Test
	public void testNutrition() {
		MorphCSVProperties properties = new MorphCSVProperties();
		properties.setMappingDocumentFilePath("https://raw.githubusercontent.com/oeg-upm/mappingpedia-contents/master/carrefour/565b0940-93ed-46d3-a651-ee01394b7767/openfoodfactsexample-NPi6.r2rml");
		properties.setOutputFilePath(configurationDirectory + File.separator + "openfoodfactsexample-NPi6.xml");
		properties.addCSVFile("https://raw.githubusercontent.com/oeg-upm/morph-rdb/master/morph-examples/examples-csv/openfoodfactsexample.csv");
		properties.setQueryFilePath("https://raw.githubusercontent.com/oeg-upm/morph-rdb/master/morph-examples/examples-csv/preview.rq");
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


}
