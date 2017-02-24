package es.upm.fi.dia.oeg.morph.example;

import static org.junit.Assert.*;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner;
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunnerFactory;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVRunnerFactory;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVRunner;

public class ExampleWithPropertiesFileCSV {
    private Logger logger = LogManager.getLogger(this.getClass());
    //static { PropertyConfigurator.configure("log4j.properties"); }
    String configurationDirectory = System.getProperty("user.dir") + File.separator + "examples-csv";


    
    @Test
    public void testExample1BatchCSV() {
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
    
}
