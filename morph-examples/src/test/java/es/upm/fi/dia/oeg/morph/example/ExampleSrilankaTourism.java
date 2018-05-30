package es.upm.fi.dia.oeg.morph.example;

import static org.junit.Assert.*;

import org.junit.Test;

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVRunnerFactory;

public class ExampleSrilankaTourism {

    
    @Test
    public void testSrilankaTourism2016Batch() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-batch.morph.properties";
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
    public void testSrilankaTourism2016Query01() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-query1.morph.properties";
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
    public void testSrilankaTourism2016Query02() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-query2.morph.properties";
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
    public void testSrilankaTourism2016Query03() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-query3.morph.properties";
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
    public void testSrilankaTourism2015PropertiesFile() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2015-P23-batch-csv.morph.properties";
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
    public void testSrilankaTourism2014PropertiesFile() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2014-P21-batch-csv.morph.properties";
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
    public void testSrilankaTourism2013PropertiesFile() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2013-P21-batch-csv.morph.properties";
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
    public void testSrilankaTourism2012PropertiesFile() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2012-P21-batch-csv.morph.properties";
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

}
