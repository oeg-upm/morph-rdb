package es.upm.fi.dia.oeg.morph.example;

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVRunnerFactory;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ExampleEuroStatPopulation {

    
    @Test
    public void testNaiveBatch() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-eurostat";
        String configurationFile = "eurostatpopulation-naive-batch.morph.properties";
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
    public void testColumnsBatch() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-eurostat";
        String configurationFile = "eurostatpopulation-columns-batch.morph.properties";
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
    public void testRangeBatch() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-eurostat";
        String configurationFile = "eurostatpopulation-range-batch.morph.properties";
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
