package es.upm.fi.dia.oeg.morph.example;

import static org.junit.Assert.*;

import org.junit.Test;

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVRunnerFactory;

public class ExampleSrilankaTourism {

    
    @Test
    public void testSrilankaTourism2016NaiveBatch() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-naive-batch.morph.properties";
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
    public void testSrilankaTourism2016TransposedBatch() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-naive-transposed-batch.morph.properties";
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
    public void testSrilankaTourism2016TransposedQuery1() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-naive-transposed-query1.morph.properties";
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
    public void testSrilankaTourism2016TransposedQuery2() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-naive-transposed-query2.morph.properties";
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
    public void testSrilankaTourism2016TransposedQuery3() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-naive-transposed-query3.morph.properties";
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
    public void testSrilankaTourism2016ColumnsBatch() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-columns-batch.morph.properties";
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
    public void testSrilankaTourism2016RangeBatch() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-range-batch.morph.properties";
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
    public void testSrilankaTourism2016NaiveQuery1() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-naive-query1.morph.properties";
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
    public void testSrilankaTourism2016ColumnsQuery01() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-columns-query1.morph.properties";
        try {
            MorphCSVRunnerFactory runnerFactory = new MorphCSVRunnerFactory();
            MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
            String status = runner.run();
            System.out.println("Batch process DONE with status = " + status);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Batch process FAILED with status = " + e.getMessage());
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testSrilankaTourism2016ColumnsQuery02() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-columns-query2.morph.properties";
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
    public void testSrilankaTourism2016ColumnsQuery03() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-columns-query3.morph.properties";
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
    public void testSrilankaTourism2016NaiveQuery2() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-naive-query2.morph.properties";
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
    public void testSrilankaTourism2016RangeQuery01() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-range-query1.morph.properties";
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
    public void testSrilankaTourism2016RangeQuery02() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-range-query2.morph.properties";
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
    public void testSrilankaTourism2016RangeQuery03() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-range-query3.morph.properties";
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
    public void testSrilankaTourism2016NaiveQuery3() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-srilanka-tourism";
        String configurationFile = "2016-P21-naive-query3.morph.properties";
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
