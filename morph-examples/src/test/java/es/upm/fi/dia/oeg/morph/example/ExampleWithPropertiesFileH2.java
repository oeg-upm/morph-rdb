package es.upm.fi.dia.oeg.morph.example;

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunnerFactory;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.junit.Assert.assertTrue;

/**
 * Created by fpriyatna on 2016-04-08.
 */
public class ExampleWithPropertiesFileH2 {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    //static { PropertyConfigurator.configure("log4j.properties"); }
    String configurationDirectory = System.getProperty("user.dir") + File.separator + "examples-h2";


    
    @Test
    public void test1BatchH2() {
        String configurationFile = "example1-batch-h2.r2rml.properties";
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
    public void test1Sparql01H2() {
        String configurationFile = "example1-query01-h2.r2rml.properties";
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
    public void test1Sparql02H2() {
        String configurationFile = "example1-query02-h2.r2rml.properties";
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
    public void test1Sparql03H2() {
        String configurationFile = "example1-query03-h2.r2rml.properties";
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
    public void test1Sparql04H2() {
        String configurationFile = "example1-query04-h2.r2rml.properties";
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
    public void test1Sparql05H2() {
        String configurationFile = "example1-query05-h2.r2rml.properties";
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
