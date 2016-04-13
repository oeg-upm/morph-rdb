package es.upm.fi.dia.oeg.morph.example;

import static org.junit.Assert.*;

import java.io.File;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunnerFactory;

public class ExampleWithPropertiesFileCSV {
    private static Logger logger = Logger.getLogger(ExampleWithPropertiesFileCSV.class);
    static { PropertyConfigurator.configure("log4j.properties"); }
    String configurationDirectory = System.getProperty("user.dir") + File.separator + "examples-csv";


    
    @Test
    public void test1BatchCSV() {
        String configurationFile = "example1-batch-csv.r2rml.properties";
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
