package es.upm.fi.dia.oeg.morph.example;

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner;
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunnerFactory;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVProperties;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVRunnerFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.junit.Assert.assertTrue;

public class ExampleEPW {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    //static { PropertyConfigurator.configure("log4j.properties"); }
    String configurationDirectory = System.getProperty("user.dir") + File.separator + "examples-epw";

	@Test
	public void testMadridEPW_Batch() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples-epw";
		String configurationFile = "MadridEPWExample-batch.morph.properties";
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
	public void testMadridEPW_Q01() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples-epw";
		String configurationFile = "MadridEPWExample-q01.morph.properties";
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
	public void testMadridEPW_Q02() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples-epw";
		String configurationFile = "MadridEPWExample-q02.morph.properties";
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
	public void testMadridEPW_Q03() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples-epw";
		String configurationFile = "MadridEPWExample-q03.morph.properties";
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
	public void testAthensEPW_Batch() {
		String configurationDirectory = System.getProperty("user.dir") + "/examples-epw";
		String configurationFile = "AthensEPWExample-batch.morph.properties";
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
    public void testAthensEPW_Q01() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-epw";
        String configurationFile = "AthensEPWExample-q01.morph.properties";
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
    public void testAthensEPW_Q02() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-epw";
        String configurationFile = "AthensEPWExample-q02.morph.properties";
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
    public void testAthensEPW_Q03() {
        String configurationDirectory = System.getProperty("user.dir") + "/examples-epw";
        String configurationFile = "AthensEPWExample-q03.morph.properties";
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
