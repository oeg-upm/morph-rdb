package es.upm.fi.dia.oeg.morph.example;

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunnerFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.junit.Assert.assertTrue;


public class ExampleGTFSMySQL {
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	//static { PropertyConfigurator.configure("log4j.properties"); }
	String configurationDirectory = System.getProperty("user.dir") + File.separator + "examples-gtfs-mysql";

    @Test
    public void testGTFS1_batch_mysql() {
		String configurationFile = "gtfs1-batch-mysql.morph.properties";
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			logger.info("Batch process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			logger.info("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}

    @Test
    public void testGTFS5_batch_mysql() {
        String configurationFile = "gtfs5-batch-mysql.morph.properties";
        try {
            MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
            MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
            runner.run();
            logger.info("Batch process DONE------\n\n");
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Batch process FAILED------\n\n");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS10_batch_mysql() {
        String configurationFile = "gtfs10-batch-mysql.morph.properties";
        try {
            MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
            MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
            runner.run();
            logger.info("Batch process DONE------\n\n");
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Batch process FAILED------\n\n");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS1_q01_mysql() {
        //1 instances
        String configurationFile = "gtfs1-q01-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS1_q11_mysql() {
        //48 instances
        String configurationFile = "gtfs1-q11-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS1_q12_mysql() {
        //6 instances
        String configurationFile = "gtfs1-q12-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS1_q13_mysql() {
        //24 instances
        String configurationFile = "gtfs1-q13-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS1_q14_mysql() {
        //20 instances
        String configurationFile = "gtfs1-q14-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS1_q15_mysql() {
        //26 instances
        String configurationFile = "gtfs1-q15-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS1_q16_mysql() {
        //2 instances
        String configurationFile = "gtfs1-q16-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS1_q17_mysql() {
        //2 instances
        String configurationFile = "gtfs1-q17-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS1_q18_mysql() {
        //2 instances
        String configurationFile = "gtfs1-q18-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS5_q1_mysql() {
        //2 instances
        String configurationFile = "gtfs5-q1-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS10_q1_mysql() {
        //2 instances
        String configurationFile = "gtfs10-q1-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }


    @Test
    public void testGTFS1_q02_mysql() {
        //117 instances
        String configurationFile = "gtfs1-q02-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS5_q2_mysql() {
        //2 instances
        String configurationFile = "gtfs5-q2-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS10_q2_mysql() {
        //2 instances
        String configurationFile = "gtfs10-q2-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS1_q03_mysql() {
        //1 instances
        String configurationFile = "gtfs1-q03-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS5_q3_mysql() {
        //1 instances
        String configurationFile = "gtfs5-q3-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS10_q3_mysql() {
        //2 instances
        String configurationFile = "gtfs10-q3-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

	@Test
	public void testGTFS1_q04_mysql() {
		//13 instances
		String configurationFile = "gtfs1-q04-mysql.morph.properties";
		try {
			String[] args = {configurationDirectory, configurationFile};
			MorphRDBRunner.main(args);
			System.out.println("Query process DONE------");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error : " + e.getMessage());
			System.out.println("Query process FAILED------");
			assertTrue(e.getMessage(), false);
		}
	}

    @Test
    public void testGTFS5_q4_mysql() {
        //13 instances
        String configurationFile = "gtfs5-q4-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS10_q4_mysql() {
        //2 instances
        String configurationFile = "gtfs10-q4-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS1_q05_mysql() {
        //2 instances
        String configurationFile = "gtfs1-q05-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS10_q5_mysql() {
        //2 instances
        String configurationFile = "gtfs10-q5-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS5_q5_mysql() {
        //2 instances
        String configurationFile = "gtfs5-q5-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS1_q06_mysql() {
        //1 instance
        String configurationFile = "gtfs1-q06-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS5_q6_mysql() {
        //2 instances
        String configurationFile = "gtfs5-q6-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS10_q6_mysql() {
        //2 instances
        String configurationFile = "gtfs10-q6-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS1_q07_mysql() {
        //48 instances
        String configurationFile = "gtfs1-q07-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS5_q7_mysql() {
        //2 instances
        String configurationFile = "gtfs5-q7-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS10_q7_mysql() {
        //2 instances
        String configurationFile = "gtfs10-q7-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS1_q08_mysql() {
        //0 instances
        String configurationFile = "gtfs1-q08-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS5_q8_mysql() {
        //2 instances
        String configurationFile = "gtfs5-q8-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS10_q8_mysql() {
        //2 instances
        String configurationFile = "gtfs10-q3b-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS1_q09_mysql() {
        //0 instances
        String configurationFile = "gtfs1-q09-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS5_q9_mysql() {
        //2 instances
        String configurationFile = "gtfs5-q9-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }


    @Test
    public void testGTFS10_q9_mysql() {
        //2 instances
        String configurationFile = "gtfs10-q9-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS1_q10_mysql() {
        //1 instances
        String configurationFile = "gtfs1-q10-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS5_q10_mysql() {
        //2 instances
        String configurationFile = "gtfs5-q10-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS5_q11_mysql() {
        //2 instances
        String configurationFile = "gtfs5-q11-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS5_q12_mysql() {
        //2 instances
        String configurationFile = "gtfs5-q12-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS5_q13_mysql() {
        //2 instances
        String configurationFile = "gtfs5-q13-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }


    @Test
    public void testGTFS5_q14_mysql() {
        //2 instances
        String configurationFile = "gtfs5-q14-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS5_q15_mysql() {
        //2 instances
        String configurationFile = "gtfs5-q15-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS5_q16_mysql() {
        //2 instances
        String configurationFile = "gtfs5-q16-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS5_q17_mysql() {
        //2 instances
        String configurationFile = "gtfs5-q17-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }


    @Test
    public void testGTFS5_q18_mysql() {
        //2 instances
        String configurationFile = "gtfs5-q18-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testGTFS10_q10_mysql() {
        //2 instances
        String configurationFile = "gtfs10-q3d-mysql.morph.properties";
        try {
            String[] args = {configurationDirectory, configurationFile};
            MorphRDBRunner.main(args);
            System.out.println("Query process DONE------");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error : " + e.getMessage());
            System.out.println("Query process FAILED------");
            assertTrue(e.getMessage(), false);
        }
    }


}
