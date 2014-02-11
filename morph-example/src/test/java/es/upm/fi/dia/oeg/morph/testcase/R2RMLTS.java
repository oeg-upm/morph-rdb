package es.upm.fi.dia.oeg.morph.testcase;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

import es.upm.fi.dia.oeg.obdi.core.engine.AbstractRunner;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLRunner;

public class R2RMLTS {
	private static Logger logger = Logger.getLogger(R2RMLTS.class);
	//private String mappingDirectory = TestUtility.getMappingDirectoryByOS();
	private String mappingDirectory = System.getProperty("user.dir") 
			+ File.separator + "R2RMLTS";
	
	static {
		PropertyConfigurator.configure("log4j.properties");
	}
	
	public void run(String configurationDirectory, String configurationFile, String testName) {
		try {
			AbstractRunner runner = new R2RMLRunner(configurationDirectory, configurationFile);
			runner.run();
			logger.info("------" + testName + " DONE------\n\n");
		} catch(Exception e) {
			e.printStackTrace();
			logger.error("Error : " + e.getMessage());
			logger.info("------" + testName + " FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}
	
	@Test
	public void testR2RMLTC0000() throws Exception {
		String testName = "R2RMLTC0000";
		String directoryName = "D000-1table1column0rows";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}

	@Test
	public void testR2RMLTC0001a() throws Exception {
		String directoryName = "D001-1table1column1row";
		String testName = "R2RMLTC0001a";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}

	@Test
	public void testR2RMLTC0001b() throws Exception {
		String directoryName = "D001-1table1column1row";
		String testName = "R2RMLTC0001b";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}

	@Test
	public void testR2RMLTC0002a() throws Exception {
		String directoryName = "D002-1table2columns1row";
		String testName = "R2RMLTC0002a";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0002b() throws Exception {
		String directoryName = "D002-1table2columns1row";
		String testName = "R2RMLTC0002b";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}

	@Test
	public void testR2RMLTC0002c() throws Exception {
		//Title: Two columns mapping, an undefined SQL identifier
		//Purpose: Tests the presence of an undefined SQL identifier 
		//Expected result: non-conforming R2RML mapping 
		String directoryName = "D002-1table2columns1row";
		String testName = "R2RMLTC0002c";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0002d() throws Exception {
		String directoryName = "D002-1table2columns1row";
		String testName = "R2RMLTC0002d";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}

	@Test
	public void testR2RMLTC0002e() throws Exception {
		//Title: Two columns mapping, an undefined rr:tableName
		//Purpose: Tests the presence of an undefined rr:tableName 
		//Expected result: non-conforming R2RML mapping 
		String directoryName = "D002-1table2columns1row";
		String testName = "R2RMLTC0002e";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}

	@Test
	public void testR2RMLTC0002f() throws Exception {
		//Incorrect mappings : 
		String directoryName = "D002-1table2columns1row";
		String testName = "R2RMLTC0002f";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}

	@Test
	public void testR2RMLTC0002g() throws Exception {
		//Title: Two columns mapping, invalid SQL query
		//Purpose: Tests the presence of an invalid SQL query
		//Expected result: non-conforming R2RML mapping 
		String directoryName = "D002-1table2columns1row";
		String testName = "R2RMLTC0002g";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0002h() throws Exception {
		//Title: Two columns mapping, invalid SQL query
		//Purpose: Tests the presence of an invalid SQL query 
		//Expected result: non-conforming R2RML mapping 
		String directoryName = "D002-1table2columns1row";
		String testName = "R2RMLTC0002h";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0002i() throws Exception {
		String directoryName = "D002-1table2columns1row";
		String testName = "R2RMLTC0002i";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0002j() throws Exception {
		String directoryName = "D002-1table2columns1row";
		String testName = "R2RMLTC0002j";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}

	@Test
	public void testR2RMLTC0003a() throws Exception {
		String directoryName = "D003-1table3columns1row";
		String testName = "R2RMLTC0003a";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0003b() throws Exception {
		String directoryName = "D003-1table3columns1row";
		String testName = "R2RMLTC0003b";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0003c() throws Exception {
		String directoryName = "D003-1table3columns1row";
		String testName = "R2RMLTC0003c";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0004a() throws Exception {
		String directoryName = "D004-1table2columns1row";
		String testName = "R2RMLTC0004a";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0004b() throws Exception {
		String directoryName = "D004-1table2columns1row";
		String testName = "R2RMLTC0004b";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0005a() throws Exception {
		String directoryName = "D005-1table3columns3rows2duplicates";
		String testName = "R2RMLTC0005a";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}

	@Test
	public void testR2RMLTC0005b() throws Exception {
		String directoryName = "D005-1table3columns3rows2duplicates";
		String testName = "R2RMLTC0005b";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}

	@Test
	public void testR2RMLTC0006a() throws Exception {
		//wrong result in the testcase document
		//or mapping is missing termtype
		String directoryName = "D006-1table1primarykey1column1row";
		String testName = "R2RMLTC0006a";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0007a() throws Exception {
		String directoryName = "D007-1table1primarykey2columns1row";
		String testName = "R2RMLTC0007a";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0007b() throws Exception {
		String directoryName = "D007-1table1primarykey2columns1row";
		String testName = "R2RMLTC0007b";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0007c() throws Exception {
		String directoryName = "D007-1table1primarykey2columns1row";
		String testName = "R2RMLTC0007c";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0007d() throws Exception {
		String directoryName = "D007-1table1primarykey2columns1row";
		String testName = "R2RMLTC0007d";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0007e() throws Exception {
		String directoryName = "D007-1table1primarykey2columns1row";
		String testName = "R2RMLTC0007e";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0007f() throws Exception {
		String directoryName = "D007-1table1primarykey2columns1row";
		String testName = "R2RMLTC0007f";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0007g() throws Exception {
		String directoryName = "D007-1table1primarykey2columns1row";
		String testName = "R2RMLTC0007g";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0007h() throws Exception {
		String directoryName = "D007-1table1primarykey2columns1row";
		String testName = "R2RMLTC0007h";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	
	@Test
	public void testR2RMLTC0008a() throws Exception {
		String directoryName = "D008-1table1compositeprimarykey3columns1row";
		String testName = "R2RMLTC0008a";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0008b() throws Exception {
		String directoryName = "D008-1table1compositeprimarykey3columns1row";
		String testName = "R2RMLTC0008b";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0008c() throws Exception {
		String directoryName = "D008-1table1compositeprimarykey3columns1row";
		String testName = "R2RMLTC0008c";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0009a() throws Exception {
		String directoryName = "D009-2tables1primarykey1foreignkey";
		String testName = "R2RMLTC0009a";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0009b() throws Exception {
		String directoryName = "D009-2tables1primarykey1foreignkey";
		String testName = "R2RMLTC0009b";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0009c() throws Exception {
		String directoryName = "D009-2tables1primarykey1foreignkey";
		String testName = "R2RMLTC0009c";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0009d() throws Exception {
		String directoryName = "D009-2tables1primarykey1foreignkey";
		String testName = "R2RMLTC0009d";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0010a() throws Exception {
		String directoryName = "D010-1table1primarykey3colums3rows";
		String testName = "R2RMLTC0010a";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0010b() throws Exception {
		String directoryName = "D010-1table1primarykey3colums3rows";
		String testName = "R2RMLTC0010b";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0010c() throws Exception {
		String directoryName = "D010-1table1primarykey3colums3rows";
		String testName = "R2RMLTC0010c";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0011a() throws Exception {
		String directoryName = "D011-M2MRelations";
		String testName = "R2RMLTC0011a";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0011b() throws Exception {
		String directoryName = "D011-M2MRelations";
		String testName = "R2RMLTC0011b";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0012a() throws Exception {
		String directoryName = "D012-2tables2duplicates0nulls";
		String testName = "R2RMLTC0012a";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0012b() throws Exception {
		String directoryName = "D012-2tables2duplicates0nulls";
		String testName = "R2RMLTC0012b";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0012c() throws Exception {
		String directoryName = "D012-2tables2duplicates0nulls";
		String testName = "R2RMLTC0012c";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}

	@Test
	public void testR2RMLTC0012d() throws Exception {
		String directoryName = "D012-2tables2duplicates0nulls";
		String testName = "R2RMLTC0012d";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0012e() throws Exception {
		String directoryName = "D012-2tables2duplicates0nulls";
		String testName = "R2RMLTC0012e";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0013a() throws Exception {
		String directoryName = "D013-1table1primarykey3columns2rows1nullvalue";
		String testName = "R2RMLTC0013a";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0014a() throws Exception {
		String directoryName = "D014-3tables1primarykey1foreignkey";
		String testName = "R2RMLTC0014a";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0014b() throws Exception {
		String directoryName = "D014-3tables1primarykey1foreignkey";
		String testName = "R2RMLTC0014b";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0014c() throws Exception {
		String directoryName = "D014-3tables1primarykey1foreignkey";
		String testName = "R2RMLTC0014c";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0014d() throws Exception {
		String directoryName = "D014-3tables1primarykey1foreignkey";
		String testName = "R2RMLTC0014d";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0015a() throws Exception {
		String directoryName = "D015-1table3columns1composityeprimarykey3rows2languages";
		String testName = "R2RMLTC0015a";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0015b() throws Exception {
		String directoryName = "D015-1table3columns1composityeprimarykey3rows2languages";
		String testName = "R2RMLTC0015b";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0016a() throws Exception {
		String directoryName = "D016-1table1primarykey10columns3rowsSQLdatatypes";
		String testName = "R2RMLTC0016a";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}

	@Test
	public void testR2RMLTC0016b() throws Exception {
		String directoryName = "D016-1table1primarykey10columns3rowsSQLdatatypes";
		String testName = "R2RMLTC0016b";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}

	@Test
	public void testR2RMLTC0016c() throws Exception {
		String directoryName = "D016-1table1primarykey10columns3rowsSQLdatatypes";
		String testName = "R2RMLTC0016c";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0016d() throws Exception {
		//testcase mapping missing datatype tag
		String directoryName = "D016-1table1primarykey10columns3rowsSQLdatatypes";
		String testName = "R2RMLTC0016d";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}

	@Test
	public void testR2RMLTC0016e() throws Exception {
		String directoryName = "D016-1table1primarykey10columns3rowsSQLdatatypes";
		String testName = "R2RMLTC0016e";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0018a() throws Exception {
		String directoryName = "D018-1table1primarykey2columns3rows";
		String testName = "R2RMLTC0018a";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}

	@Test
	public void testR2RMLTC0019a() throws Exception {
		String directoryName = "D019-1table1primarykey3columns3rows";
		String testName = "R2RMLTC0019a";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}
	
	@Test
	public void testR2RMLTC0019b() throws Exception {
		String directoryName = "D019-1table1primarykey3columns3rows";
		String testName = "R2RMLTC0019b";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}

	@Test
	public void testR2RMLTC0020a() throws Exception {
		//wrong mapping/result of TC
		String directoryName = "D020-1table1column5rows";
		String testName = "R2RMLTC0020a";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}

	@Test
	public void testR2RMLTC0020b() throws Exception {
		String directoryName = "D020-1table1column5rows";
		String testName = "R2RMLTC0020b";
		String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
		String configurationFile = testName + ".r2rml.properties";
		this.run(configurationDirectory, configurationFile, testName);
	}

}
