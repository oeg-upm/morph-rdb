package es.upm.fi.dia.oeg.morph.testcase;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.R2RMLRDBRunnerFactory;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.R2RMLRunner;

public class R2RMLTS {
	private static Logger logger = Logger.getLogger(R2RMLTS.class);
	//private String mappingDirectory = TestUtility.getMappingDirectoryByOS();
	private String mappingDirectory = System.getProperty("user.dir") 
			+ File.separator + "R2RMLTS";
	private Map<String, String> mapTestCaseName = new LinkedHashMap<String, String>();
	
	public R2RMLTS() {
		this.mapTestCaseName.put("R2RMLTC0000", "D000-1table1column0rows");
		
		this.mapTestCaseName.put("R2RMLTC0001a", "D001-1table1column1row");
		this.mapTestCaseName.put("R2RMLTC0001b", "D001-1table1column1row");
		
		this.mapTestCaseName.put("R2RMLTC0002a", "D002-1table2columns1row");
		this.mapTestCaseName.put("R2RMLTC0002b", "D002-1table2columns1row");
		this.mapTestCaseName.put("R2RMLTC0002c", "D002-1table2columns1row");
		this.mapTestCaseName.put("R2RMLTC0002d", "D002-1table2columns1row");
		this.mapTestCaseName.put("R2RMLTC0002e", "D002-1table2columns1row");
		this.mapTestCaseName.put("R2RMLTC0002f", "D002-1table2columns1row");
		this.mapTestCaseName.put("R2RMLTC0002g", "D002-1table2columns1row");
		this.mapTestCaseName.put("R2RMLTC0002h", "D002-1table2columns1row");
		this.mapTestCaseName.put("R2RMLTC0002i", "D002-1table2columns1row");
		this.mapTestCaseName.put("R2RMLTC0002j", "D002-1table2columns1row");
		
		this.mapTestCaseName.put("R2RMLTC0003a", "D003-1table3columns1row");
		this.mapTestCaseName.put("R2RMLTC0003b", "D003-1table3columns1row");
		this.mapTestCaseName.put("R2RMLTC0003c", "D003-1table3columns1row");
		
		this.mapTestCaseName.put("R2RMLTC0004a", "D004-1table2columns1row");
		this.mapTestCaseName.put("R2RMLTC0004b", "D003-1table3columns1row");
		
		this.mapTestCaseName.put("R2RMLTC0005a", "D005-1table3columns3rows2duplicates");
		this.mapTestCaseName.put("R2RMLTC0005b", "D005-1table3columns3rows2duplicates");

		this.mapTestCaseName.put("R2RMLTC0006a", "D006-1table1primarykey1column1row");

		this.mapTestCaseName.put("R2RMLTC0007a", "D007-1table1primarykey2columns1row");
		this.mapTestCaseName.put("R2RMLTC0007b", "D007-1table1primarykey2columns1row");
		this.mapTestCaseName.put("R2RMLTC0007c", "D007-1table1primarykey2columns1row");
		this.mapTestCaseName.put("R2RMLTC0007d", "D007-1table1primarykey2columns1row");
		this.mapTestCaseName.put("R2RMLTC0007e", "D007-1table1primarykey2columns1row");
		this.mapTestCaseName.put("R2RMLTC0007f", "D007-1table1primarykey2columns1row");
		this.mapTestCaseName.put("R2RMLTC0007g", "D007-1table1primarykey2columns1row");
		this.mapTestCaseName.put("R2RMLTC0007h", "D007-1table1primarykey2columns1row");

		this.mapTestCaseName.put("R2RMLTC0008a", "D008-1table1compositeprimarykey3columns1row");
		this.mapTestCaseName.put("R2RMLTC0008b", "D008-1table1compositeprimarykey3columns1row");
		this.mapTestCaseName.put("R2RMLTC0008c", "D008-1table1compositeprimarykey3columns1row");

		this.mapTestCaseName.put("R2RMLTC0009a", "D009-2tables1primarykey1foreignkey");
		this.mapTestCaseName.put("R2RMLTC0009b", "D009-2tables1primarykey1foreignkey");
		this.mapTestCaseName.put("R2RMLTC0009c", "D009-2tables1primarykey1foreignkey");
		this.mapTestCaseName.put("R2RMLTC0009d", "D009-2tables1primarykey1foreignkey");

		this.mapTestCaseName.put("R2RMLTC0010a", "D010-1table1primarykey3colums3rows");
		this.mapTestCaseName.put("R2RMLTC0010b", "D010-1table1primarykey3colums3rows");
		this.mapTestCaseName.put("R2RMLTC0010c", "D010-1table1primarykey3colums3rows");

		this.mapTestCaseName.put("R2RMLTC0011a", "D011-M2MRelations");
		this.mapTestCaseName.put("R2RMLTC0011b", "D011-M2MRelations");
		
		this.mapTestCaseName.put("R2RMLTC0012a", "D012-2tables2duplicates0nulls");
		this.mapTestCaseName.put("R2RMLTC0012b", "D012-2tables2duplicates0nulls");
		this.mapTestCaseName.put("R2RMLTC0012c", "D012-2tables2duplicates0nulls");
		this.mapTestCaseName.put("R2RMLTC0012d", "D012-2tables2duplicates0nulls");
		this.mapTestCaseName.put("R2RMLTC0012e", "D012-2tables2duplicates0nulls");

		this.mapTestCaseName.put("R2RMLTC0013a", "D013-1table1primarykey3columns2rows1nullvalue");

		this.mapTestCaseName.put("R2RMLTC0014a", "D014-3tables1primarykey1foreignkey");
		this.mapTestCaseName.put("R2RMLTC0014b", "D014-3tables1primarykey1foreignkey");
		this.mapTestCaseName.put("R2RMLTC0014c", "D014-3tables1primarykey1foreignkey");
		this.mapTestCaseName.put("R2RMLTC0014d", "D014-3tables1primarykey1foreignkey");

		this.mapTestCaseName.put("R2RMLTC0015a", "D015-1table3columns1composityeprimarykey3rows2languages");
		this.mapTestCaseName.put("R2RMLTC0015b", "D015-1table3columns1composityeprimarykey3rows2languages");

		this.mapTestCaseName.put("R2RMLTC0016a", "D016-1table1primarykey10columns3rowsSQLdatatypes");
		this.mapTestCaseName.put("R2RMLTC0016b", "D016-1table1primarykey10columns3rowsSQLdatatypes");
		this.mapTestCaseName.put("R2RMLTC0016c", "D016-1table1primarykey10columns3rowsSQLdatatypes");
		this.mapTestCaseName.put("R2RMLTC0016d", "D016-1table1primarykey10columns3rowsSQLdatatypes");
		this.mapTestCaseName.put("R2RMLTC0016e", "D016-1table1primarykey10columns3rowsSQLdatatypes");

		this.mapTestCaseName.put("R2RMLTC0018a", "D018-1table1primarykey2columns3rows");

		this.mapTestCaseName.put("R2RMLTC0019a", "D019-1table1primarykey3columns3rows");
		this.mapTestCaseName.put("R2RMLTC0019b", "D019-1table1primarykey3columns3rows");

		this.mapTestCaseName.put("R2RMLTC0020a", "D020-1table1column5rows");
		this.mapTestCaseName.put("R2RMLTC0020b", "D020-1table1column5rows");
		
	}
	
//	public static void main(String args[]) {
//		R2RMLTS testcaseInstance = new R2RMLTS();
//		for(String testName : testcaseInstance.mapTestCaseName.keySet()) {
//			try {
//				testcaseInstance.run(testName);	
//			} catch(Exception e) {
//				
//			}
//			
//		}
//	}
	
	
	static {
		PropertyConfigurator.configure("log4j.properties");
	}
	
	public void run(String testName, boolean conformingMapping) {
		try {
			String directoryName = this.mapTestCaseName.get(testName);
			String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
			String configurationFile = testName + ".r2rml.properties";
			R2RMLRDBRunnerFactory runnerFactory = new R2RMLRDBRunnerFactory(); 
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			logger.info("------" + testName + " DONE------");
			assertTrue(conformingMapping);
		} catch(Exception e) {
			e.printStackTrace();
			logger.error("Error : " + e.getMessage());
			logger.info("------" + testName + " FAILED------\n\n");
			assertTrue(e.getMessage(), !conformingMapping);
		}
	}
	
	@Test
	public void testR2RMLTC0000() throws Exception {
		String testName = "R2RMLTC0000";
		this.run(testName, true);
	}

	@Test
	public void testR2RMLTC0001a() throws Exception {
		String testName = "R2RMLTC0001a";
		this.run(testName, true);
	}

	@Test
	public void testR2RMLTC0001b() throws Exception {
		String testName = "R2RMLTC0001b";
		this.run(testName, true);
	}

	@Test
	public void testR2RMLTC0002a() throws Exception {
		String testName = "R2RMLTC0002a";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0002b() throws Exception {
		String testName = "R2RMLTC0002b";
		this.run(testName, true);
	}

	@Test
	public void testR2RMLTC0002c() throws Exception {
		//Title: Two columns mapping, an undefined SQL identifier
		//Purpose: Tests the presence of an undefined SQL identifier 
		//Expected result: non-conforming R2RML mapping 
		String testName = "R2RMLTC0002c";
		this.run(testName, false);
	}
	
	@Test
	public void testR2RMLTC0002d() throws Exception {
		String testName = "R2RMLTC0002d";
		this.run(testName, true);
	}

	@Test
	public void testR2RMLTC0002e() throws Exception {
		//Title: Two columns mapping, an undefined rr:tableName
		//Purpose: Tests the presence of an undefined rr:tableName 
		//Expected result: non-conforming R2RML mapping 
		String testName = "R2RMLTC0002e";
		this.run(testName, false);
	}

	@Test
	public void testR2RMLTC0002f() throws Exception {
		//Incorrect mappings : 
		String testName = "R2RMLTC0002f";
		this.run(testName, false);
	}

	@Test
	public void testR2RMLTC0002g() throws Exception {
		//Title: Two columns mapping, invalid SQL query
		//Purpose: Tests the presence of an invalid SQL query
		//Expected result: non-conforming R2RML mapping 
		String testName = "R2RMLTC0002g";
		this.run(testName, false);
	}
	
	@Test
	public void testR2RMLTC0002h() throws Exception {
		//Title: Two columns mapping, invalid SQL query
		//Purpose: Tests the presence of an invalid SQL query 
		//Expected result: non-conforming R2RML mapping 
		String testName = "R2RMLTC0002h";
		this.run(testName, false);
	}
	
	@Test
	public void testR2RMLTC0002i() throws Exception {
		String testName = "R2RMLTC0002i";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0002j() throws Exception {
		String testName = "R2RMLTC0002j";
		this.run(testName, true);
	}

	@Test
	public void testR2RMLTC0003a() throws Exception {
		//Title: Three columns mapping, undefined SQL Version identifier
		//Purpose: Tests the presence of an undefined SQL Version identifier
		//Expected result: non-conforming R2RML mapping 		
		String testName = "R2RMLTC0003a";
		this.run(testName, false);
	}
	
	@Test
	public void testR2RMLTC0003b() throws Exception {
		String testName = "R2RMLTC0003b";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0003c() throws Exception {
		String testName = "R2RMLTC0003c";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0004a() throws Exception {
		String testName = "R2RMLTC0004a";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0004b() throws Exception {
		String testName = "R2RMLTC0004b";
		this.run(testName, false);
	}
	
	@Test
	public void testR2RMLTC0005a() throws Exception {
		String testName = "R2RMLTC0005a";
		this.run(testName, true);
	}

	@Test
	public void testR2RMLTC0005b() throws Exception {
		String testName = "R2RMLTC0005b";
		this.run(testName, true);
	}

	@Test
	public void testR2RMLTC0006a() throws Exception {
		//wrong result in the testcase document
		//or mapping is missing termtype
		String testName = "R2RMLTC0006a";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0007a() throws Exception {
		String testName = "R2RMLTC0007a";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0007b() throws Exception {
		String testName = "R2RMLTC0007b";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0007c() throws Exception {
		String testName = "R2RMLTC0007c";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0007d() throws Exception {
		String testName = "R2RMLTC0007d";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0007e() throws Exception {
		String testName = "R2RMLTC0007e";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0007f() throws Exception {
		String testName = "R2RMLTC0007f";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0007g() throws Exception {
		String testName = "R2RMLTC0007g";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0007h() throws Exception {
		String testName = "R2RMLTC0007h";
		this.run(testName, false);
	}
	
	
	@Test
	public void testR2RMLTC0008a() throws Exception {
		String testName = "R2RMLTC0008a";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0008b() throws Exception {
		String testName = "R2RMLTC0008b";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0008c() throws Exception {
		String testName = "R2RMLTC0008c";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0009a() throws Exception {
		String testName = "R2RMLTC0009a";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0009b() throws Exception {
		String testName = "R2RMLTC0009b";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0009c() throws Exception {
		String testName = "R2RMLTC0009c";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0009d() throws Exception {
		String testName = "R2RMLTC0009d";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0010a() throws Exception {
		String testName = "R2RMLTC0010a";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0010b() throws Exception {
		String testName = "R2RMLTC0010b";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0010c() throws Exception {
		String testName = "R2RMLTC0010c";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0011a() throws Exception {
		String testName = "R2RMLTC0011a";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0011b() throws Exception {
		String testName = "R2RMLTC0011b";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0012a() throws Exception {
		String testName = "R2RMLTC0012a";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0012b() throws Exception {
		String testName = "R2RMLTC0012b";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0012c() throws Exception {
		String testName = "R2RMLTC0012c";
		this.run(testName, false);
	}

	@Test
	public void testR2RMLTC0012d() throws Exception {
		String testName = "R2RMLTC0012d";
		this.run(testName, false);
	}
	
	@Test
	public void testR2RMLTC0012e() throws Exception {
		String testName = "R2RMLTC0012e";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0013a() throws Exception {
		String testName = "R2RMLTC0013a";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0014a() throws Exception {
		String testName = "R2RMLTC0014a";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0014b() throws Exception {
		String testName = "R2RMLTC0014b";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0014c() throws Exception {
		String testName = "R2RMLTC0014c";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0014d() throws Exception {
		String testName = "R2RMLTC0014d";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0015a() throws Exception {
		String testName = "R2RMLTC0015a";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0015b() throws Exception {
		//Title: Generation of language tags from a table with language information, and a term map with invalid rr:language value
		//Purpose: Tests a term map with an invalid rr:language value, which is an error
		//Expected result: non-conforming R2RML mapping 		
		String testName = "R2RMLTC0015b";
		this.run(testName, false);
	}
	
	@Test
	public void testR2RMLTC0016a() throws Exception {
		String testName = "R2RMLTC0016a";
		this.run(testName, true);
	}

	@Test
	public void testR2RMLTC0016b() throws Exception {
		String testName = "R2RMLTC0016b";
		this.run(testName, true);
	}

	@Test
	public void testR2RMLTC0016c() throws Exception {
		String testName = "R2RMLTC0016c";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0016d() throws Exception {
		//testcase mapping missing datatype tag
		String testName = "R2RMLTC0016d";
		this.run(testName, true);
	}

	@Test
	public void testR2RMLTC0016e() throws Exception {
		String testName = "R2RMLTC0016e";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0018a() throws Exception {
		String testName = "R2RMLTC0018a";
		this.run(testName, true);
	}

	@Test
	public void testR2RMLTC0019a() throws Exception {
		String testName = "R2RMLTC0019a";
		this.run(testName, true);
	}
	
	@Test
	public void testR2RMLTC0019b() throws Exception {
		String testName = "R2RMLTC0019b";
		this.run(testName, true);
	}

	@Test
	public void testR2RMLTC0020a() throws Exception {
		//wrong mapping/result of TC
		String testName = "R2RMLTC0020a";
		this.run(testName, true);
	}

	@Test
	public void testR2RMLTC0020b() throws Exception {
		String testName = "R2RMLTC0020b";
		this.run(testName, true);
	}

}
