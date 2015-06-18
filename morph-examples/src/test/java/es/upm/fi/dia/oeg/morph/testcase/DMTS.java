package es.upm.fi.dia.oeg.morph.testcase;

import static org.junit.Assert.*;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunnerFactory;

public class DMTS {
	private String mappingDirectory = System.getProperty("user.dir") 
			+ File.separator + "testcases";
	private Map<String, String> mapTestCaseName = new LinkedHashMap<String, String>();
	
	public DMTS() {
		this.mapTestCaseName.put("DirectGraphTC0000", "D000-1table1column0rows");
		this.mapTestCaseName.put("R2RMLTC0000", "D000-1table1column0rows");
		
		this.mapTestCaseName.put("DirectGraphTC0001", "D001-1table1column1row");
		this.mapTestCaseName.put("R2RMLTC0001a", "D001-1table1column1row");
		this.mapTestCaseName.put("R2RMLTC0001b", "D001-1table1column1row");
		
		this.mapTestCaseName.put("DirectGraphTC0002", "D002-1table2columns1row");
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
		
		this.mapTestCaseName.put("DirectGraphTC0003", "D003-1table3columns1row");
		this.mapTestCaseName.put("R2RMLTC0003a", "D003-1table3columns1row");
		this.mapTestCaseName.put("R2RMLTC0003b", "D003-1table3columns1row");
		this.mapTestCaseName.put("R2RMLTC0003c", "D003-1table3columns1row");
		
		this.mapTestCaseName.put("DirectGraphTC0004", "D004-1table2columns1row");
		this.mapTestCaseName.put("R2RMLTC0004a", "D004-1table2columns1row");
		this.mapTestCaseName.put("R2RMLTC0004b", "D003-1table3columns1row");
		
		this.mapTestCaseName.put("DirectGraphTC0005", "D005-1table3columns3rows2duplicates");
		this.mapTestCaseName.put("R2RMLTC0005a", "D005-1table3columns3rows2duplicates");
		this.mapTestCaseName.put("R2RMLTC0005b", "D005-1table3columns3rows2duplicates");

		this.mapTestCaseName.put("DirectGraphTC0006", "D006-1table1primarykey1column1row");
		this.mapTestCaseName.put("R2RMLTC0006a", "D006-1table1primarykey1column1row");

		this.mapTestCaseName.put("DirectGraphTC0007", "D007-1table1primarykey2columns1row");
		this.mapTestCaseName.put("R2RMLTC0007a", "D007-1table1primarykey2columns1row");
		this.mapTestCaseName.put("R2RMLTC0007b", "D007-1table1primarykey2columns1row");
		this.mapTestCaseName.put("R2RMLTC0007c", "D007-1table1primarykey2columns1row");
		this.mapTestCaseName.put("R2RMLTC0007d", "D007-1table1primarykey2columns1row");
		this.mapTestCaseName.put("R2RMLTC0007e", "D007-1table1primarykey2columns1row");
		this.mapTestCaseName.put("R2RMLTC0007f", "D007-1table1primarykey2columns1row");
		this.mapTestCaseName.put("R2RMLTC0007g", "D007-1table1primarykey2columns1row");
		this.mapTestCaseName.put("R2RMLTC0007h", "D007-1table1primarykey2columns1row");

		this.mapTestCaseName.put("DirectGraphTC0008", "D008-1table1compositeprimarykey3columns1row");
		this.mapTestCaseName.put("R2RMLTC0008a", "D008-1table1compositeprimarykey3columns1row");
		this.mapTestCaseName.put("R2RMLTC0008b", "D008-1table1compositeprimarykey3columns1row");
		this.mapTestCaseName.put("R2RMLTC0008c", "D008-1table1compositeprimarykey3columns1row");

		this.mapTestCaseName.put("DirectGraphTC0009", "D009-2tables1primarykey1foreignkey");
		this.mapTestCaseName.put("R2RMLTC0009a", "D009-2tables1primarykey1foreignkey");
		this.mapTestCaseName.put("R2RMLTC0009b", "D009-2tables1primarykey1foreignkey");
		this.mapTestCaseName.put("R2RMLTC0009c", "D009-2tables1primarykey1foreignkey");
		this.mapTestCaseName.put("R2RMLTC0009d", "D009-2tables1primarykey1foreignkey");

		this.mapTestCaseName.put("DirectGraphTC0010", "D010-1table1primarykey3colums3rows");
		this.mapTestCaseName.put("R2RMLTC0010a", "D010-1table1primarykey3colums3rows");
		this.mapTestCaseName.put("R2RMLTC0010b", "D010-1table1primarykey3colums3rows");
		this.mapTestCaseName.put("R2RMLTC0010c", "D010-1table1primarykey3colums3rows");

		this.mapTestCaseName.put("DirectGraphTC0011", "D011-M2MRelations");
		this.mapTestCaseName.put("R2RMLTC0011a", "D011-M2MRelations");
		this.mapTestCaseName.put("R2RMLTC0011b", "D011-M2MRelations");
		
		this.mapTestCaseName.put("DirectGraphTC0012", "D012-2tables2duplicates0nulls");
		this.mapTestCaseName.put("R2RMLTC0012a", "D012-2tables2duplicates0nulls");
		this.mapTestCaseName.put("R2RMLTC0012b", "D012-2tables2duplicates0nulls");
		this.mapTestCaseName.put("R2RMLTC0012c", "D012-2tables2duplicates0nulls");
		this.mapTestCaseName.put("R2RMLTC0012d", "D012-2tables2duplicates0nulls");
		this.mapTestCaseName.put("R2RMLTC0012e", "D012-2tables2duplicates0nulls");

		this.mapTestCaseName.put("DirectGraphTC0013", "D013-1table1primarykey3columns2rows1nullvalue");
		this.mapTestCaseName.put("R2RMLTC0013a", "D013-1table1primarykey3columns2rows1nullvalue");

		this.mapTestCaseName.put("DirectGraphTC0014", "D014-3tables1primarykey1foreignkey");
		this.mapTestCaseName.put("R2RMLTC0014a", "D014-3tables1primarykey1foreignkey");
		this.mapTestCaseName.put("R2RMLTC0014b", "D014-3tables1primarykey1foreignkey");
		this.mapTestCaseName.put("R2RMLTC0014c", "D014-3tables1primarykey1foreignkey");
		this.mapTestCaseName.put("R2RMLTC0014d", "D014-3tables1primarykey1foreignkey");

		this.mapTestCaseName.put("DirectGraphTC0015", "D015-1table3columns1composityeprimarykey3rows2languages");
		this.mapTestCaseName.put("R2RMLTC0015a", "D015-1table3columns1composityeprimarykey3rows2languages");
		this.mapTestCaseName.put("R2RMLTC0015b", "D015-1table3columns1composityeprimarykey3rows2languages");

		this.mapTestCaseName.put("DirectGraphTC0016", "D016-1table1primarykey10columns3rowsSQLdatatypes");
		this.mapTestCaseName.put("R2RMLTC0016a", "D016-1table1primarykey10columns3rowsSQLdatatypes");
		this.mapTestCaseName.put("R2RMLTC0016b", "D016-1table1primarykey10columns3rowsSQLdatatypes");
		this.mapTestCaseName.put("R2RMLTC0016c", "D016-1table1primarykey10columns3rowsSQLdatatypes");
		this.mapTestCaseName.put("R2RMLTC0016d", "D016-1table1primarykey10columns3rowsSQLdatatypes");
		this.mapTestCaseName.put("R2RMLTC0016e", "D016-1table1primarykey10columns3rowsSQLdatatypes");

		this.mapTestCaseName.put("DirectGraphTC0018", "D018-1table1primarykey2columns3rows");
		this.mapTestCaseName.put("R2RMLTC0018a", "D018-1table1primarykey2columns3rows");

		this.mapTestCaseName.put("DirectGraphTC0019", "D019-1table1primarykey3columns3rows");
		this.mapTestCaseName.put("R2RMLTC0019a", "D019-1table1primarykey3columns3rows");
		this.mapTestCaseName.put("R2RMLTC0019b", "D019-1table1primarykey3columns3rows");

		this.mapTestCaseName.put("DirectGraphTC0020", "D020-1table1column5rows");
		this.mapTestCaseName.put("R2RMLTC0020a", "D020-1table1column5rows");
		this.mapTestCaseName.put("R2RMLTC0020b", "D020-1table1column5rows");
		
	}
	
//	static {
//		PropertyConfigurator.configure("log4j.properties");
//	}
	
	public void run(String testName, boolean conformingMapping) {
		try {
//			String directoryName = this.mapTestCaseName.get(testName);
			//String configurationDirectory = mappingDirectory + File.separator + directoryName + File.separator;
			String configurationDirectory = mappingDirectory + File.separator;
			String configurationFile = testName + ".morph.properties";
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory(); 
			MorphBaseRunner runner = runnerFactory.createRunner(configurationDirectory, configurationFile);
			runner.run();
			System.out.println("------" + testName + " DONE------");
			assertTrue(conformingMapping);
		} catch(Exception e) {
			e.printStackTrace();
			System.out.println("Error : " + e.getMessage());
			System.out.println("------" + testName + " FAILED------\n\n");
			assertTrue(e.getMessage(), !conformingMapping);
		}
	}
	
	@Test
	public void testDirectGraphTC0000() throws Exception {
		String testName = "DirectGraphTC0000";
		this.run(testName, true);
	}

	@Test
	public void testDirectGraphTC0001() throws Exception {
		String testName = "DirectGraphTC0001";
		this.run(testName, true);
	}

	@Test
	public void testDirectGraphTC0002() throws Exception {
		String testName = "DirectGraphTC0002";
		this.run(testName, true);
	}

	@Test
	public void testDirectGraphTC0003() throws Exception {
		String testName = "DirectGraphTC0003";
		this.run(testName, true);
	}

	@Test
	public void testDirectGraphTC0004() throws Exception {
		String testName = "DirectGraphTC0004";
		this.run(testName, true);
	}

	@Test
	public void testDirectGraphTC0005() throws Exception {
		String testName = "DirectGraphTC0005";
		this.run(testName, true);
	}
	
	@Test
	public void testDirectGraphTC0006() throws Exception {
		String testName = "DirectGraphTC0006";
		this.run(testName, true);
	}
	
	@Test
	public void testDirectGraphTC0007() throws Exception {
		String testName = "DirectGraphTC0007";
		this.run(testName, true);
	}
	
	@Test
	public void testDirectGraphTC0008() throws Exception {
		String testName = "DirectGraphTC0008";
		this.run(testName, true);
	}
	
	@Test
	public void testDirectGraphTC0009() throws Exception {
		String testName = "DirectGraphTC0009";
		this.run(testName, true);
	}
	
	@Test
	public void testDirectGraphTC0010() throws Exception {
		String testName = "DirectGraphTC0010";
		this.run(testName, true);
	}
	
	@Test
	public void testDirectGraphTC0011() throws Exception {
		String testName = "DirectGraphTC0011";
		this.run(testName, true);
	}
	
	@Test
	public void testDirectGraphTC0011b() throws Exception {
		String testName = "DirectGraphTC0011b";
		this.run(testName, true);
	}
	
	@Test
	public void testDirectGraphTC0012() throws Exception {
		String testName = "DirectGraphTC0012";
		this.run(testName, true);
	}
	
	@Test
	public void testDirectGraphTC0013() throws Exception {
		String testName = "DirectGraphTC0013";
		this.run(testName, true);
	}
	
	@Test
	public void testDirectGraphTC0014() throws Exception {
		String testName = "DirectGraphTC0014";
		this.run(testName, true);
	}
	
	@Test
	public void testDirectGraphTC0015() throws Exception {
		String testName = "DirectGraphTC0015";
		this.run(testName, true);
	}
	
	@Test
	public void testDirectGraphTC0016() throws Exception {
		String testName = "DirectGraphTC0016";
		this.run(testName, true);
	}
	
	@Test
	public void testDirectGraphTC0017() throws Exception {
		String testName = "DirectGraphTC0017";
		this.run(testName, true);
	}
	
	@Test
	public void testDirectGraphTC0018() throws Exception {
		String testName = "DirectGraphTC0018";
		this.run(testName, true);
	}
	
	@Test
	public void testDirectGraphTC0019() throws Exception {
		String testName = "DirectGraphTC0019";
		this.run(testName, true);
	}
	
	@Test
	public void testDirectGraphTC0020() throws Exception {
		String testName = "DirectGraphTC0020";
		this.run(testName, true);
	}

}
