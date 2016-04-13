package es.upm.fi.dia.oeg.morph.example;
import java.sql.*;

import org.apache.log4j.Logger;
import org.h2.tools.Csv;

import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBUtility;

public class CSVLoader {
	private static Logger logger = Logger.getLogger(CSVLoader.class);
	
	public static void main(String[] args) throws Exception {
		String csv_file= "examples-csv/Student.csv";
		Class.forName("org.h2.Driver");
		
		//String temporary_database_name = "morph-csv-db4";
        //Connection conn = DriverManager.getConnection("jdbc:h2:mem:" + temporary_database_name, "sa", "");
        Connection conn = DriverManager.getConnection("jdbc:h2:~/morph-csv21", "sa", "");
        // add application code here
        //conn.setAutoCommit(true);

        
		MorphRDBUtility.loadCSVFile(conn, csv_file);  
		conn.close();
	}
	
//    public static void loadCSVFile(Connection conn, String csv_file) throws Exception {
//    	String csv_file_name = "";
//    	//String csv_file_extension = "";
//    	
//    	if(csv_file == null) {
//    		throw new Exception("CSV file has not been defined.");
//    	}
//    	int lastDotChar = csv_file.lastIndexOf(".");
//    	if(lastDotChar == -1) {
//    		throw new Exception("CSV file does not have any extension.");
//    	}
//    	//csv_file_extension = csv_file.substring(lastDotChar + 1, csv_file.length());
//    	csv_file_name = csv_file.substring(0, lastDotChar);
//        
//    	String createTableString = "CREATE TABLE " + csv_file_name + " AS SELECT * FROM CSVREAD('" + csv_file + "');";
//    	Statement stmt = conn.createStatement();
//        
//        try {
//            stmt.execute(createTableString);
//            conn.commit();
//            logger.info("The table  " + csv_file_name + " was created successfully");
//        } catch (SQLException sqle) {
//            logger.error("Error while creating the table  " + csv_file_name);
//            sqle.printStackTrace();
//        }
//    }
    
    
}
