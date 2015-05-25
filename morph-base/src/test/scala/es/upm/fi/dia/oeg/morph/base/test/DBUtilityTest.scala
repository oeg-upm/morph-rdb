package es.upm.fi.dia.oeg.morph.base.test

import java.sql.DriverManager
import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.DBUtility
import java.util.Properties

object DBUtilityTest extends App {
  println("Testing DBUtility ...");
  val driverString = "oracle.jdbc.OracleDriver";
  val url = "jdbc:oracle:thin:@localhost:1521:xe";
  val username = "PCON";
  val password = "PCON";
  val databaseName = "";
  
  val prop = new Properties();
  prop.put("ResultSetMetaDataOptions", "1");
  prop.put("user", username);
  //prop.put("database", databaseName);
  prop.put("password", password);
  prop.put("autoReconnect", "true");
  
  Class.forName(driverString);
  
  //val conn = DriverManager.getConnection(url, prop);
      
  //DriverManager.registerDriver(new oracle.jdbc.OracleDriver);
  //val conn = DriverManager.getConnection(url, username, password);
  
  DBUtility.getLocalConnection(username, databaseName, password, driverString, url
      , "DBUtilityTest");
  
  println("Bye!");
  
  
}