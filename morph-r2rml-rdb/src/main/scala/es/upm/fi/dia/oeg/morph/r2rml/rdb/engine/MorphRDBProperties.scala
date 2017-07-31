package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import es.upm.fi.dia.oeg.morph.base.MorphProperties;
import es.upm.fi.dia.oeg.morph.base.Constants;
import org.slf4j.LoggerFactory

class MorphRDBProperties extends MorphProperties {
	def setNoOfDatabase(x:Int) = {this.noOfDatabase=x}
	def setDatabaseUser(dbUser:String) = {this.databaseUser=dbUser}
	def setDatabaseURL(dbURL:String) = {this.databaseURL=dbURL}
	def setDatabasePassword(dbPassword:String) = {this.databasePassword=dbPassword}
	def setDatabaseName(dbName:String) = {this.databaseName=dbName}
	def setDatabaseDriver(dbDriver:String) = {this.databaseDriver=dbDriver}
	def setDatabaseType(dbType:String) = {this.databaseType=dbType}
	def setMappingDocumentFilePath(mdPath:String) = {this.mappingDocumentFilePath=mdPath}

	override def readConfigurationFile(pConfigurationDirectory:String , configurationFile:String) = {
			super.readConfigurationFile(pConfigurationDirectory, configurationFile);

			this.noOfDatabase = this.readInteger(Constants.NO_OF_DATABASE_NAME_PROP_NAME, 0);
			if(this.getProperty(Constants.JDCB_DRIVER_PROP_NAME) != null) {
				this.noOfDatabase = 1;
			}

			if(this.noOfDatabase != 0 && this.noOfDatabase != 1) {
				throw new Exception("Only zero or one database is supported.");
			}

			for(i <- 0 until noOfDatabase) {
				val propertyDatabaseDriver = Constants.DATABASE_DRIVER_PROP_NAME + "[" + i + "]";
				this.databaseDriver = this.getProperty(propertyDatabaseDriver);
				if(this.databaseDriver == null) { 
					this.databaseDriver = this.getProperty(Constants.JDCB_DRIVER_PROP_NAME); 
				}

				val propertyDatabaseURL = Constants.DATABASE_URL_PROP_NAME + "[" + i + "]";
				this.databaseURL = this.getProperty(propertyDatabaseURL);
				if(this.databaseURL == null) { 
					this.databaseURL = this.getProperty(Constants.JDCB_URL_PROP_NAME); 
				}

				val propertyDatabaseName= Constants.DATABASE_NAME_PROP_NAME + "[" + i + "]";
				this.databaseName = this.getProperty(propertyDatabaseName);
				if(this.databaseName == null) { 
					this.databaseName = this.getProperty(Constants.JDCB_DATABASE_PROP_NAME); 
				}

				val propertyDatabaseUser = Constants.DATABASE_USER_PROP_NAME + "[" + i + "]";
				this.databaseUser = this.getProperty(propertyDatabaseUser);
				if(this.databaseUser == null) { 
					this.databaseUser = this.getProperty(Constants.JDCB_USERNAME_PROP_NAME); 
				}

				val propertyDatabasePassword = Constants.DATABASE_PWD_PROP_NAME  + "[" + i + "]";
				this.databasePassword = this.getProperty(propertyDatabasePassword);
				if(this.databasePassword == null) { 
					this.databasePassword = this.getProperty(Constants.JDCB_PASSWORD_PROP_NAME); 
				}

				val propertyDatabaseType = Constants.DATABASE_TYPE_PROP_NAME  + "[" + i + "]";
				this.databaseType = this.getProperty(propertyDatabaseType);
				if(this.databaseType == null) { 
					this.databaseType = this.getProperty(Constants.DRIVER_PROP_NAME); 
				}

				//			if(this.databaseType == null) {
				//				this.databaseType = Constants.DATABASE_MYSQL;
				//			}

				val propertyDatabaseTimeout = Constants.DATABASE_TIMEOUT_PROP_NAME  + "[" + i + "]";
				val timeoutPropertyString = this.getProperty(propertyDatabaseTimeout);
				if(timeoutPropertyString != null && !timeoutPropertyString.equals("")) {
					this.databaseTimeout = Integer.parseInt(timeoutPropertyString.trim());
				}


			}



	
	}
}

object MorphRDBProperties {
  val logger = LoggerFactory.getLogger(this.getClass());

	def apply(pConfigurationDirectory:String , configurationFile:String) : MorphRDBProperties = {
			val properties = new MorphRDBProperties();
			properties.readConfigurationFile(pConfigurationDirectory, configurationFile);
			properties
	}
}