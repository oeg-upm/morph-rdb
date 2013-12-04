package es.upm.fi.dia.oeg.upm.morph.sql

import Zql.ZConstant
import Zql.ZSelectItem
import es.upm.fi.dia.oeg.morph.base.Constants
import org.apache.log4j.Logger

class MorphSQLConstant(v: String, typ: Int, val dbType : String, val columnType : String
    , val schema:String, val table:String, val column:String) 
extends ZConstant(v, typ) {
	//var columnType : String = null;
	//var dbType : String = null;
	

	
//	override def toString() : String = {
//	  var result : String = null;
//	  
//	  if(this.columnType == null) {
//	    result = super.toString;
//	  } else {
//	    result = super.toString + "::" + this.columnType;
//	  }
//	  
//	  result;
//	}
	
//	def setColumnType(columnType : String) = {
//	  this.columnType = columnType;
//	}
	
//	def setDbType(dbType : String) = {
//	  this.dbType = dbType;
//	}
	
	override def toString() = {
		val result = {
			this.dbType match {
			  case Constants.DATABASE_MONETDB => {
				  val selectItem = new ZSelectItem(this.getValue());
				  
				  var selectItemStringList : List[String] = Nil;
				  if(selectItem.getTable() != null) {
				    selectItemStringList = selectItemStringList ::: List("\"" + selectItem.getTable() + "\"") 
				  }
				  if(selectItem.getColumn() != null) {
				    selectItemStringList = selectItemStringList ::: List("\"" + selectItem.getColumn() + "\"")
				  }
				  
				  val monetdbResult = selectItemStringList.mkString(".");
//				  
//				  val table = "\"" + selectItem.getTable() + "\"";
//				  val column = "\"" + selectItem.getColumn() + "\"";
//				  val monetdbResult = table + "." + column;
				  monetdbResult;
			  }
			  case Constants.DATABASE_POSTGRESQL => {
				  val postgresqlResult = {
					if(this.columnType == null) {
						super.toString;
					} else {
						super.toString + "::" + this.columnType;
					}
				  }
				  postgresqlResult
			  }
			  case _ => {
			    super.toString();
			  }
			}		  
		}
		result;
	}
	
}

object MorphSQLConstant {
	val logger = Logger.getLogger("MorphSQLConstant");

//  def createMonetDBColumn(value: String, typ: Int) = {
//	  val monetdbColumn = new MorphSQLConstant(value, typ);
//	  monetdbColumn.dbType = Constants.DATABASE_MONETDB;
//	  monetdbColumn
//  }

	def apply(zConstant : ZConstant) : MorphSQLConstant = {
	  
	  zConstant match {
	    case sqlConstant:MorphSQLConstant => {
	      val result = this(zConstant.getValue(), zConstant.getType()
	          , sqlConstant.dbType, sqlConstant.columnType);
		  result
	    }
	    case _ => {
	      val result = this(zConstant.getValue(), zConstant.getType(), null, null);
	      result
	    }
	  }
	}


	def apply(v: String, typ: Int) : MorphSQLConstant = {
		this(v, typ, null, null);
	}

		
	def apply(v: String, typ: Int, pDBType:String) : MorphSQLConstant = {
		this(v, typ, pDBType, null);
	}
	
	def apply(v: String, typ: Int, pDBType:String , pColumnType:String) : MorphSQLConstant = {
	  val result = {
		  typ match {
		    case ZConstant.COLUMNNAME => {
				val splitColumns = MorphSQLSelectItem.splitAndClean(v, pDBType);
				val splitColumnsSize = splitColumns.size;
				var column:String = null;
				var table:String = null;
				var schema:String = null;
				
				splitColumnsSize match {
				  case 1 => { //nr
				    column = splitColumns(0); 
				  }
				  case 2 => { //product.nr
				    table = splitColumns(0);
				    column = splitColumns(1);
				  }
				  case 3 => { //benchmark.product.nr
					schema = splitColumns(0);
					table = splitColumns(1);
					column = splitColumns(2);
				  }
				  case 4 => { //benchmark.dbo.product.nr
					schema = splitColumns(0);
					table = splitColumns(1) + "." + splitColumns(2);
					column = splitColumns(3);
				  }
				  case _ => {
					  logger.warn("Invalid input")
				  }
				}
				
				val columnType = {
					if(pColumnType == null) {
						val splitColumnType = column.split("::");
						if(splitColumnType.length > 1) {
							splitColumnType(1);	
						} else {
						  null
						}			  
					} else {
					  pColumnType
					}
				}
				
				val resultAux = new MorphSQLConstant(v, typ, pDBType, columnType
				    , schema, table, column);
				resultAux			      
		    }
		    case _ => {
		    	val resultAux = new MorphSQLConstant(v, typ, pDBType, null, null, null, null);
		    	resultAux
		    }
		  }	    
	  }
		
	  result
	}
}