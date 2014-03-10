package es.upm.fi.dia.oeg.morph.base.sql

import Zql.ZFromItem
import es.upm.fi.dia.oeg.morph.base.Constants
import Zql.ZExp
import java.util.Random

class SQLFromItem(fullName:String , val form:Constants.LogicalTableType.Value) 
extends ZFromItem(fullName) with SQLLogicalTable {
	var joinType:String=null;
	var onExp:ZExp =null;
	

	
	override def generateAlias() :String ={
		//return R2OConstants.VIEW_ALIAS + this.hashCode();
		if(super.getAlias() == null) {
			super.setAlias(Constants.VIEW_ALIAS + new Random().nextInt(10000));
		}
		super.getAlias();
	}

	override def toString() : String = {
	  this.print(true)
	}
	
	def setJoinType(joinType:String ) = { this.joinType = joinType;	}

	def setOnExp(onExp:ZExp ) = { this.onExp = onExp; }
	
	def getOnExp() : ZExp = { this.onExp; }
	
	def getJoinType() : String = { joinType; }

	override def print(withAlias:Boolean ) : String  = {
		val alias = this.getAlias();
		val result = if(alias != null && withAlias) {
			this.setAlias("");
			val resultAux = if(this.form == Constants.LogicalTableType.TABLE_NAME) {
				val tableName = super.toString().trim();
				tableName + " " + alias;
			} else {
				"(" + super.toString() + ") " + alias;
			}
			resultAux
		} else {
			val tableName = super.toString();
			tableName;
		}		
		
		
		if(alias != null) { this.setAlias(alias); }
		  
		return result;		
	}

//	def setDatabaseType(dbType:String ) = { this.dbType = dbType; }

//	def getDbType() : String = { dbType; }
}

object SQLFromItem {
	def apply(fullName:String, form:Constants.LogicalTableType.Value, dbType:String) = {
	  val sqlFromItem = new SQLFromItem(fullName, form);
	  sqlFromItem.databaseType = dbType;
	  sqlFromItem;
	}
}