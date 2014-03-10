package es.upm.fi.dia.oeg.morph.base.sql

import scala.collection.mutable.LinkedList
import Zql.ZGroupBy
import Zql.ZSelectItem
import Zql.ZOrderBy
import Zql.ZExp

trait IQuery extends SQLLogicalTable {
//	def getDatabaseType() : String;
//	def setDatabaseType(databaseType:String);
	
//	def generateAlias():String ;
//	def setAlias(alias:String ) = {this.alias=alias};
//	def getAlias():String = this.alias
	
	//SELECT ITEM IS AN ORDERED LIST
	def setSelectItems(newSelectItems:List[ZSelectItem]);
	def addSelectItems(newSelectItems:List[ZSelectItem] );
	def getSelectItems():List[ZSelectItem];
	def getSelectItemAliases():List[String];
	def cleanupSelectItems();
	
	
	//ORDER BY IS AN ORDERED LIST
	def cleanupOrderBy();
	def setOrderBy(orderByConditions:List[ZOrderBy] );
	def getOrderByConditions():List[ZOrderBy] ;
	
	def setGroupBy(groupBy:ZGroupBy);
	def getGroupBy():ZGroupBy ;
	def addGroupBy(groupBy:ZGroupBy );
	
	def addWhere(newWhere:ZExp );
	
	def setDistinct(distinct:Boolean );
	def getDistinct():Boolean ;
	
	def setSlice(slice:Long );
	def setOffset(offset:Long );
	

	def pushProjectionsDown(pushedProjections:List[ZSelectItem] );
	def pushFilterDown(pushedFilter:ZExp );
	def pushOrderByDown(pushedProjections:List[ZSelectItem] );
	def pushGroupByDown();
}