package es.upm.fi.dia.oeg.morph.base.sql

import Zql.ZOrderBy
import es.upm.fi.dia.oeg.morph.base.Constants
import java.util.Random
import Zql.ZGroupBy
import Zql.ZSelectItem
import Zql.ZExp

class SQLUnion(val unionQueries:Iterable[SQLQuery]) extends IQuery {
	var alias:String = null;
//	var databaseType:String = null;
	var orderByConditions:List[ZOrderBy] = List.empty;
	var joinType:String  = null;
	var onExp:ZExp=null;
	var slice:Long = -1;
	var offset:Long = -1;
	var groupBy:ZGroupBy  = null;
	
	def getAlias() : String = this.alias;
	
	def setAlias(alias:String) = {this.alias = alias}

	def setOrderBy(orderByConditions:List[ZOrderBy] ) = {
	  this.orderByConditions = orderByConditions;
	}
	
	def getOrderByConditions():List[ZOrderBy] = this.orderByConditions;
	
//	def add(newQuery:IQuery ) = {
//		if(this.unionQueries == null) {
//			this.unionQueries = List.empty;
//		}
//		
//		if(newQuery != null) {
//		  newQuery match {
//		    case newQuerySQL:SQLQuery => {
//				this.unionQueries = List(newQuerySQL);	
//			} 
//		    case newQueryUnion:SQLUnion => {
//				val newQueries = newQueryUnion.unionQueries;
//				this.unionQueries = this.unionQueries.toList ::: newQueries.toList; 
//			}		    
//		  }
//		}
//	}

	def print() : String = {
		var result:String = null;
		val unionString = "\n" + Constants.SQL_KEYWORD_UNION + "\n" ;
		
		if(this.unionQueries != null) {
			result = this.unionQueries.mkString("", unionString, "\n")
		}
		
		if(this.orderByConditions != null && this.orderByConditions.size > 0) {
			val orderByString = this.orderByConditions.mkString(
			    Constants.SQL_KEYWORD_ORDER_BY + " ", ", ", " ")
			result = result + orderByString;
		}
			
		if(this.slice > 0) {
			result = result + "\n" + "LIMIT " + this.slice; 
		}
		
		if(this.offset> 0) {
			result = result + "\n" + "OFFSET " + this.offset; 
		}

		
		result;	  
	}
	
	override def toString() :String = {
		this.print();
	}

	def generateAlias() : String = {
		if(this.alias == null) {
			this.alias = Constants.VIEW_ALIAS + new Random().nextInt(10000);
		}
		this.alias;
	}

	def getSelectItemAliases() : List[String] = {
		this.unionQueries.toList(0).getSelectItemAliases;
	}

	def cleanupSelectItems() = {
		if(this.unionQueries != null) {
			this.unionQueries.foreach(sqlQuery => sqlQuery.cleanupSelectItems());
		}
	}


	def getSelectItems() : List[ZSelectItem]  = {
	  val firstQuerySelectItems = this.unionQueries.toList(0).getSelectItems();
	  
		val result = firstQuerySelectItems.map(selectItem => {
			val selectItemAlias = selectItem.getAlias();
			
			
			val newColumnName = if(selectItemAlias == null || selectItemAlias.equals("")) {
				if(selectItem.isExpression()) {
					selectItem.getExpression().toString();
				} else {
					selectItem.getColumn();	
				}
			} else {
				selectItemAlias;
			}
			
			val columnType = selectItem match {
			  case morphSQLSelectItem:MorphSQLSelectItem => {
					morphSQLSelectItem.columnType;
				} 
			  case _ => { null; }
			} 
			
			val newSelectItem = MorphSQLSelectItem(newColumnName, null
			    , this.getDatabaseType(), columnType);
			newSelectItem;
		})
		
		result.toList;
	}

	def setSelectItems(newSelectItems:List[ZSelectItem] ) = {
	  this.unionQueries.foreach(query => query.setSelectItems(newSelectItems));
	}

	def addWhere(newWhere:ZExp ) = {
	  this.unionQueries.foreach(query =>query.addWhere(newWhere) );
	}

	def cleanupOrderBy() = {
	  this.unionQueries.foreach(query => query.cleanupOrderBy());
	}

//	def getDatabaseType() : String  = {
//		databaseType;
//	}

	def setJoinType(joinType:String ) = {
		this.joinType = joinType;
	}

	def setOnExp(onExp:ZExp ) = {
		this.onExp = onExp;
	}

	def getJoinType() : String  = {
		this.joinType;
	}

	def getOnExp() : ZExp  = {
		this.onExp;
	}

	def print(withAlias:Boolean ) : String = {
		val result = if(withAlias) {
			this.toString();
		} else {
			val alias = this.getAlias();
			this.setAlias("");
			val resultAux = this.toString();
			if(alias != null) {
			  this.setAlias(alias);
			}
			resultAux;
		}
		result;
	}


	def setDistinct(distinct:Boolean ) = {
		// TODO Auto-generated method stub
		
	}

	def getDistinct() : Boolean = {
		// TODO Auto-generated method stub
		false;
	}

	def pushProjectionsDown(pushedProjections:List[ZSelectItem] ) = {
		val unionQueryAlias = this.getAlias();
		
		for(sqlQuery <- this.unionQueries) {
			val sqlQueryAlias = sqlQuery.getAlias();
			sqlQuery.setAlias(unionQueryAlias);
			sqlQuery.pushProjectionsDown(pushedProjections);
			if(sqlQueryAlias != null) {
				sqlQuery.setAlias(sqlQueryAlias);	
			}
		}
	}

	def pushOrderByDown(pushedProjections:List[ZSelectItem] ) = {
		if(this.orderByConditions != null) {
			val mapInnerAliasSelectItem = pushedProjections.map(selectItem => {
			  	val selectItemColumn = selectItem.getColumn();
				(selectItemColumn -> selectItem);
			}).toMap;
			
			val newOrderByCollection = MorphSQLUtility.pushOrderByDown(
			    this.orderByConditions, mapInnerAliasSelectItem);
			this.setOrderBy(newOrderByCollection);			
		}
	}

	def pushFilterDown(pushedFilter:ZExp ) = {
	  this.unionQueries.foreach(sqlQuery => {
			val mapInnerAliasSelectItem = 
					SQLQuery.buildMapAliasSelectItemAux(this.getAlias(), sqlQuery.getSelectItems());
			val newExpression = sqlQuery.pushExpDown(pushedFilter, mapInnerAliasSelectItem);
			sqlQuery.addWhere(newExpression);
	  });
	}

	def setSlice(slice:Long ) = {
		this.slice = slice;
		
	}

	def setOffset(offset:Long ) = {
		this.offset = offset;
	}

	def addSelectItems(newSelectItems:List[ZSelectItem] ) {
	  this.unionQueries.foreach(query => query.addSelectItems(newSelectItems))
	}

	def addGroupBy(groupBy:ZGroupBy ) = {
		this.groupBy = groupBy;
	}

	def getGroupBy() : ZGroupBy = {
		this.groupBy;
	}

	def setGroupBy(groupBy:ZGroupBy ) =  {
		this.groupBy = groupBy;
	}

	def pushGroupByDown() = {
		//TODO
	}

//	override def setDatabaseType(dbType:String ) =  {
//		this.databaseType = dbType;
//	}

}

object SQLUnion {
	def apply(newUnionQueries:Iterable[_ <: IQuery]) : SQLUnion = {
		val queries = newUnionQueries.flatMap(newUnionQuery => {
			newUnionQuery match {
			  case sqlQuery:SQLQuery => {
			    Some(List(sqlQuery))
			  }
			  case sqlUnion:SQLUnion => {
			    Some(sqlUnion.unionQueries)
			  }
			  case _ => { None }
			}
		});
		
		new SQLUnion(queries.flatten);
	} 
}