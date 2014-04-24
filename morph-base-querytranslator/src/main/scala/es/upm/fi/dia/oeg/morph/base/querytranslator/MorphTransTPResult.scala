package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions._

import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.base.sql.SQLQuery
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.sql.SQLFromItem
import es.upm.fi.dia.oeg.morph.base.engine.QueryTranslationOptimizer
import Zql.ZUpdate
import es.upm.fi.dia.oeg.morph.base.Constants

class MorphTransTPResult(val alphaResult:MorphAlphaResult
    , val condSQLResult:MorphCondSQLResult, val prSQLResult:MorphPRSQLResult) {

	val logger = Logger.getLogger(this.getClass());
  
	def toQuery(optimizer:QueryTranslationOptimizer, databaseType:String) : IQuery = {
		val alphaResult = this.alphaResult
		val alphaSubject = alphaResult.alphaSubject;
		val alphaPredicateObjects = alphaResult.alphaPredicateObjects;
		val prSQLResult = this.prSQLResult;
		val prSQL = prSQLResult.toList;
		val condSQLResult = this.condSQLResult;
		val condSQL = { if(condSQLResult != null) { condSQLResult.toExpression; } 
			else { null }
		}
		
		val resultAux = {
			//don't do subquery elimination here! why?
			if(optimizer != null && optimizer.subQueryElimination) {
				try {
					SQLQuery.createQuery(alphaSubject, alphaPredicateObjects
					    , prSQL, condSQL, databaseType);
				} catch {
				  case e:Exception => {
					val errorMessage = "error in eliminating subquery!";
					logger.error(errorMessage);
					null;							    
				  }
				}
			} else { null } 					  
		}


		if(resultAux == null) { //without subquery elimination or error occured during the process
			val resultAux2 = new SQLQuery(alphaSubject);
			if(alphaPredicateObjects != null) {
				for(alphaPredicateObject <- alphaPredicateObjects) {
				  alphaSubject match {
				    case _:SQLFromItem => {
				      resultAux2.addFromItem(alphaPredicateObject);//alpha predicate object}
				    }
				    case _:SQLQuery => {
						val onExpression = alphaPredicateObject.onExpression;
						alphaPredicateObject.onExpression = null;
						resultAux2.addFromItem(alphaPredicateObject);//alpha predicate object
						resultAux2.pushFilterDown(onExpression);							      
				    }
				    case _ => {
				      resultAux2.addFromItem(alphaPredicateObject);//alpha predicate object
				    }
				  }
				}					
			}
			resultAux2.setSelectItems(prSQL);
			resultAux2.setWhere(condSQL);
			resultAux2;
		} else {
		  resultAux
		}	  
	}

	def toUpdate() = {
	  val alphaSubject = this.alphaResult.alphaSubject;
	  val tableName = alphaSubject.print(true); 
	  val zUpdate = new ZUpdate(tableName);
	  //zUpdate.addColumnUpdate("col1", Constants.SQL_EXPRESSION_TRUE);
	  
	  val condSQLSubject = this.condSQLResult.condSQLSubject;
	  zUpdate.addWhere(condSQLSubject);
	  
	  val condSQLPredicateObjects = this.condSQLResult.condSQLPredicateObjects;
	  val mapSetValue = this.condSQLResult.condSQLPredicateObjects.flatMap(x => {
	    if(x.getOperator().equals("=")) {
	      Some(x.getOperand(0).toString() -> x.getOperand(1))
	    }
	    else { None }
	  }).toMap
	  
	  mapSetValue.foreach { case(k,v) => { zUpdate.addColumnUpdate(k, v)} } 
	  
	  zUpdate
	}
}