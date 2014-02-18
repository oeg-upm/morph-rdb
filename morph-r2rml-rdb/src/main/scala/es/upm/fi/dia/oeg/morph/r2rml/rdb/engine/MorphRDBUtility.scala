package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import java.util.Collection
import es.upm.fi.dia.oeg.morph.base.Constants
import Zql.ZExpression
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLJoinCondition
import scala.collection.JavaConversions._
import Zql.ZConstant
import Zql.ZQuery
import java.io.ByteArrayInputStream
import Zql.ZqlParser
import es.upm.fi.dia.oeg.obdi.core.sql.SQLQuery
import org.apache.log4j.Logger

class MorphRDBUtility {

}

object MorphRDBUtility {
	val logger = Logger.getLogger(this.getClass().getName());
	
	def generateJoinCondition(joinConditions:Collection[R2RMLJoinCondition] 
	, parentTableAlias:String, joinQueryAlias:String , dbType:String ) : ZExpression = {
		var onExpression:ZExpression = null;
		val enclosedCharacter = Constants.getEnclosedCharacter(dbType);
		
		if(joinConditions != null) {
			for(joinCondition <- joinConditions) {
				var childColumnName = joinCondition.getChildColumnName();
				childColumnName = childColumnName.replaceAll("\"", enclosedCharacter);
				childColumnName = parentTableAlias + "." + childColumnName;
				val childColumn = new ZConstant(childColumnName, ZConstant.COLUMNNAME);

				var parentColumnName = joinCondition.getParentColumnName();
				parentColumnName = parentColumnName.replaceAll("\"", enclosedCharacter);
				parentColumnName = joinQueryAlias + "." + parentColumnName;
				val parentColumn = new ZConstant(parentColumnName, ZConstant.COLUMNNAME);
				
				val joinConditionExpression = new ZExpression("=", childColumn, parentColumn);
				if(onExpression == null) {
					onExpression = joinConditionExpression;
				} else {
					onExpression = new ZExpression("AND", onExpression, joinConditionExpression);
				}
			}
		}
		
		return onExpression;
	}
	
	def toZQuery(sqlString:String ) : ZQuery = {
		try {
			//sqlString = sqlString.replaceAll(".date ", ".date2");
			val bs = new ByteArrayInputStream(sqlString.getBytes());
			val parser = new ZqlParser(bs);
			val statement = parser.readStatement();
			val zQuery = statement.asInstanceOf[ZQuery];
			zQuery;
		} catch {
		  case e:Exception => {
			val errorMessage = "error parsing query string : \n" + sqlString; 
			logger.error(errorMessage);
			logger.error("error message = " + e.getMessage());
			throw e;		    
		  }
		  case e:Error => {
			val errorMessage = "error parsing query string : \n" + sqlString;
			logger.error(errorMessage);
			throw new Exception(errorMessage);
		}		  
		} 
	}

	def toSQLQuery(sqlString:String ) : SQLQuery = {
		val zQuery = this.toZQuery(sqlString);
		val sqlQuery = new SQLQuery(zQuery);
		sqlQuery;
	}	
}