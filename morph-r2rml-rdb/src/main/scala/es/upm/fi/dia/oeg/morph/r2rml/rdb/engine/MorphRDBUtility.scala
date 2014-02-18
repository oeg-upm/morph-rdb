package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import java.util.Collection
import es.upm.fi.dia.oeg.morph.base.Constants
import Zql.ZExpression
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLJoinCondition
import scala.collection.JavaConversions._
import Zql.ZConstant

class MorphRDBUtility {

}

object MorphRDBUtility {
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
}