package es.upm.fi.dia.oeg.morph.base.sql

import Zql.ZExpression
import Zql.ZExp

class SQLExpression(arg0:String , arg1:ZExp ) extends ZExpression(arg0:String , arg1:ZExp ) {
	var operatorPlace:SQLExpression.OPERATOR_PLACE.Value  = null ;
	
	def setOperatorPlace(operatorPlace:SQLExpression.OPERATOR_PLACE.Value) =  {
		this.operatorPlace = operatorPlace;
	}

	override def toString() : String = {
		val operands = this.getOperands();
		if(operands != null && operands.size() == 2 
		    && this.operatorPlace == SQLExpression.OPERATOR_PLACE.INFIX ) {
			val operand0 = operands.get(0);
			val operand1 = operands.get(1);
			operand0 + " " + this.getOperator() + " " + operand1; 
		} else {
			super.toString();
		}
	}


}

object SQLExpression {
	object OPERATOR_PLACE extends Enumeration {
		type OPERATOR_PLACE = Value
		val PREFIX, INFIX, SUFFIX = Value
	}  
}