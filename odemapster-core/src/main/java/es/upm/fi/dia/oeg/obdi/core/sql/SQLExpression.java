package es.upm.fi.dia.oeg.obdi.core.sql;

import java.util.Vector;

import Zql.ZExp;
import Zql.ZExpression;

public class SQLExpression extends ZExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public enum OPERATOR_PLACE {PREFIX, INFIX, SUFFIX};
	private OPERATOR_PLACE operatorPlace;
	
	public SQLExpression(String arg0, ZExp arg1) {
		super(arg0, arg1);
		// TODO Auto-generated constructor stub
	}

	public void setOperatorPlace(OPERATOR_PLACE operatorPlace) {
		this.operatorPlace = operatorPlace;
	}

	@Override
	public String toString() {
		Vector<ZExp> operands = this.getOperands();
		if(operands != null && operands.size() == 2 && this.operatorPlace == OPERATOR_PLACE.INFIX ) {
			ZExp operand0 = operands.get(0);
			ZExp operand1 = operands.get(1);
			return operand0 + " " + this.getOperator() + " " + operand1; 
		} else {
			return super.toString();
		}
	}

	
}
