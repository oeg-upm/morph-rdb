package es.upm.fi.dia.oeg.obdi.core.querytranslator;

import Zql.ZExpression;

public class CondSQLResult {
	private ZExpression expression;
	
	public CondSQLResult(ZExpression expression) {
		super();
		this.expression = expression;
	}

	public ZExpression getExpression() {
		return expression;
	}

	@Override
	public String toString() {
		return "CondSQL [toString()=" + this.expression + "]";
	}

	
	
	
}
