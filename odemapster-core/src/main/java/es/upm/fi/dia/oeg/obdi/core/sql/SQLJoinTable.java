package es.upm.fi.dia.oeg.obdi.core.sql;

import java.util.Collection;
import java.util.Vector;

import Zql.ZExp;
import Zql.ZExpression;
import Zql.ZFromItem;
import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.morph.base.MorphSQLUtility;

@SuppressWarnings("serial")
public class SQLJoinTable extends ZFromItem {
//	private static Logger logger = Logger.getLogger(SQLJoinTable.class);
	
	private SQLLogicalTable joinSource;
	private String joinType;
	private ZExpression onExpression;

	public SQLJoinTable(SQLLogicalTable joinSource,String joinType, ZExpression onExp) {
		super();
		this.joinType = joinType;
		this.joinSource = joinSource;
		this.onExpression = onExp;
	}

	public SQLJoinTable(SQLLogicalTable joinSource) {
		this(joinSource, null, null);
	}
	
	public void setOnExpression(ZExpression onExp) {
		this.onExpression = onExp;
	}

	public void addOnExpression(ZExpression onExp2) {
		Collection<ZExp> expressionsList = new Vector<ZExp>();
		if(this.onExpression != null) {expressionsList.add(this.onExpression);}
		if(onExp2 != null) {expressionsList.add(onExp2);}
		this.onExpression = MorphSQLUtility.combineExpresions(expressionsList
				, Constants.SQL_LOGICAL_OPERATOR_AND());
	}
	
	public void setJoinType(String joinType) {
		this.joinType = joinType;
	}

	public void setJoinSource(SQLFromItem fromItem) {
		this.joinSource = fromItem;
	}

	@Override
	public String toString() {
		StringBuffer result = new StringBuffer();
		if(this.joinType != null) {
			result.append(this.joinType + " JOIN ");	
		}
		result.append(this.joinSource);
		if(this.onExpression != null) {
			result.append(" ON " + this.onExpression);	
		}
		
		return result.toString();
	}

	public SQLLogicalTable getJoinSource() {
		return joinSource;
	}

	public String getJoinType() {
		return joinType;
	}

	public ZExpression getOnExpression() {
		return onExpression;
	}
}
