package es.upm.fi.dia.oeg.obdi.core.sql;

import java.util.Random;

import Zql.ZExp;
import Zql.ZFromItem;
import es.upm.fi.dia.oeg.morph.base.Constants;


public class SQLFromItem extends ZFromItem implements SQLLogicalTable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public enum LogicalTableType {TABLE_NAME, QUERY_STRING};
	
//	public static int FORM_TABLE = ZAliasedName.FORM_TABLE;
//	public static int FORM_QUERY = FORM_TABLE + ZAliasedName.FORM_COLUMN;  
//	private int form;
	
	//private String alias;
	private LogicalTableType form;
	private String joinType;
	private ZExp onExp;
	private String dbType;
	
	public SQLFromItem(String fullName, LogicalTableType form, String dbType) {
		super(fullName);
		this.form = form;
		this.dbType = dbType;
	}

	public LogicalTableType getForm() {
		return form;
	}

	
	public String generateAlias() {
		//return R2OConstants.VIEW_ALIAS + this.hashCode();
		if(super.getAlias() == null) {
			super.setAlias(Constants.VIEW_ALIAS() + new Random().nextInt(10000));
		}
		return super.getAlias();
	}

	@Override
	public String toString() {
		String result = "";
		
		String alias = this.getAlias();
		if(alias != null) {
			this.setAlias("");
			if(this.form == LogicalTableType.TABLE_NAME) {
				String enclosedCharacter = Constants.getEnclosedCharacter(dbType);
				String tableName;
				if(enclosedCharacter == null || enclosedCharacter.equals("")) {
					tableName = super.toString().trim();
				} else {
					tableName = enclosedCharacter + super.toString().trim() + enclosedCharacter;
				}
				//tableName = R2RMLUtility.replaceNameWithSpaceChars(tableName);
				result = tableName + " " + alias;
			} else {
				result = "(" + super.toString() + ") " + alias;
			}
			
			this.setAlias(alias);
		} else {
			String tableName = super.toString();
			//tableName = R2RMLUtility.replaceNameWithSpaceChars(tableName);
			result = tableName;
		}		
		
		return result;
	}
	
	public void setJoinType(String joinType) {
		this.joinType = joinType;
	}

	public void setOnExp(ZExp onExp) {
		this.onExp = onExp;
	}
	
	public ZExp getOnExp() {
		return this.onExp;
	}
	
	public String getJoinType() {
		return joinType;
	}

	public String print(boolean withAlias) {
		String result;
		if(withAlias) {
			result = this.toString();
		} else {
			String alias = this.getAlias();
			if(alias == null || alias.equals("")) {
				result = this.toString();
			} else {
				this.setAlias("");
				result = this.toString();
				this.setAlias(alias);
			}
		}
		return result;
	}

	public void setDbType(String dbType) {
		this.dbType = dbType;
	}

	public String getDbType() {
		return dbType;
	}
}
