package es.upm.fi.dia.oeg.obdi.core.sql;

import Zql.ZSelectItem;

public class PostgreSQLSelectItem extends ZSelectItem {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String datatype = null;
	
	public PostgreSQLSelectItem() {
		// TODO Auto-generated constructor stub
	}

	public PostgreSQLSelectItem(String arg0) {
		super(arg0);
		// TODO Auto-generated constructor stub
	}

	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}

	@Override
	public String toString() {
		String result = null;
		
		if(this.datatype == null) {
			result = super.toString();
		} else {
			String expString = this.getExpression().toString();
			expString += "::" + this.datatype; 
			result = expString;
			
			String alias = this.getAlias();  
			if(alias != null && !alias.equals("")) {
				result += " AS " + alias;
			}
		}
		return result;
	}

	
}
