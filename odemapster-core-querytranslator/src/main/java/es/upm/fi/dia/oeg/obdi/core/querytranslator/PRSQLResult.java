package es.upm.fi.dia.oeg.obdi.core.querytranslator;

import java.util.Collection;

import Zql.ZSelectItem;

public class PRSQLResult {
	private ZSelectItem prSQLSubject;
	private Collection<ZSelectItem> prSQLPredicateObjects;
	
	public PRSQLResult(ZSelectItem prSQLSubject, Collection<ZSelectItem> prSQLPredicateObjects) {
		super();
		this.prSQLSubject = prSQLSubject;
		this.prSQLPredicateObjects = prSQLPredicateObjects;
	}

	public ZSelectItem getPrSQLSubject() {
		return prSQLSubject;
	}

	public Collection<ZSelectItem> getPrSQLPredicateObjects() {
		return prSQLPredicateObjects;
	}

	
	
}
