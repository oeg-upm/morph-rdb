package es.upm.fi.dia.oeg.obdi.core.querytranslator;

import java.util.List;

import es.upm.fi.dia.oeg.obdi.core.sql.SQLJoinTable;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLLogicalTable;

public class AlphaResult {
	private SQLLogicalTable alphaSubject;
	private List<SQLJoinTable> alphaPredicateObjects;
	private String predicateURI;
	private List<SQLLogicalTable> alphaPredicateObjects2;
	
	public AlphaResult(SQLLogicalTable alphaSubject
			, List<SQLJoinTable> alphaPredicateObjects, String predicateURI) {
		super();
		this.alphaSubject = alphaSubject;
		this.alphaPredicateObjects = alphaPredicateObjects;
		this.predicateURI = predicateURI;
	}

	public SQLLogicalTable getAlphaSubject() {
		return alphaSubject;
	}

	public List<SQLJoinTable> getAlphaPredicateObjects() {
		return alphaPredicateObjects;
	}

	@Override
	public String toString() {
		return "AlphaResult [alphaSubject=" + alphaSubject
				+ ", alphaPredicateObjects=" + alphaPredicateObjects
				+ ", predicateURI=" + predicateURI + "]";
	}

	public String getPredicateURI() {
		return predicateURI;
	}

	public List<SQLLogicalTable> getAlphaPredicateObjects2() {
		return alphaPredicateObjects2;
	}

	public void setAlphaPredicateObjects2(
			List<SQLLogicalTable> alphaPredicateObjects2) {
		this.alphaPredicateObjects2 = alphaPredicateObjects2;
	}
	
	
	
}
