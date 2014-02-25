package es.upm.fi.dia.oeg.obdi.core.querytranslator;

import es.upm.fi.dia.oeg.obdi.core.sql.SQLSelectItem;

public class BetaResult {
	private SQLSelectItem betaSub;
	private SQLSelectItem betaPre;
	private SQLSelectItem betaObj;
	private String predicateURI;
	
	public BetaResult(SQLSelectItem betaSub, SQLSelectItem betaPre,
			SQLSelectItem betaObj, String predicateURI) {
		super();
		this.betaSub = betaSub;
		this.betaPre = betaPre;
		this.betaObj = betaObj;
		this.predicateURI = predicateURI;
	}
	
	public SQLSelectItem getBetaSub() {
		return betaSub;
	}
	
	public SQLSelectItem getBetaPre() {
		return betaPre;
	}
	
	public SQLSelectItem getBetaObj() {
		return betaObj;
	}

	@Override
	public String toString() {
		return "BetaResult [betaSub=" + betaSub + ", betaPre=" + betaPre
				+ ", betaObj=" + betaObj + ", predicateURI=" + predicateURI
				+ "]";
	}

	public String getPredicateURI() {
		return predicateURI;
	}
	
	
}
