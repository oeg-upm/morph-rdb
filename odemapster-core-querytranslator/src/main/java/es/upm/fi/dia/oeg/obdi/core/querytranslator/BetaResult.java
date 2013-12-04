package es.upm.fi.dia.oeg.obdi.core.querytranslator;

import Zql.ZSelectItem;


public class BetaResult {
	private ZSelectItem betaSub;
	private ZSelectItem betaPre;
	private ZSelectItem betaObj;
	private String predicateURI;
	
	public BetaResult(ZSelectItem betaSub, ZSelectItem betaPre,
			ZSelectItem betaObj, String predicateURI) {
		super();
		this.betaSub = betaSub;
		this.betaPre = betaPre;
		this.betaObj = betaObj;
		this.predicateURI = predicateURI;
	}
	
	public ZSelectItem getBetaSub() {
		return betaSub;
	}
	
	public ZSelectItem getBetaPre() {
		return betaPre;
	}
	
	public ZSelectItem getBetaObj() {
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
