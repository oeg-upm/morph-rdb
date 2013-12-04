package es.upm.fi.dia.oeg.obdi.core.querytranslator;

import java.util.List;
import java.util.Vector;

public class BetaResultSet {
	List<BetaResult> betaResultSet;

	public BetaResultSet(List<BetaResult> betaResultSet) {
		super();
		this.betaResultSet = betaResultSet;
	}
	
	public BetaResultSet(BetaResult betaResult) {
		super();
		this.betaResultSet = new Vector<BetaResult>();
		this.betaResultSet.add(betaResult);
	}


	public int size() {
		return this.betaResultSet.size();
	}
	
	public BetaResult get(int i) {
		return this.betaResultSet.get(i);
	}

	@Override
	public String toString() {
		return "BetaResultSet [betaResultSet=" + betaResultSet + "]";
	}
	
	
}
