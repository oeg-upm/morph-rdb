package es.upm.fi.dia.oeg.obdi.core.querytranslator;

import es.upm.fi.dia.oeg.obdi.core.engine.IQueryTranslationOptimizer;

public class QueryTranslationOptimizer implements IQueryTranslationOptimizer {

	private boolean selfJoinElimination = true;
	private boolean transJoinSubQueryElimination = false;
	private boolean transSTGSubQueryElimination = false;
	private boolean unionQueryReduction = true;
	private boolean subQueryElimination = true;
	private boolean subQueryAsView = false;
	
	public boolean isSelfJoinElimination() {
		return selfJoinElimination;
	}

	public void setSelfJoinElimination(boolean selfJoinElimination) {
		this.selfJoinElimination = selfJoinElimination;
	}

	public void setUnionQueryReduction(boolean unionQueryReduction) {
		this.unionQueryReduction = unionQueryReduction;
	}

	public boolean isUnionQueryReduction() {
		return this.unionQueryReduction;
	}

	public boolean isSubQueryElimination() {
		return subQueryElimination;
	}

	public void setSubQueryElimination(boolean subQueryElimination) {
		this.subQueryElimination = subQueryElimination;
	}

	public boolean isSubQueryAsView() {
		return subQueryAsView;
	}

	public void setSubQueryAsView(boolean subQueryAsView) {
		this.subQueryAsView = subQueryAsView;
	}

	public boolean isTransJoinSubQueryElimination() {
		return this.transJoinSubQueryElimination;
	}

	public void setTransJoinSubQueryElimination(boolean subQueryElimination) {
		this.transJoinSubQueryElimination = subQueryElimination;
	}

	public boolean isTransSTGSubQueryElimination() {
		return transSTGSubQueryElimination;
	}

	public void setTransSTGSubQueryElimination(boolean transSTGSubQueryElimination) {
		this.transSTGSubQueryElimination = transSTGSubQueryElimination;
	}

}
