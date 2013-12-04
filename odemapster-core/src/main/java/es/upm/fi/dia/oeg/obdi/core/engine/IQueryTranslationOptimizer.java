package es.upm.fi.dia.oeg.obdi.core.engine;

public interface IQueryTranslationOptimizer {
	public void setSelfJoinElimination(boolean selfJoinElimination);
	public boolean isSelfJoinElimination();
	
	public void setUnionQueryReduction(boolean unionQueryReduction);
	public boolean isUnionQueryReduction();
	
	public boolean isSubQueryElimination();
	public void setSubQueryElimination(boolean subQueryElimination);

	public boolean isTransJoinSubQueryElimination();
	public void setTransJoinSubQueryElimination(boolean subQueryElimination);

	public boolean isTransSTGSubQueryElimination();
	public void setTransSTGSubQueryElimination(boolean subQueryElimination);

	public boolean isSubQueryAsView();
	public void setSubQueryAsView(boolean subQueryAsView);
}
