package es.upm.fi.dia.oeg.obdi.core.querytranslator;


public class QueryTranslationOptimizerFactory extends
		QueryTranslationOptimizer {
	
	public static QueryTranslationOptimizer createQueryTranslationOptimizerC() {
		QueryTranslationOptimizer queryTranslationOptimizerC = new QueryTranslationOptimizer();
		queryTranslationOptimizerC.setSelfJoinElimination(false);
		queryTranslationOptimizerC.setUnionQueryReduction(true);
		queryTranslationOptimizerC.setSubQueryElimination(false);
		queryTranslationOptimizerC.setTransJoinSubQueryElimination(false);
		queryTranslationOptimizerC.setTransSTGSubQueryElimination(false);
		return queryTranslationOptimizerC;
	}

	public static QueryTranslationOptimizer createQueryTranslationOptimizerE() {
		QueryTranslationOptimizer queryTranslationOptimizerE = new QueryTranslationOptimizer();
		queryTranslationOptimizerE.setSelfJoinElimination(false);
		queryTranslationOptimizerE.setUnionQueryReduction(true);
		queryTranslationOptimizerE.setSubQueryElimination(true);
		queryTranslationOptimizerE.setTransJoinSubQueryElimination(true);
		queryTranslationOptimizerE.setTransSTGSubQueryElimination(true);
		return queryTranslationOptimizerE;
	}
	
	public static QueryTranslationOptimizer createQueryTranslationOptimizerFE() {
		QueryTranslationOptimizer queryTranslationOptimizerFE = new QueryTranslationOptimizer();
		queryTranslationOptimizerFE.setSelfJoinElimination(true);
		queryTranslationOptimizerFE.setUnionQueryReduction(true);
		queryTranslationOptimizerFE.setSubQueryElimination(true);
		queryTranslationOptimizerFE.setSubQueryAsView(false);
		queryTranslationOptimizerFE.setTransJoinSubQueryElimination(true);
		queryTranslationOptimizerFE.setTransSTGSubQueryElimination(true);
		return queryTranslationOptimizerFE;
	}
	
	public static QueryTranslationOptimizer createQueryTranslationOptimizerFC() {
		QueryTranslationOptimizer queryTranslationOptimizerFC = new QueryTranslationOptimizer();
		queryTranslationOptimizerFC.setSelfJoinElimination(true);
		queryTranslationOptimizerFC.setUnionQueryReduction(true);
		queryTranslationOptimizerFC.setSubQueryElimination(false);
		queryTranslationOptimizerFC.setTransJoinSubQueryElimination(false);
		queryTranslationOptimizerFC.setTransSTGSubQueryElimination(false);
		return queryTranslationOptimizerFC;
	}	
}
