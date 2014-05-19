package es.upm.fi.dia.oeg.morph.base.engine

class QueryTranslationOptimizerFactory {

}

object QueryTranslationOptimizerFactory {
	def createQueryTranslationOptimizerC() : QueryTranslationOptimizer = {
		val queryTranslationOptimizerC = new QueryTranslationOptimizer();
		queryTranslationOptimizerC.selfJoinElimination = false;
		queryTranslationOptimizerC.unionQueryReduction = true;
		queryTranslationOptimizerC.subQueryElimination = false;
		queryTranslationOptimizerC.transJoinSubQueryElimination = false;
		queryTranslationOptimizerC.transSTGSubQueryElimination = false;
		queryTranslationOptimizerC;
	}

	def createQueryTranslationOptimizerE() : QueryTranslationOptimizer = {
		val  queryTranslationOptimizerE = new QueryTranslationOptimizer();
		queryTranslationOptimizerE.selfJoinElimination = false;
		queryTranslationOptimizerE.unionQueryReduction = true;
		queryTranslationOptimizerE.subQueryElimination = true;
		queryTranslationOptimizerE.transJoinSubQueryElimination=true;
		queryTranslationOptimizerE.transSTGSubQueryElimination=true;
		queryTranslationOptimizerE;
	}
	
	def createQueryTranslationOptimizerFE() : QueryTranslationOptimizer  = {
		val queryTranslationOptimizerFE = new QueryTranslationOptimizer();
		queryTranslationOptimizerFE.selfJoinElimination = true;
		queryTranslationOptimizerFE.unionQueryReduction = true;
		queryTranslationOptimizerFE.subQueryElimination = true;
		queryTranslationOptimizerFE.subQueryAsView = false;
		queryTranslationOptimizerFE.transJoinSubQueryElimination = true;
		queryTranslationOptimizerFE.transSTGSubQueryElimination =true ;
		queryTranslationOptimizerFE;
	}
	
	def createQueryTranslationOptimizerFC() : QueryTranslationOptimizer = {
		val queryTranslationOptimizerFC = new QueryTranslationOptimizer();
		queryTranslationOptimizerFC.selfJoinElimination = true;
		queryTranslationOptimizerFC.unionQueryReduction = true;
		queryTranslationOptimizerFC.subQueryElimination = false;
		queryTranslationOptimizerFC.transJoinSubQueryElimination = false;
		queryTranslationOptimizerFC.transSTGSubQueryElimination = false;
		queryTranslationOptimizerFC;
	}  
}