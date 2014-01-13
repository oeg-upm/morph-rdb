package es.upm.fi.dia.oeg.morph.base.querytranslator

class MorphBetaResultSet(betaResultSet:java.util.List[MorphBetaResult] ) {
	def size() = {
		this.betaResultSet.size();
	}
	
	def  get(i:Integer) : MorphBetaResult = {
		this.betaResultSet.get(i);
	}

}