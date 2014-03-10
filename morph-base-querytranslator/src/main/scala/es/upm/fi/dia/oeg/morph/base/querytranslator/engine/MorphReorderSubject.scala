package es.upm.fi.dia.oeg.morph.base.querytranslator.engine

import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderTransformationBase
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.PatternTriple

class MorphReorderSubject extends ReorderTransformationBase {
	def weight(pt:PatternTriple ) = {
		val ptHashCode = pt.subject.hashCode(); 
		Math.abs(ptHashCode);
	}
}