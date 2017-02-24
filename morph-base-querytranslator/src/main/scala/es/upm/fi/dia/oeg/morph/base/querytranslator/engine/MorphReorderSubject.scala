package es.upm.fi.dia.oeg.morph.base.querytranslator.engine

//import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformationBase
import org.apache.jena.sparql.engine.optimizer.reorder.PatternTriple

//class MorphReorderSubject extends ReorderTransformationBase {
class MorphReorderSubject {  
	def weight(pt:PatternTriple ) = {
		val ptHashCode = pt.subject.hashCode(); 
		Math.abs(ptHashCode);
	}
}