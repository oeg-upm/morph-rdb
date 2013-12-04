package es.upm.fi.dia.oeg.obdi.core.querytranslator;

import com.hp.hpl.jena.sparql.engine.optimizer.reorder.PatternTriple;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderTransformationBase;

public class ReorderSubject extends ReorderTransformationBase {

	@Override
	protected double weight(PatternTriple pt) {
		double ptHashCode = pt.subject.hashCode(); 
		return Math.abs(ptHashCode);
	}

}
