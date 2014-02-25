package es.upm.fi.dia.oeg.obdi.core.querytranslator;

import com.hp.hpl.jena.sparql.algebra.optimize.Optimize.RewriterFactory;
import com.hp.hpl.jena.sparql.algebra.optimize.Rewrite;
import com.hp.hpl.jena.sparql.util.Context;


public class QueryRewritterFactory implements RewriterFactory {

	public Rewrite create(Context arg0) {
		return new QueryRewritter();
	}

}
