package es.upm.fi.dia.oeg.obdi.core.querytranslator;

import com.hp.hpl.jena.sparql.algebra.optimize.Optimize.RewriterFactory;
import com.hp.hpl.jena.sparql.algebra.optimize.Rewrite;
import com.hp.hpl.jena.sparql.util.Context;

import es.upm.fi.dia.oeg.morph.querytranslator.MorphQueryRewriter;


public class QueryRewritterFactory implements RewriterFactory {

	public Rewrite create(Context arg0) {
		return (Rewrite) new MorphQueryRewriter(null, true);
	}
}
