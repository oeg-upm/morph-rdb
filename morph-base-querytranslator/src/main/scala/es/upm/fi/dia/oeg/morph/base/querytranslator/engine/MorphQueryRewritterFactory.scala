package es.upm.fi.dia.oeg.morph.base.querytranslator.engine

import org.apache.jena.sparql.algebra.optimize.Optimize.RewriterFactory;
import org.apache.jena.sparql.algebra.optimize.Rewrite;
import org.apache.jena.sparql.util.Context;

class MorphQueryRewritterFactory extends RewriterFactory {
	def create(arg0:Context ) = {
		val result = new MorphQueryRewriter(null, true);
		result.asInstanceOf[Rewrite];
	}
}