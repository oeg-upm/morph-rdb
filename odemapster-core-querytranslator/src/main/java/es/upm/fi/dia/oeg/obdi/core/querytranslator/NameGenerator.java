package es.upm.fi.dia.oeg.obdi.core.querytranslator;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;

import es.upm.fi.dia.oeg.morph.base.Constants;


public class NameGenerator {
	private Constants constants = new Constants();
	
	public String generateName(Node node) {
		String nodeHashCode = (node.hashCode() + "").replaceAll("-", "");//remove negative values
		
		String result = null;
		if(node.isVariable()) {
			result = this.generateName((Var) node);
		} else if(node.isURI()) {
			String localName = node.getLocalName(); 
			result = constants.PREFIX_URI() + localName + nodeHashCode;
		} else if(node.isLiteral()) {
			result = constants.PREFIX_LIT() + nodeHashCode;
		}

		result = result.replaceAll("-", "_"); 
		return result;
	}
	
	public String generateName(Var var) {
		return constants.PREFIX_VAR() + var.getName();
	}
	

}
