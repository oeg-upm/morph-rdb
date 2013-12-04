package es.upm.fi.dia.oeg.morph.querytranslator

import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.sparql.core.Var
import es.upm.fi.dia.oeg.morph.base.Constants

class NameGenerator {
	
	def generateName(node:Node) : String = {
		val nodeHashCode = (node.hashCode() + "").replaceAll("-", "");//remove negative values
		
		val result = {
			if(node.isVariable()) {
				this.generateName(node.asInstanceOf[Var]);
			} else if(node.isURI()) {
				val localName = node.getLocalName(); 
				Constants.PREFIX_URI + localName + nodeHashCode;
			} else if(node.isLiteral()) {
				Constants.PREFIX_LIT + nodeHashCode;
			} else {
			  null
			}		  
		}
		
		val finalResult = result.replaceAll("-", "_");
		finalResult
	}
	
	def generateName(pVar:Var) : String = {
		Constants.PREFIX_VAR + pVar.getName();
	}
}