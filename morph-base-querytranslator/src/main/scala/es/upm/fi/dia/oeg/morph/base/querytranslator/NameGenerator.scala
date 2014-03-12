package es.upm.fi.dia.oeg.morph.base.querytranslator

import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.sparql.core.Var
import es.upm.fi.dia.oeg.morph.base.Constants
import com.hp.hpl.jena.graph.Node_Variable

class NameGenerator {
	
	def generateName(node:Node) : String = {
		val nodeHashCode = (node.hashCode() + "").replaceAll("-", "");//remove negative values
		
		val result = {
			if(node.isVariable()) {
				Constants.PREFIX_VAR + node.getName();
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

	def generateName(pVar:Node_Variable) : String = {
		Constants.PREFIX_VAR + pVar.getName();
	}	
}