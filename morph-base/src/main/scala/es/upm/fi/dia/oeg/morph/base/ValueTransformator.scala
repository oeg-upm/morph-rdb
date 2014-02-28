package es.upm.fi.dia.oeg.morph.base

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;

class ValueTransformator {

}

object ValueTransformator {
	def transformToLexical(originalValue:String, pDatatype:Option[String]) : String = {
		if(pDatatype.isDefined && originalValue != null) {
			val datatype = pDatatype.get;
		    val xsdDateTimeURI = XSDDatatype.XSDdateTime.getURI().toString();
		    val xsdBooleanURI = XSDDatatype.XSDboolean.getURI().toString();
		    
	        if(datatype.equals(xsdDateTimeURI)) {
		      originalValue.trim().replaceAll(" ", "T");
		    } else if(datatype.equals(xsdBooleanURI)) {
				if(originalValue.equalsIgnoreCase("T") || originalValue.equalsIgnoreCase("True") ) {
					"true";
				} else if(originalValue.equalsIgnoreCase("F") || originalValue.equalsIgnoreCase("False")) {
					"false";
				} else {
					"false";
				}		      
		    } else {
		      originalValue
		    }		  
		} else {
		  originalValue
		}
	}
	
}