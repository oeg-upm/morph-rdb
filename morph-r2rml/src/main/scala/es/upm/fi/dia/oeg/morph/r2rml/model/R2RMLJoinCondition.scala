package es.upm.fi.dia.oeg.morph.r2rml.model

import org.apache.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.morph.base.Constants
import org.slf4j.LoggerFactory

class R2RMLJoinCondition(val childColumnName:String , val parentColumnName:String ) {
  val logger = LoggerFactory.getLogger(this.getClass());

	override def toString() : String = {
		return "JOIN(Parent:" + this.parentColumnName + ",CHILD:" + this.childColumnName + ")";
	}
}

object R2RMLJoinCondition {
  val logger = LoggerFactory.getLogger(this.getClass());
	
	def apply(resource:Resource ) : R2RMLJoinCondition = {
		val childStatement = resource.getProperty(Constants.R2RML_CHILD_PROPERTY);
		if(childStatement == null) {
			val errorMessage = "Missing child column in join condition!";
			logger.error(errorMessage);
			throw new Exception(errorMessage);		  
		}
		val childColumnName = childStatement.getObject().toString();

		val parentStatement = resource.getProperty(Constants.R2RML_PARENT_PROPERTY);
		if(parentStatement == null) {
			val errorMessage = "Missing parent column in join condition!";
			logger.error(errorMessage);
			throw new Exception(errorMessage);		  
		}
		val parentColumnName = parentStatement.getObject().toString();
		
		new R2RMLJoinCondition(childColumnName, parentColumnName); 
	}
  

}
