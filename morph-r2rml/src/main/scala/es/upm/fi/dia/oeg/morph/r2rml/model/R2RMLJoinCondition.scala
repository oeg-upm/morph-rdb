package es.upm.fi.dia.oeg.morph.r2rml.model

import org.apache.log4j.Logger
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.morph.base.Constants

class R2RMLJoinCondition(val childColumnName:String , val parentColumnName:String ) {
	def logger = Logger.getLogger(this.getClass().getName());

	override def toString() : String = {
		return "JOIN(Parent:" + this.parentColumnName + ",CHILD:" + this.childColumnName + ")";
	}
}

object R2RMLJoinCondition {
	def logger = Logger.getLogger(this.getClass().getName());
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
