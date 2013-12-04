package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLJoinConditionException;

public class R2RMLJoinCondition {
	private static Logger logger = Logger.getLogger(R2RMLJoinCondition.class);
	
	private String childColumnName;
	private String parentColumnName;
	
	
	public R2RMLJoinCondition(String childColumnName, String parentColumnName) {
		super();
		this.childColumnName = childColumnName;
		this.parentColumnName = parentColumnName;
	}
	
	public R2RMLJoinCondition(Resource resource) throws R2RMLJoinConditionException {
		Statement childStatement = resource.getProperty(Constants.R2RML_CHILD_PROPERTY());
		if(childStatement != null) {
			this.childColumnName = childStatement.getObject().toString();
		} else {
			String errorMessage = "Missing child column in join condition!";
			logger.error(errorMessage);
			throw new R2RMLJoinConditionException(errorMessage);
		}

		Statement parentStatement = resource.getProperty(Constants.R2RML_PARENT_PROPERTY());
		if(parentStatement != null) {
			this.parentColumnName = parentStatement.getObject().toString();
		} else {
			String errorMessage = "Missing parent column in join condition!";
			logger.error(errorMessage);
			throw new R2RMLJoinConditionException(errorMessage);
		}
		
	}

	@Override
	public String toString() {
		return "JOIN(Parent:" + this.parentColumnName + ",CHILD:" + this.childColumnName + ")";
	}

	public String getChildColumnName() {
		return childColumnName;
	}

	public String getParentColumnName() {
		return parentColumnName;
	}
	
	
	
}	
