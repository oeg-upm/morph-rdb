package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model;

import java.util.Collection;
import java.util.HashSet;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLInvalidTermMapException;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap.TermMapType;

public class R2RMLSubjectMap extends R2RMLTermMap {
	
	private static Logger logger = Logger.getLogger(R2RMLSubjectMap.class);
	private Collection<String> classURIs;
	private R2RMLGraphMap graphMap;

//	public R2RMLSubjectMap(String constantValue) {
//		super(TermMapPosition.SUBJECT, constantValue);
//		super.setTermType(Constants.R2RML_LITERAL_URI());
//	}

	public static R2RMLSubjectMap create(String constantValue) {
		R2RMLSubjectMap sm = new R2RMLSubjectMap();
		sm.termMapType = TermMapType.CONSTANT;
		sm.constantValue = constantValue;
		sm.termType = Constants.R2RML_LITERAL_URI();
		return sm;
	}
	
	public static R2RMLSubjectMap create(Resource resource, R2RMLTriplesMap owner) 
			throws R2RMLInvalidTermMapException {
		R2RMLSubjectMap sm = new R2RMLSubjectMap();
		sm.parse(resource, owner);
		
		if(sm.getTermType() != null && sm.getTermType().equals(Constants.R2RML_LITERAL_URI())) {
			throw new R2RMLInvalidTermMapException("Literal is not permitted in the subject!");
		}
		
		StmtIterator classStatements = resource.listProperties(
				Constants.R2RML_CLASS_PROPERTY());
		if(classStatements != null) {
			sm.classURIs = new HashSet<String>();
			
			while(classStatements.hasNext()) {
				Statement classStatement = classStatements.nextStatement();
				sm.classURIs.add(classStatement.getObject().toString());
			}
		}
		
		Statement graphMapStatement = resource.getProperty(Constants.R2RML_GRAPHMAP_PROPERTY());
		if(graphMapStatement != null) {
			sm.graphMap = R2RMLGraphMap.create((Resource) graphMapStatement.getObject(), owner);
		}

		Statement graphStatement = resource.getProperty(Constants.R2RML_GRAPH_PROPERTY());
		if(graphStatement != null) {
			String graphStatementObjectValue = graphStatement.getObject().toString();
			if(!Constants.R2RML_DEFAULT_GRAPH_URI().equals(graphStatementObjectValue)) {
				sm.graphMap = R2RMLGraphMap.create(graphStatementObjectValue);
				logger.debug("this.graphMap = " + sm.graphMap);				
			}

		}
		
		return sm;

	}

	public Collection<String> getClassURIs() {
		return classURIs;
	}

	public R2RMLGraphMap getGraphMap() {
		return graphMap;
	}
	
}
