package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model;

import java.util.Collection;
import java.util.HashSet;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLInvalidTermMapException;

public class R2RMLSubjectMap extends R2RMLTermMap {
	private static Logger logger = Logger.getLogger(R2RMLSubjectMap.class);
	
	private Collection<String> classURIs;
	private R2RMLGraphMap graphMap;

	public R2RMLSubjectMap(String constantValue) {
		super(TermMapPosition.SUBJECT, constantValue);
		super.setTermType(Constants.R2RML_LITERAL_URI());
	}
	
	public R2RMLSubjectMap(Resource resource, R2RMLTriplesMap owner) throws R2RMLInvalidTermMapException {
		super(resource, TermMapPosition.SUBJECT, owner);
		
		if(this.getTermType() != null && this.getTermType().equals(Constants.R2RML_LITERAL_URI())) {
			throw new R2RMLInvalidTermMapException("Literal is not permitted in the subject!");
		}
		
		StmtIterator classStatements = resource.listProperties(
				Constants.R2RML_CLASS_PROPERTY());
		if(classStatements != null) {
			this.classURIs = new HashSet<String>();
			
			while(classStatements.hasNext()) {
				Statement classStatement = classStatements.nextStatement();
				this.classURIs.add(classStatement.getObject().toString());
			}
		}
		
		Statement graphMapStatement = resource.getProperty(Constants.R2RML_GRAPHMAP_PROPERTY());
		if(graphMapStatement != null) {
			this.graphMap = new R2RMLGraphMap((Resource) graphMapStatement.getObject(), owner);
		}

		Statement graphStatement = resource.getProperty(Constants.R2RML_GRAPH_PROPERTY());
		if(graphStatement != null) {
			String graphStatementObjectValue = graphStatement.getObject().toString();
			if(!Constants.R2RML_DEFAULT_GRAPH_URI().equals(graphStatementObjectValue)) {
				this.graphMap = new R2RMLGraphMap(graphStatementObjectValue);
				logger.debug("this.graphMap = " + this.graphMap);				
			}

		}

	}

	public Collection<String> getClassURIs() {
		return classURIs;
	}

	public R2RMLGraphMap getGraphMap() {
		return graphMap;
	}
	
}
