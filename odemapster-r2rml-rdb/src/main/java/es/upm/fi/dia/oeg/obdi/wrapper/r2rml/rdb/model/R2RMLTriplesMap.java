package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model;

import java.sql.Connection;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.morph.base.TableMetaData;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping;
import es.upm.fi.dia.oeg.obdi.core.model.IConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.IRelationMapping;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLElement;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLElementVisitor;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLInvalidRefObjectMapException;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLInvalidTermMapException;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLInvalidTriplesMapException;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLJoinConditionException;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLPredicateObjectMap.ObjectMapType;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap.TermMapType;

public class R2RMLTriplesMap extends AbstractConceptMapping 
implements R2RMLElement, IConceptMapping {
	private static Logger logger = Logger.getLogger(R2RMLTriplesMap.class);
	private String triplesMapName;
	private R2RMLMappingDocument owner;
	private R2RMLLogicalTable logicalTable;
	private R2RMLSubjectMap subjectMap;
	private Collection<R2RMLPredicateObjectMap> predicateObjectMaps;
	
	public R2RMLTriplesMap(Resource triplesMap, R2RMLMappingDocument owner) 
			throws R2RMLInvalidTriplesMapException, R2RMLInvalidRefObjectMapException, R2RMLJoinConditionException, R2RMLInvalidTermMapException {
		this.owner = owner;
		this.triplesMapName = triplesMap.getLocalName();
		this.resource = triplesMap;
		
		try {
			Statement logicalTableStatement = triplesMap.getProperty(
					Constants.R2RML_LOGICALTABLE_PROPERTY());
			if(logicalTableStatement != null) {
				RDFNode logicalTableStatementObject = 
						logicalTableStatement.getObject();
				Resource logicalTableStatementObjectResource = 
						(Resource) logicalTableStatementObject;
				this.logicalTable = R2RMLLogicalTable.parse(
						logicalTableStatementObjectResource, this);
				try {
					Connection conn = this.owner.getConn();
					if(conn != null) {
						logger.info("Building metadata for triples map: " + this.triplesMapName);
						this.logicalTable.buildMetaData(conn);
						logger.info("metadata built.");						
					}
				} catch(Exception e) {
					logger.error(e.getMessage());
				}
				
			} else {
				String errorMessage = "Missing rr:logicalTable";
				logger.error(errorMessage);
				throw new R2RMLInvalidTriplesMapException(errorMessage);
			}
			
		} catch(Exception e) {
			throw new R2RMLInvalidTriplesMapException(e);
		}


		//rr:subjectMap
		StmtIterator subjectMaps = triplesMap.listProperties(
				Constants.R2RML_SUBJECTMAP_PROPERTY());
		if(subjectMaps == null) {
			String errorMessage = "Missing rr:subjectMap";
			logger.error(errorMessage);
			throw new R2RMLInvalidTriplesMapException(errorMessage);
		}
		Collection<Statement> subjectMapsSet = subjectMaps.toSet();
		if(subjectMapsSet.size() > 1) {
			String errorMessage = "Multiple rr:subjectMap predicates are not allowed";
			logger.error(errorMessage);
			throw new R2RMLInvalidTriplesMapException(errorMessage);
		}
		Statement subjectMapStatement = triplesMap.getProperty(
				Constants.R2RML_SUBJECTMAP_PROPERTY());
		if(subjectMapStatement != null) {
			Resource subjectMapStatementObjectResource = (Resource) subjectMapStatement.getObject();
			this.subjectMap = new R2RMLSubjectMap(subjectMapStatementObjectResource, this);
		} else {
			String errorMessage = "Missing rr:subjectMap";
			logger.error(errorMessage);
			throw new R2RMLInvalidTriplesMapException(errorMessage);
		}

		//rr:subject
		Statement subjectStatement = triplesMap.getProperty(
				Constants.R2RML_SUBJECT_PROPERTY());
		if(subjectStatement != null) {
			String constantValueObject = subjectStatement.getObject().toString();
			this.subjectMap = new R2RMLSubjectMap(constantValueObject);
		}

		//rr:predicateObjectMap
		StmtIterator predicateObjectMapStatements = triplesMap.listProperties(
				Constants.R2RML_PREDICATEOBJECTMAP_PROPERTY());
		if(predicateObjectMapStatements != null) {
			this.predicateObjectMaps = new HashSet<R2RMLPredicateObjectMap>();
			while(predicateObjectMapStatements.hasNext()) {
				Resource predicateObjectMapStatementObjectResource = (Resource) predicateObjectMapStatements.nextStatement().getObject();
				R2RMLPredicateObjectMap predicateObjectMap = 
						new R2RMLPredicateObjectMap(predicateObjectMapStatementObjectResource, owner, this); 
				this.predicateObjectMaps.add(predicateObjectMap);
			}
		}
	}

	public Object accept(R2RMLElementVisitor visitor) throws Exception {
		return visitor.visit(this);
	}

	public R2RMLLogicalTable getLogicalTable() {
		return logicalTable;
	}

	public R2RMLSubjectMap getSubjectMap() {
		return subjectMap;
	}

	public Collection<R2RMLPredicateObjectMap> getPredicateObjectMaps() {
		return predicateObjectMaps;
	}

	@Override
	public String toString() {
		return this.triplesMapName;
	}

	@Override
	public String getConceptName() {
		String result = null;
		
		Collection<String> classURIs = this.subjectMap.getClassURIs();
		if(classURIs != null) {
			if(classURIs.size() > 1) {
				logger.warn("only one class URI is returned!");
			}
			result = classURIs.iterator().next();
		}

		return result;
	}

	public Collection<R2RMLRefObjectMap> getRefObjectMaps() {
		Collection<R2RMLRefObjectMap> result = null;

		if(this.predicateObjectMaps != null) {
			for(R2RMLPredicateObjectMap predicateObjectMap : this.predicateObjectMaps) {
				if(predicateObjectMap.getObjectMapType() == ObjectMapType.RefObjectMap) {
					if(result == null) {
						result = new HashSet<R2RMLRefObjectMap>();
					}
					result.add(predicateObjectMap.getRefObjectMap());
				}
			}
		}
		return result;
	}

	@Override
	public Collection<AbstractPropertyMapping> getPropertyMappings(
			String propertyURI) {
		Collection<AbstractPropertyMapping> result =  new Vector<AbstractPropertyMapping>();
		Collection<R2RMLPredicateObjectMap> predicateObjectMaps = 
				this.getPredicateObjectMaps();
		if(predicateObjectMaps != null && predicateObjectMaps.size() > 0) {
			for(R2RMLPredicateObjectMap predicateObjectMap : predicateObjectMaps) {
				String predicateMapValue = predicateObjectMap.getPredicateMap().getOriginalValue();
				if(predicateMapValue.equals(propertyURI)) {
					result.add(predicateObjectMap);
				}
			}
		}
		return result;
	}

	@Override
	public Collection<AbstractPropertyMapping> getPropertyMappings() {
		Collection<AbstractPropertyMapping> result = new Vector<AbstractPropertyMapping>();
		if(this.predicateObjectMaps != null && this.predicateObjectMaps.size() > 0) {
			result.addAll(this.predicateObjectMaps);
		}
		return result;
	}



	@Override
	public Collection<IRelationMapping> getRelationMappings() {
		Collection<IRelationMapping> result = new Vector<IRelationMapping>();
		if(this.predicateObjectMaps != null) {
			for(AbstractPropertyMapping pm : this.predicateObjectMaps) {
				MappingType mappingType = pm.getPropertyMappingType();
				if(mappingType == MappingType.RELATION) {
					result.add((R2RMLPredicateObjectMap) pm);
				}
			}
		}
		return result;
	}


	public R2RMLMappingDocument getOwner() {
		return owner;
	}

	@Override
	public boolean isPossibleInstance(String uri) {
		boolean result = false;
		
		TermMapType subjectMapTermMapType = this.subjectMap.getTermMapType();
		if(subjectMapTermMapType == TermMapType.TEMPLATE) {
			Map<String, String> templateValues = this.subjectMap.getTemplateValues(uri);
			if(templateValues != null && templateValues.size() > 0) {
				result = true;
				for(String value : templateValues.values()) {
					if(value.contains("/")) {
						result = false;
					}
				}
			}
		} else {
			result = false;
			String errorMessage = "Can't determine whether " + uri + " is a possible instance of " + this.toString();
			logger.warn(errorMessage);
		}
		
		return result;
	}

	@Override
	public Long getLogicalTableSize() {
		return this.logicalTable.getLogicalTableSize();
	}

	@Override
	public Collection<String> getMappedClassURIs() {
		Collection<String> classURIs = this.subjectMap.getClassURIs();
		return classURIs;
	}

	@Override
	public TableMetaData getTableMetaData() {
		return this.logicalTable.getTableMetaData();
	}


}
