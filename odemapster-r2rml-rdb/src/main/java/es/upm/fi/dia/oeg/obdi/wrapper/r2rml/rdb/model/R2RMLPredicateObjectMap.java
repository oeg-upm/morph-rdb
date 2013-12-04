package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping;
import es.upm.fi.dia.oeg.obdi.core.model.IAttributeMapping;
import es.upm.fi.dia.oeg.obdi.core.model.IRelationMapping;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLInvalidRefObjectMapException;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLInvalidTermMapException;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLJoinConditionException;

public class R2RMLPredicateObjectMap extends AbstractPropertyMapping implements IRelationMapping, IAttributeMapping{
	public enum ObjectMapType {ObjectMap, RefObjectMap}
	private static Logger logger = Logger.getLogger(R2RMLPredicateObjectMap.class);
	private R2RMLPredicateMap predicateMap;
	private R2RMLObjectMap objectMap;
	private R2RMLGraphMap graphMap;
	private R2RMLRefObjectMap refObjectMap;
	private ObjectMapType objectMapType;
	
	private String alias;
	
	public R2RMLPredicateObjectMap(Resource resource, R2RMLMappingDocument mappingDocument
			, R2RMLTriplesMap parent) 
			throws R2RMLInvalidRefObjectMapException, R2RMLJoinConditionException, R2RMLInvalidTermMapException {
		this.parent = parent;
		this.resource = resource;
		
		Statement predicateMapStatement = resource.getProperty(Constants.R2RML_PREDICATEMAP_PROPERTY());
		if(predicateMapStatement != null) {
			Resource predicateMapResource = (Resource) predicateMapStatement.getObject();
			this.predicateMap = new R2RMLPredicateMap(predicateMapResource, parent);
		}

		Statement predicateStatement = resource.getProperty(Constants.R2RML_PREDICATE_PROPERTY());
		if(predicateStatement != null) {
			String constantValueObject = predicateStatement.getObject().toString();
			this.predicateMap = new R2RMLPredicateMap(constantValueObject);
		}
		
		Statement objectMapStatement = resource.getProperty(
				Constants.R2RML_OBJECTMAP_PROPERTY());
		if(objectMapStatement != null) {
			Resource objectMapStatementObject = (Resource) objectMapStatement.getObject();
			if(R2RMLRefObjectMap.isRefObjectMap(objectMapStatementObject)) {
				this.objectMapType = ObjectMapType.RefObjectMap;
				this.refObjectMap = new R2RMLRefObjectMap(objectMapStatementObject, mappingDocument, this);				
			} else {
				this.objectMapType = ObjectMapType.ObjectMap;
				this.objectMap = new R2RMLObjectMap(objectMapStatementObject, parent);
			}
		}

		Statement objectStatement = resource.getProperty(
				Constants.R2RML_OBJECT_PROPERTY());
		if(objectStatement != null) {
			this.objectMapType = ObjectMapType.ObjectMap;
			String constantValueObject = objectStatement.getObject().toString();
			this.objectMap = new R2RMLObjectMap(constantValueObject);
		}

//		Statement refObjectMapStatement = resource.getProperty(
//				R2RMLConstants.R2RML_REFOBJECTMAP_PROPERTY);
//		if(refObjectMapStatement != null) {
//			this.objectMapType = ObjectMapType.RefObjectMap;
//			this.refObjectMap = new R2RMLRefObjectMap((Resource) refObjectMapStatement.getObject(), mappingDocument);
//		}

		Statement graphMapStatement = resource.getProperty(Constants.R2RML_GRAPHMAP_PROPERTY());
		if(graphMapStatement != null) {
			Resource graphMapResource = (Resource) graphMapStatement.getObject();
			this.graphMap = new R2RMLGraphMap(graphMapResource, parent);
		}
		
		Statement graphStatement = resource.getProperty(Constants.R2RML_GRAPH_PROPERTY());
		if(graphStatement != null) {
			String graphStatementObjectValue = graphStatement.getObject().toString();
			if(!Constants.R2RML_DEFAULT_GRAPH_URI().equals(graphStatementObjectValue)) {
				this.graphMap = new R2RMLGraphMap(graphStatementObjectValue);
			}
			
		}
		

	}

	public String getAttributeName() {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getAttributeName");
		return null;
	}

	public R2RMLGraphMap getGraphMap() {
		return graphMap;
	}

	@Override
	public String getMappedPredicateName() {
		return this.predicateMap.getOriginalValue();
	}

	public R2RMLObjectMap getObjectMap() {
		return objectMap;
	}

	public ObjectMapType getObjectMapType() {
		return objectMapType;
	}

	public R2RMLPredicateMap getPredicateMap() {
		return predicateMap;
	}

	public String getPropertyMappingID() {
		// TODO Auto-generated method stub
		return null;
	}

	public MappingType getPropertyMappingType() {
		MappingType result;
		if(this.objectMap != null) {
			String objectMapTermType = this.objectMap.getTermType();
			if(objectMapTermType.equals(Constants.R2RML_LITERAL_URI())) {
				result = MappingType.ATTRIBUTE;
			} else {
				result = MappingType.RELATION;
			}
		} else if(this.refObjectMap != null) {
			result = MappingType.RELATION;
		} else {
			result = null;
		}
		return result;
	}

	public String getRangeClassMapping() {
		// TODO Auto-generated method stub
		if(this.refObjectMap != null) {
			return this.refObjectMap.getParentTripleMapName();
		} else {
			return null;
		}
		
	}

	public R2RMLRefObjectMap getRefObjectMap() {
		return refObjectMap;
	}

	public String getRelationName() {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getRelationName");
		return null;
	}

	@Override
	public String toString() {
		return "R2RMLPredicateObjectMap [predicateMap=" + predicateMap
				+ ", objectMap=" + objectMap + ", refObjectMap=" + refObjectMap
				+ ", objectMapType=" + objectMapType + "]";
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	
	
}
