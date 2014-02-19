package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model;

import java.util.Collection;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping;
import es.upm.fi.dia.oeg.obdi.core.model.IAttributeMapping;
import es.upm.fi.dia.oeg.obdi.core.model.IRelationMapping;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLInvalidRefObjectMapException;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLInvalidTermMapException;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLJoinConditionException;

public class R2RMLPredicateObjectMap extends AbstractPropertyMapping 
implements IRelationMapping, IAttributeMapping{
	public enum ObjectMapType {ObjectMap, RefObjectMap}
	private static Logger logger = Logger.getLogger(R2RMLPredicateObjectMap.class);
	private List<R2RMLPredicateMap> predicateMaps = new Vector<R2RMLPredicateMap>();
	private List<R2RMLObjectMap> objectMaps = new Vector<R2RMLObjectMap>();
	private R2RMLGraphMap graphMap;
	private List<R2RMLRefObjectMap> refObjectMaps = new Vector<R2RMLRefObjectMap>();
	private List<ObjectMapType> objectMapTypes = new Vector<R2RMLPredicateObjectMap.ObjectMapType>();
	private String alias;
	
	public R2RMLPredicateObjectMap(Resource resource, AbstractMappingDocument mappingDocument
			, R2RMLTriplesMap parent) 
			throws R2RMLInvalidRefObjectMapException, R2RMLJoinConditionException, R2RMLInvalidTermMapException {
		this.parent = parent;
		this.resource = resource;
		this.predicateMaps = new Vector<R2RMLPredicateMap>();
		this.refObjectMaps = new Vector<R2RMLRefObjectMap>();
		this.objectMaps = new Vector<R2RMLObjectMap>();
		this.objectMapTypes = new Vector<R2RMLPredicateObjectMap.ObjectMapType>();
		
		StmtIterator pmStatements = resource.listProperties(Constants.R2RML_PREDICATEMAP_PROPERTY());
		if(pmStatements != null) {
			while(pmStatements.hasNext()) {
				Statement pmStatement = pmStatements.next(); 
				//Statement predicateMapStatement = resource.getProperty(Constants.R2RML_PREDICATEMAP_PROPERTY());
				if(pmStatement != null) {
					Resource predicateMapResource = (Resource) pmStatement.getObject();
					R2RMLPredicateMap pm = R2RMLPredicateMap.create(predicateMapResource, parent);
					this.predicateMaps.add(pm);
				}
			}
		}

		
		StmtIterator pStatements = resource.listProperties(Constants.R2RML_PREDICATE_PROPERTY());
		if(pStatements != null) {
			while(pStatements.hasNext()) {
//				Statement predicateStatement = resource.getProperty(Constants.R2RML_PREDICATE_PROPERTY());
				Statement predicateStatement = pStatements.next();
				if(predicateStatement != null) {
					String constantValueObject = predicateStatement.getObject().toString();
					//R2RMLPredicateMap predicateMap = new R2RMLPredicateMap(constantValueObject);
					R2RMLPredicateMap predicateMap = R2RMLPredicateMap.create(constantValueObject);
					this.predicateMaps.add(predicateMap);
				}
			}
		}

		StmtIterator omStatements = resource.listProperties(Constants.R2RML_OBJECTMAP_PROPERTY());
		if(omStatements != null) {
			while(omStatements.hasNext()) {
//				Statement omStatement = resource.getProperty(
//						Constants.R2RML_OBJECTMAP_PROPERTY());
				Statement omStatement = omStatements.next();
				if(omStatement != null) {
					Resource omStatementObject = (Resource) omStatement.getObject();
					if(R2RMLRefObjectMap.isRefObjectMap(omStatementObject)) {
						this.objectMapTypes.add(ObjectMapType.RefObjectMap);
						R2RMLRefObjectMap rom = new R2RMLRefObjectMap(omStatementObject, mappingDocument, this);
						this.refObjectMaps.add(rom);
					} else {
						this.objectMapTypes.add(ObjectMapType.ObjectMap);
						R2RMLObjectMap objectMap = R2RMLObjectMap.create(omStatementObject, parent);
						this.objectMaps.add(objectMap);
					}
				}
			}
		}

		StmtIterator objectStatements = resource.listProperties(Constants.R2RML_OBJECT_PROPERTY());
		if(objectStatements != null) {
			while(objectStatements.hasNext()) {
//				Statement objectStatement = resource.getProperty(
//						Constants.R2RML_OBJECT_PROPERTY());
				Statement objectStatement = objectStatements.next();
				if(objectStatement != null) {
					this.objectMapTypes.add(ObjectMapType.ObjectMap);
					String constantValueObject = objectStatement.getObject().toString();
					R2RMLObjectMap objectMap = R2RMLObjectMap.create(constantValueObject);
					this.objectMaps.add(objectMap);
				}
			}
		}

//		Statement refObjectMapStatement = resource.getProperty(
//				R2RMLConstants.R2RML_REFOBJECTMAP_PROPERTY);
//		if(refObjectMapStatement != null) {
//			this.objectMapType = ObjectMapType.RefObjectMap;
//			this.refObjectMap = new R2RMLRefObjectMap((Resource) refObjectMapStatement.getObject(), mappingDocument);
//		}

		StmtIterator graphMapStatements = resource.listProperties(Constants.R2RML_GRAPHMAP_PROPERTY());
		if(graphMapStatements != null) {
			while(graphMapStatements.hasNext()) {
//				Statement graphMapStatement = resource.getProperty(Constants.R2RML_GRAPHMAP_PROPERTY());
				Statement graphMapStatement = graphMapStatements.next(); 
				if(graphMapStatement != null) {
					Resource graphMapResource = (Resource) graphMapStatement.getObject();
					this.graphMap = R2RMLGraphMap.create(graphMapResource, parent);
				}
			}
		}

		StmtIterator graphStatements = resource.listProperties(Constants.R2RML_GRAPH_PROPERTY());
		if(graphStatements != null) {
			while(graphStatements.hasNext()) {
				Statement graphStatement = graphStatements.next();
				if(graphStatement != null) {
					String graphStatementObjectValue = graphStatement.getObject().toString();
					if(!Constants.R2RML_DEFAULT_GRAPH_URI().equals(graphStatementObjectValue)) {
						this.graphMap = R2RMLGraphMap.create(graphStatementObjectValue);
					}
				}
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
	public String getMappedPredicateName(int index) {
		String result;
		if(this.predicateMaps != null && !this.predicateMaps.isEmpty()) {
			result = this.predicateMaps.get(index).getOriginalValue();
		} else {
			result = null;
		}
		return result;
	}

	public R2RMLObjectMap getObjectMap(int index) {
		R2RMLObjectMap result;
		if(this.objectMaps != null && !this.objectMaps.isEmpty()) {
			result = this.objectMaps.get(index);
		} else {
			result = null;
		}
		return result;
	}

	public Collection<R2RMLObjectMap> getObjectMaps() {
		return objectMaps;
	}

	public Collection<ObjectMapType> getObjectMapTypes() {
		return objectMapTypes;
	}

	public ObjectMapType getObjectMapType(int index) {
		ObjectMapType result;
		if(this.objectMapTypes != null && !this.objectMapTypes.isEmpty()) {
			result = this.objectMapTypes.get(index);
		} else {
			result = null;
		}
		return result;
	}

	public R2RMLPredicateMap getPredicateMap(int index) {
		R2RMLPredicateMap result;
		if(this.predicateMaps != null && !this.predicateMaps.isEmpty()) {
			result = predicateMaps.get(index);
		} else {
			result = null;
		}
		return result;
	}

	public Collection<R2RMLPredicateMap> getPredicateMaps() {
		return predicateMaps;
	}

	public String getPropertyMappingID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MappingType getPropertyMappingType(int index) {
		MappingType result;
		if(this.objectMaps != null && !this.objectMaps.isEmpty() 
				&& this.objectMaps.get(index) != null) {
			String objectMapTermType = this.objectMaps.get(index).getTermType();
			if(objectMapTermType.equals(Constants.R2RML_LITERAL_URI())) {
				result = MappingType.ATTRIBUTE;
			} else {
				result = MappingType.RELATION;
			}
		} else if(this.refObjectMaps != null && !this.refObjectMaps.isEmpty() 
				&& this.refObjectMaps.get(index) != null) {
			result = MappingType.RELATION;
		} else {
			result = null;
		}
		return result;
	}

	public String getRangeClassMapping(int index) {
		String result;
		if(this.refObjectMaps != null && !this.refObjectMaps.isEmpty() 
				&& this.refObjectMaps.get(index) != null) {
			result = this.refObjectMaps.get(index).getParentTripleMapName();
		} else {
			result = null;
		}
		return result;
		
	}

	public R2RMLRefObjectMap getRefObjectMap(int index) {
		R2RMLRefObjectMap result;
		if(this.refObjectMaps != null && !this.refObjectMaps.isEmpty()) {
			result = this.refObjectMaps.get(index);
		} else {
			result = null;
		}
		return result;
	}

	public Collection<R2RMLRefObjectMap> getRefObjectMaps() {
		return this.refObjectMaps;
	}

	public String getRelationName() {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getRelationName");
		return null;
	}

	@Override
	public String toString() {
		return "R2RMLPredicateObjectMap [predicateMaps=" + predicateMaps
				+ ", objectMaps=" + objectMaps + ", refObjectMaps=" + refObjectMaps
				+ ", objectMapTypes=" + objectMapTypes + "]";
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	@Override
	public String getRangeClassMapping() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<String> getMappedPredicateNames() {
		Collection<String> result = new Vector<String>();
		for(R2RMLPredicateMap pm : this.predicateMaps) {
			result.add(pm.getOriginalValue());
		}
		return result;
	}


	
	
}
