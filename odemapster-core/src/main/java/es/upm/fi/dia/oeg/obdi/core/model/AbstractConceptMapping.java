package es.upm.fi.dia.oeg.obdi.core.model;

import java.util.Collection;

import com.hp.hpl.jena.rdf.model.Resource;

import es.upm.fi.dia.oeg.morph.base.TableMetaData;


public abstract class AbstractConceptMapping extends AbstractRDB2RDFMapping implements IConceptMapping {
	public abstract String getConceptName();
	public abstract Collection<AbstractPropertyMapping> getPropertyMappings(String propertyURI);
	public abstract Collection<AbstractPropertyMapping> getPropertyMappings();
	public abstract Collection<IRelationMapping> getRelationMappings();
	//public abstract String getLogicalTableAlias();
	//public abstract void setLogicalTableAlias(String logicalTableAlias);
	public abstract boolean isPossibleInstance(String uri);
	public abstract Long getLogicalTableSize();
	public abstract TableMetaData getTableMetaData();
	public abstract Collection<String> getMappedClassURIs();
	public abstract AbstractLogicalTable getLogicalTable();
	
	protected Resource resource;
	
	public Resource getResource() {
		return resource;
	}
	public void setResource(Resource resource) {
		this.resource = resource;
	}
}
