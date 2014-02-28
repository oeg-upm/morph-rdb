package es.upm.fi.dia.oeg.obdi.core.model;

import java.util.Collection;
import java.util.List;

import com.hp.hpl.jena.rdf.model.Resource;

import es.upm.fi.dia.oeg.morph.base.model.IConceptMapping;
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseLogicalTable;
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData;
import es.upm.fi.dia.oeg.morph.base.sql.MorphTableMetaData;




public abstract class AbstractConceptMapping 
extends AbstractRDB2RDFMapping implements IConceptMapping {
	public abstract String getConceptName();
	public abstract Collection<AbstractPropertyMapping> getPropertyMappings(String propertyURI);
	public abstract Collection<AbstractPropertyMapping> getPropertyMappings();
//	public abstract Collection<IRelationMapping> getRelationMappings();
	//public abstract String getLogicalTableAlias();
	//public abstract void setLogicalTableAlias(String logicalTableAlias);
	public abstract boolean isPossibleInstance(String uri);
	public abstract MorphBaseLogicalTable getLogicalTable();
	public abstract Long getLogicalTableSize();
	public abstract MorphTableMetaData getTableMetaData();
	public abstract Collection<String> getMappedClassURIs();
	public abstract List<String> getSubjectReferencedColumns();
	
	protected AbstractMappingDocument owner;
	public AbstractMappingDocument getOwner() { return owner; }
	public void setOwner(AbstractMappingDocument owner) { this.owner = owner; }
	
	protected Resource resource = null;
	public Resource getResource() { return resource; }
	public void setResource(Resource resource) { this.resource = resource;	}	
	
	public abstract void buildMetaData(MorphDatabaseMetaData dbMetaData);
	

	
}
