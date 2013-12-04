package es.upm.fi.dia.oeg.obdi.core.model;

import com.hp.hpl.jena.rdf.model.Resource;





public abstract class AbstractPropertyMapping extends AbstractRDB2RDFMapping implements IPropertyMapping{
	public abstract String getMappedPredicateName();
	
	protected AbstractConceptMapping parent;

	protected Resource resource;
	
	public AbstractConceptMapping getParent() {
		return parent;
	}
	
	public abstract MappingType getPropertyMappingType();

	public boolean isObjectPropertyMapping() {
		return MappingType.RELATION == this.getPropertyMappingType();
	}

	public boolean isDataPropertyMapping() {
		return MappingType.ATTRIBUTE == this.getPropertyMappingType();
	}

	public Resource getResource() {
		return resource;
	}

	public void setResource(Resource resource) {
		this.resource = resource;
	}

	
	
}
