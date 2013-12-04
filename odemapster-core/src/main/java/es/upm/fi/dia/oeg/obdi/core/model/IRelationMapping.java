package es.upm.fi.dia.oeg.obdi.core.model;

public interface IRelationMapping extends IPropertyMapping{
	public String getRelationName();
	public String getRangeClassMapping();
	public AbstractConceptMapping getParent();
}
