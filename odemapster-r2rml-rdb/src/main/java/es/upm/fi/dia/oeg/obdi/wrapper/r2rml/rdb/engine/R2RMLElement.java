package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine;


public interface R2RMLElement {
	public Object accept(R2RMLElementVisitor visitor) throws Exception;
}
