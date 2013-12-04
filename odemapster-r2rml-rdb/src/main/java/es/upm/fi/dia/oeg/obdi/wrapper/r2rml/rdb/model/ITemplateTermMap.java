package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model;

import java.util.Collection;
import java.util.Map;

public interface ITemplateTermMap {
	public String getTemplateString();
	public Collection<String> getTemplateColumns();
	public Map<String, String> getTemplateValues(String uri);
}
