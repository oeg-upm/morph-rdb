package es.upm.fi.dia.oeg.obdi.core;

import org.w3c.dom.Element;

import es.upm.fi.dia.oeg.obdi.core.exception.ParseException;


public interface IParseable {
	public void parse(Element xmlElement) throws ParseException;
}
