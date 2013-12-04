package es.upm.fi.dia.oeg.obdi.core.materializer;

import java.io.IOException;

public class NQuadMaterializer extends AbstractMaterializer {

	@Override
	public Object createSubject(boolean isBlankNode, String subjectURI) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void materializeDataPropertyTriple(String predicateName,
			Object objectValue, String datatype, String lang, String graph) {
		// TODO Auto-generated method stub

	}

	@Override
	public void materializeObjectPropertyTriple(String predicateName,
			String rangeURI, boolean isBlankNodeObject, String graph) {
		// TODO Auto-generated method stub

	}

	@Override
	public void materializeRDFTypeTriple(String subjectURI, String conceptName,
			boolean isBlankNodeSubject, String graph) {
		// TODO Auto-generated method stub

	}

	@Override
	public void materialize() throws IOException {
		// TODO Auto-generated method stub

	}

}
