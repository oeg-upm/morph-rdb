package es.upm.fi.dia.oeg.obdi.core.querytranslator;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

public class ExtendedTriple extends Triple implements Comparable<ExtendedTriple>{
	boolean isSingleTripleFromTripleBlock = false;
	
	public boolean isSingleTripleFromTripleBlock() {
		return isSingleTripleFromTripleBlock;
	}

	public void setSingleTripleFromTripleBlock(boolean isSingleTripleFromTripleBlock) {
		this.isSingleTripleFromTripleBlock = isSingleTripleFromTripleBlock;
	}

	public ExtendedTriple(Node s, Node p, Node o) {
		super(s, p, o);
	}

	public int compareTo(ExtendedTriple o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
