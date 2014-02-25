package es.upm.fi.dia.oeg.obdi.core.querytranslator;

import java.util.List;
import java.util.Vector;

public class AlphaResultUnion {
	private List<AlphaResult> alphaResultUnion;
	public AlphaResultUnion() {
		this.alphaResultUnion = new Vector<AlphaResult>();
	}
	
	public AlphaResultUnion(List<AlphaResult> alphaResultUnion) {
		super();
		this.alphaResultUnion = alphaResultUnion;
	}
	
	public AlphaResultUnion(AlphaResult alphaResult) {
		super();
		this.alphaResultUnion = new Vector<AlphaResult>();
		this.alphaResultUnion.add(alphaResult);
	}

	public int size() {
		return this.alphaResultUnion.size();
	}
	
	public AlphaResult get(String predicateURI) {
		AlphaResult result = null;
		
		if(predicateURI != null) {
			for(AlphaResult alphaResult : this.alphaResultUnion) {
				if(predicateURI.equals(alphaResult.getPredicateURI())) {
					result = alphaResult;
				}
			}			
		}
		
		return result;

	}
	
	public AlphaResult get(int i) {
		return this.alphaResultUnion.get(i);
	}

	@Override
	public String toString() {
		return "AlphaResultUnion [alphaResultUnion=" + alphaResultUnion + "]";
	}
	
	public void add(AlphaResult newAlphaResult) {
		this.alphaResultUnion.add(newAlphaResult);
	}
}
