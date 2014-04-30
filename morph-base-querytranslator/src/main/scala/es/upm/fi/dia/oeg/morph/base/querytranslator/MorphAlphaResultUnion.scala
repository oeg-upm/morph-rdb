package es.upm.fi.dia.oeg.morph.base.querytranslator



class MorphAlphaResultUnion(var alphaResultUnion:List[MorphAlphaResult]) {
	def this() = {this(Nil)}
	
	def this(alphaResult:MorphAlphaResult) = {this(List(alphaResult))}
	
	def size() : Integer = {
		this.alphaResultUnion.size;
	}
	
	def get(predicateURI:String ) : MorphAlphaResult = {
		var result:MorphAlphaResult = null;
		
		if(predicateURI != null) {
			for(alphaResult <- this.alphaResultUnion) {
				for(alphaPO <- alphaResult.alphaPredicateObjects) {
					if(predicateURI.equals(alphaPO._2)) {
						result = alphaResult;
					}				  
				}

			}			
		}
		
		result;
	}
	
	def get(i:Integer) : MorphAlphaResult = {
		this.alphaResultUnion(i);
	}

	def add(newAlphaResult:MorphAlphaResult) {
		this.alphaResultUnion = this.alphaResultUnion ::: List(newAlphaResult);
	}
}

