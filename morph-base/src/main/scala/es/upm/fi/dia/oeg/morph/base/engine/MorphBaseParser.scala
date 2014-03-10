package es.upm.fi.dia.oeg.morph.base.engine

import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument

abstract class MorphBaseParser {
	def parse(mappingResource:Object ) : MorphBaseMappingDocument;;
}