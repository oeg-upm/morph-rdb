package es.upm.dia.fi.oeg.morph.r2rml.model

trait IConstantTermMap {
   var constantValue:String=null;
  
	def setConstantValue(constantValue:String)={this.constantValue=constantValue};
	def getConstantValue():String = {this.constantValue};
}