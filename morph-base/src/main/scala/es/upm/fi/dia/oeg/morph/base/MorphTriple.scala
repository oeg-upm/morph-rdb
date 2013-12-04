package es.upm.fi.dia.oeg.morph.base

import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.graph.Node

class MorphTriple(s:Node, p:Node, o:Node, val isSingleTripleFromTripleBlock:Boolean) 
extends Triple(s:Node,p:Node,o:Node) {

  override def toString() = {
    super.toString + ":" + isSingleTripleFromTripleBlock;
  }
}