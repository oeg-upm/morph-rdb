package es.upm.fi.dia.oeg.obdi.core.querytranslator;

import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.algebra.optimize.Optimize;
import com.hp.hpl.jena.sparql.algebra.optimize.Rewrite;
import com.hp.hpl.jena.sparql.algebra.optimize.TransformFilterConjunction;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.ExprList;

import es.upm.fi.dia.oeg.morph.base.MorphTriple;
import es.upm.fi.dia.oeg.newrqr.KyrieLib;


public class KyrieRewritter implements Rewrite {
  private static Logger logger = Logger.getLogger(KyrieRewritter.class);
	private KyrieLib kyrie;
	private List<Var> vars;

	public KyrieRewritter(String ontologyFile, Collection<String> mappedPredicates) {
		this.kyrie = new KyrieLib(ontologyFile, mappedPredicates, true);
	}

	public Op rewrite(Op op) {
		Op result = null;

		if (op instanceof OpBGP) { // triple or bgp pattern
			OpBGP bgp = (OpBGP) op;
			result = this.secondRewrite(this.kyrie.rewriteOp(bgp, this.vars));
		} else if (op instanceof OpJoin) { // AND pattern
			OpJoin opJoin = (OpJoin) op;
			Op leftChild = opJoin.getLeft();
			Op rightChild = opJoin.getRight();
			Op leftChildRewritten = this.rewrite(leftChild);
			Op rightChildRewritten = this.rewrite(rightChild);
			if (leftChildRewritten instanceof OpBGP && rightChildRewritten instanceof OpBGP) {
				OpBGP leftChildRewrittenBGP = (OpBGP) leftChildRewritten;
				OpBGP rightChildRewrittenBGP = (OpBGP) rightChildRewritten;
				leftChildRewrittenBGP.getPattern().addAll(rightChildRewrittenBGP.getPattern());
				result = leftChildRewrittenBGP;
			} else
				result = OpJoin.create(leftChildRewritten, rightChildRewritten);

		} else if (op instanceof OpLeftJoin) { // OPT pattern
			OpLeftJoin opLeftJoin = (OpLeftJoin) op;
			ExprList exprList = opLeftJoin.getExprs();
			Op leftChild = opLeftJoin.getLeft();
			Op rightChild = opLeftJoin.getRight();
			Op leftChildRewritten = this.rewrite(leftChild);
			Op rightChildRewritten = this.rewrite(rightChild);

			if (leftChildRewritten instanceof OpBGP && rightChildRewritten instanceof OpBGP) {
				OpBGP leftChildRewrittenBGP = (OpBGP) leftChildRewritten;
				OpBGP rightChildRewrittenBGP = (OpBGP) rightChildRewritten;
				BasicPattern rightBasicPattern = rightChildRewrittenBGP.getPattern();
				int rightBasicPatternSize = rightBasicPattern.size();
				if (rightBasicPatternSize == 1) {
					KyrieRewritter.logger.debug("Optional pattern with only one triple pattern.");

					Triple rightTp = rightChildRewrittenBGP.getPattern().get(0);
					Node rightTpSubject = rightTp.getSubject();
					Node rightTpObject = rightTp.getObject();

					Collection<Triple> leftChildTriples = leftChildRewrittenBGP.getPattern().getList();
					Collection<Node> leftChildSubjects = QueryTranslatorUtility.getSubjects(leftChildTriples);
					Collection<Node> leftChildObjects = QueryTranslatorUtility.getObjects(leftChildTriples);

					if (leftChildSubjects.contains(rightTpSubject) && !leftChildObjects.contains(rightTpObject)) {
						MorphTriple rightEtp = new MorphTriple(
								rightTp.getSubject(), rightTp.getPredicate(), rightTp.getObject(), true);
						BasicPattern leftChildRewrittenPattern = leftChildRewrittenBGP.getPattern();
						leftChildRewrittenPattern.add(rightEtp);
						KyrieRewritter.logger.debug("leftChildRewrittenPattern = " + leftChildRewrittenPattern);
						result = leftChildRewrittenBGP;
					} else
						result = OpLeftJoin.create(leftChildRewritten, rightChildRewritten, exprList);
				} else
					result = OpLeftJoin.create(leftChildRewritten, rightChildRewritten, exprList);
			} else
				result = OpLeftJoin.create(leftChildRewritten, rightChildRewritten, exprList);
		} else if (op instanceof OpUnion) { // UNION pattern
			OpUnion opUnion = (OpUnion) op;
			Op leftChild = opUnion.getLeft();
			Op rightChild = opUnion.getRight();
			Op leftChildRewritten = this.rewrite(leftChild);
			Op rightChildRewritten = this.rewrite(rightChild);
			result = new OpUnion(leftChildRewritten, rightChildRewritten);
		} else if (op instanceof OpFilter) { // FILTER pattern
			OpFilter opFilter = (OpFilter) op;
			ExprList exprs = opFilter.getExprs();
			Op subOp = opFilter.getSubOp();

			TransformFilterConjunction tfc = new TransformFilterConjunction();
			Optimize.apply("test", tfc, opFilter);

			Op subOpRewritten = this.rewrite(subOp);
			result = OpFilter.filter(exprs, subOpRewritten);
		} else if (op instanceof OpProject) {
			OpProject opProject = (OpProject) op;
			Op subOp = opProject.getSubOp();
			Op subOpRewritten = this.rewrite(subOp);
			this.vars = opProject.getVars();
			result = new OpProject(subOpRewritten, opProject.getVars());
		} else if (op instanceof OpSlice) {
			OpSlice opSlice = (OpSlice) op;
			Op subOp = opSlice.getSubOp();
			Op subOpRewritten = this.rewrite(subOp);
			result = new OpSlice(subOpRewritten, opSlice.getStart(), opSlice.getLength());
		} else if (op instanceof OpDistinct) {
			OpDistinct opDistinct = (OpDistinct) op;
			Op subOp = opDistinct.getSubOp();
			Op subOpRewritten = this.rewrite(subOp);
			result = new OpDistinct(subOpRewritten);
		} else if (op instanceof OpOrder) {
			OpOrder opOrder = (OpOrder) op;
			Op subOp = opOrder.getSubOp();
			Op subOpRewritten = this.rewrite(subOp);
			result = new OpOrder(subOpRewritten, opOrder.getConditions());
		} else
			result = op;

		return result;
	}

	public Op secondRewrite(Op op) {
		Op result = null;

		if (op instanceof OpBGP) { // triple or bgp pattern
			OpBGP bgp = (OpBGP) op;
			result = QueryTranslatorUtility.groupTriplesBySubject(bgp);

			// List<OpBGP> bgpSplitted = QueryTranslatorUtility.splitBGP(bgp2.getPattern().getList());
			// if(bgpSplitted.size() > 1) {
			// Op op2 = QueryTranslatorUtility.bgpsToJoin(bgpSplitted);
			// return op2;
			// } else {
			// return bgp2;
			// }
		} else if (op instanceof OpJoin) { // AND pattern
			OpJoin opJoin = (OpJoin) op;
			Op leftChild = opJoin.getLeft();
			Op rightChild = opJoin.getRight();
			Op leftChildRewritten = this.rewrite(leftChild);
			Op rightChildRewritten = this.rewrite(rightChild);
			if (leftChildRewritten instanceof OpBGP && rightChildRewritten instanceof OpBGP) {
				OpBGP leftChildRewrittenBGP = (OpBGP) leftChildRewritten;
				OpBGP rightChildRewrittenBGP = (OpBGP) rightChildRewritten;
				leftChildRewrittenBGP.getPattern().addAll(rightChildRewrittenBGP.getPattern());
				result = leftChildRewrittenBGP;
			} else
				result = OpJoin.create(leftChildRewritten, rightChildRewritten);

		} else if (op instanceof OpLeftJoin) { // OPT pattern
			OpLeftJoin opLeftJoin = (OpLeftJoin) op;
			ExprList exprList = opLeftJoin.getExprs();
			Op leftChild = opLeftJoin.getLeft();
			Op rightChild = opLeftJoin.getRight();
			Op leftChildRewritten = this.rewrite(leftChild);
			Op rightChildRewritten = this.rewrite(rightChild);

			if (leftChildRewritten instanceof OpBGP && rightChildRewritten instanceof OpBGP) {
				OpBGP leftChildRewrittenBGP = (OpBGP) leftChildRewritten;
				OpBGP rightChildRewrittenBGP = (OpBGP) rightChildRewritten;
				BasicPattern rightBasicPattern = rightChildRewrittenBGP.getPattern();
				int rightBasicPatternSize = rightBasicPattern.size();
				if (rightBasicPatternSize == 1) {
					KyrieRewritter.logger.debug("Optional pattern with only one triple pattern.");

					Triple rightTp = rightChildRewrittenBGP.getPattern().get(0);
					Node rightTpSubject = rightTp.getSubject();
					Node rightTpObject = rightTp.getObject();

					Collection<Triple> leftChildTriples = leftChildRewrittenBGP.getPattern().getList();
					Collection<Node> leftChildSubjects = QueryTranslatorUtility.getSubjects(leftChildTriples);
					Collection<Node> leftChildObjects = QueryTranslatorUtility.getObjects(leftChildTriples);

					if (leftChildSubjects.contains(rightTpSubject) && !leftChildObjects.contains(rightTpObject)) {
						MorphTriple rightEtp = new MorphTriple(
								rightTp.getSubject(), rightTp.getPredicate(), rightTp.getObject(), true);
						BasicPattern leftChildRewrittenPattern = leftChildRewrittenBGP.getPattern();
						leftChildRewrittenPattern.add(rightEtp);
						KyrieRewritter.logger.debug("leftChildRewrittenPattern = " + leftChildRewrittenPattern);
						result = leftChildRewrittenBGP;
					} else
						result = OpLeftJoin.create(leftChildRewritten, rightChildRewritten, exprList);
				} else
					result = OpLeftJoin.create(leftChildRewritten, rightChildRewritten, exprList);
			} else
				result = OpLeftJoin.create(leftChildRewritten, rightChildRewritten, exprList);
		} else if (op instanceof OpUnion) { // UNION pattern
			OpUnion opUnion = (OpUnion) op;
			Op leftChild = opUnion.getLeft();
			Op rightChild = opUnion.getRight();
			Op leftChildRewritten = this.rewrite(leftChild);
			Op rightChildRewritten = this.rewrite(rightChild);
			result = new OpUnion(leftChildRewritten, rightChildRewritten);
		} else if (op instanceof OpFilter) { // FILTER pattern
			OpFilter opFilter = (OpFilter) op;
			ExprList exprs = opFilter.getExprs();
			Op subOp = opFilter.getSubOp();

			TransformFilterConjunction tfc = new TransformFilterConjunction();
			Optimize.apply("test", tfc, opFilter);

			// Op op2 = null;
			// if(subOp instanceof OpBGP) {
			// BasicPattern basicPattern = ((OpBGP) subOp).getPattern();
			// op2 = TransformFilterPlacement.transform(exprs, basicPattern);
			// }
			Op subOpRewritten = this.rewrite(subOp);
			result = OpFilter.filter(exprs, subOpRewritten);
		} else if (op instanceof OpProject) {
			// logger.debug("op instanceof OpProject/OpSlice/OpDistinct");
			OpProject opProject = (OpProject) op;
			Op subOp = opProject.getSubOp();
			Op subOpRewritten = this.rewrite(subOp);
			result = new OpProject(subOpRewritten, opProject.getVars());
		} else if (op instanceof OpSlice) {
			OpSlice opSlice = (OpSlice) op;
			Op subOp = opSlice.getSubOp();
			Op subOpRewritten = this.rewrite(subOp);
			result = new OpSlice(subOpRewritten, opSlice.getStart(), opSlice.getLength());
		} else if (op instanceof OpDistinct) {
			OpDistinct opDistinct = (OpDistinct) op;
			Op subOp = opDistinct.getSubOp();
			Op subOpRewritten = this.rewrite(subOp);
			result = new OpDistinct(subOpRewritten);
		} else if (op instanceof OpOrder) {
			OpOrder opOrder = (OpOrder) op;
			Op subOp = opOrder.getSubOp();
			Op subOpRewritten = this.rewrite(subOp);
			result = new OpOrder(subOpRewritten, opOrder.getConditions());
		} else
			result = op;

		return result;
	}

}
