package org.apache.spark.sql.catalyst.batchexpressions.codegen

import org.apache.spark.sql.catalyst.batchexpressions.RBProjection
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._

object GenerateBatchMutableProjection extends BatchCodeGenerator[Seq[Expression], () => RBProjection] {

  override protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer(_))

  override protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  override protected def create(expressions: Seq[Expression]): (() => RBProjection) = {
    val projectionCode = expressions.zipWithIndex.flatMap { case(e, i) =>
      val evaluationCode = expressionEvaluator(e)
      evaluationCode.code :+
      q"""
        ${setCV()}
      """
    }
    val code = q""""""
    toolBox.eval(code).asInstanceOf[() => RBProjection]
  }

}
