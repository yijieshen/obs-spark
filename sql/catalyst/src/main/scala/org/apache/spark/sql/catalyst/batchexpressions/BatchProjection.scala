package org.apache.spark.sql.catalyst.batchexpressions

import org.apache.spark.sql.catalyst.expressions._

class BatchProjection(expressions: Seq[Expression]) extends (RowBatch => RowBatch) {

  protected val exprArray = expressions.toArray

  def apply(input: RowBatch): RowBatch = {
    val n2v = input.name2Vector
    var i = 0
    while (i < exprArray.length) {
      val curExpr = exprArray(i)
      curExpr match {
        case BatchAlias(l @ BatchLiteral(_, _), name) =>
          n2v(name) = l.v
        case BatchAlias(be: BatchExpression, name) =>
          val ber = be.eval(input)
          n2v(name) = ber
        case _ =>
      }
      i += 1
    }
    input
  }
}
