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
        case Alias(l @ BatchLiteral(_, _), name) =>
          n2v(name) = l.v
        case Alias(be: BatchExpression, name) =>
          val ber = be.eval(input)
          n2v(name) = ber
        //case BoundReference(_, AttributeReference(name, _, _)) =>
      }
      i += 1
    }

    input
  }
}
