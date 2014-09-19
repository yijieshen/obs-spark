package org.apache.spark.sql.catalyst.batchexpressions.codegen

import org.apache.spark.sql.catalyst.batchexpressions._
import org.apache.spark.sql.catalyst.expressions._

object GenerateBatchPredicate extends BatchCodeGenerator[Expression, (RowBatch) => RowBatch]{

  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  override protected def canonicalize(in: Expression): Expression =
    ExpressionCanonicalizer(in)

  override protected def bind(in: Expression, inputSchema: Seq[Attribute]): Expression =
    BindReferences.bindReference(in, inputSchema)

  override protected def create(predicate: Expression): ((RowBatch) => RowBatch) = {
    val selector = freshName("selector")
    val eval = expressionEvaluator(predicate)

    val code =
      q"""
        (input: $rowBatchType) => {
          ..${eval.code}
          val $selector = ${eval.cvTerm}.asInstanceOf[${typeOf[BooleanColumnVector]}].bs
          input.curSelector = $selector
          input
        }
      """
//    println(show(code))
    toolBox.eval(code).asInstanceOf[RowBatch => RowBatch]
  }
}
