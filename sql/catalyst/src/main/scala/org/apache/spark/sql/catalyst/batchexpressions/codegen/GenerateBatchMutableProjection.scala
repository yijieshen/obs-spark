package org.apache.spark.sql.catalyst.batchexpressions.codegen

import org.apache.spark.sql.catalyst.batchexpressions.RBProjection
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._

object GenerateBatchMutableProjection extends BatchCodeGenerator[Seq[Expression], () => RBProjection] {

  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  val outputRowBatch = newTermName(s"output")

  override protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer(_))

  override protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  override protected def create(expressions: Seq[Expression]): (() => RBProjection) = {
    val projectionCode = expressions.zipWithIndex.flatMap { case(e, i) =>
      val evaluationCode = expressionEvaluator(e)
      evaluationCode.code :+
      q"""
        ${setCV(outputRowBatch, i, evaluationCode.cvTerm)}
      """
    }
    val code =
      q"""
        () => { new $rbProjectionType {
            private[this] var $outputRowBatch: $rowBatchType = null

            def apply(input: $rowBatchType): $rowBatchType = {
              if($outputRowBatch == null) {
                $outputRowBatch = new $rowBatchType(input.rowNum, ${expressions.size})
              }
              ..$projectionCode
              output.curRowNum = input.curRowNum
              output.curSelector = input.curSelector
              output
            }
          }
        }
       """
//    println(show(code))
    toolBox.eval(code).asInstanceOf[() => RBProjection]
  }

}
