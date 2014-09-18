package org.apache.spark.sql.catalyst.batchexpressions

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}
import org.apache.spark.sql.catalyst.rules

/**
 * A collection of generators that build custom bytecode at runtime for performing the batch evaluation
 * of catalyst expression.
 */
package object codegen {

  /**
   * A lock to protect invoking the scala compiler at runtime, since it is not thread safe in Scala
   * 2.10.
   */
  protected[codegen] val globalLock = org.apache.spark.sql.catalyst.ScalaReflectionLock

  /** Canonicalizes an expression so those that differ only by names can reuse the same code. */
  object ExpressionCanonicalizer extends rules.RuleExecutor[Expression] {
    val batches =
      Batch("CleanExpressions", FixedPoint(20), CleanExpressions) :: Nil

    object CleanExpressions extends rules.Rule[Expression] {
      def apply(e: Expression): Expression = e transform {
        case Alias(c, _) => c
      }
    }
  }

}
