package org.apache.spark.sql.batchexecution

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.batchexpressions.{BatchExpression => BE}
import org.apache.spark.sql.catalyst.batchexpressions._

private[sql] case class ToBatchExpr(sqlContext: SQLContext)
  extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = plan.transform {
    case op: SparkPlan =>
      op.transformExpressionsUp {
        case Literal(value, dataType) =>
          BatchLiteral(value, dataType)
        case Rand =>
          BatchRand
        case BoundReference(ordinal, baseReference) =>
          BatchBoundReference(ordinal, baseReference)
        case a @ Alias(child: BE, name) =>
          BatchAlias(child, name)(a.exprId, a.qualifiers)

        //Arithmatic exprs
        case UnaryMinus(child: BE) =>
          BatchUnaryMinus(child)
        case Add(left: BE, right: BE) =>
          BatchAdd(left, right)
        case Subtract(left: BE, right: BE) =>
          BatchSubtract(left, right)
        case Multiply(left: BE, right: BE) =>
          BatchMultiply(left, right)
        case Divide(left: BE, right: BE) =>
          BatchDivide(left, right)
        case Remainder(left: BE, right: BE) =>
          BatchRemainder(left, right)

        //Logical exprs
        case Not(child: BE) =>
          BatchNot(child)
        case And(left: BE, right: BE) =>
          BatchAnd(left, right)
        case Or(left: BE, right: BE) =>
          BatchOr(left, right)

        //Predicate exprs
        case Equals(left: BE, right: BE) =>
          BatchEquals(left, right)
        case LessThan(left: BE, right: BE) =>
          BatchLessThan(left, right)
        case LessThanOrEqual(left: BE, right: BE) =>
          BatchLessThanOrEqual(left, right)
        case GreaterThan(left: BE, right: BE) =>
          BatchGreaterThan(left, right)
        case GreaterThanOrEqual(left: BE, right: BE) =>
          BatchGreaterThanOrEqual(left, right)
      }
  }
}