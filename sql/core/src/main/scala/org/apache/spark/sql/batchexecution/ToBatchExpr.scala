/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
        case BoundReference(ordinal, dataType, nullable) =>
          BatchBoundReference(ordinal, dataType, nullable)
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
        case EqualTo(left: BE, right: BE) =>
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
