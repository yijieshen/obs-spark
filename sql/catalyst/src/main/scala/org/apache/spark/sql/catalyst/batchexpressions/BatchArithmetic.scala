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

package org.apache.spark.sql.catalyst.batchexpressions

import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.types._

case class BatchUnaryMinus(child: BatchExpression) extends UnaryBatchExpression {
  type EvaluatedType = ColumnVector

  def dataType = child.dataType
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"-$child"

  override def eval(input: RowBatch): ColumnVector = {
    n1b(child, input, _.negate(_))
  }
}

abstract class BatchBinaryArithmetic extends BinaryBatchExpression {
  self: Product =>

  type EvaluatedType = ColumnVector

  def nullable = left.nullable || right.nullable

  override lazy val resolved =
    left.resolved && right.resolved && left.dataType == right.dataType

  def dataType = {
    if (!resolved) {
      throw new UnresolvedException(this,
        s"datatype. Can not resolve due to differing types ${left.dataType}, ${right.dataType}")
    }
    left.dataType
  }
}

case class BatchAdd(left: BatchExpression, right: BatchExpression) extends BatchBinaryArithmetic {
  def symbol = "+"

  override def eval(input: RowBatch): Any = n2b(input, left, right, (numeric, x, y) => numeric.plus(x, y))
}

case class BatchSubtract(left: BatchExpression, right: BatchExpression) extends BatchBinaryArithmetic {
  def symbol = "-"

  override def eval(input: RowBatch): Any = n2b(input, left, right, _.minus(_, _))
}

case class BatchMultiply(left: BatchExpression, right: BatchExpression) extends BatchBinaryArithmetic {
  def symbol = "*"

  override def eval(input: RowBatch): Any = n2b(input, left, right, _.times(_, _))
}

case class BatchDivide(left: BatchExpression, right: BatchExpression) extends BatchBinaryArithmetic {
  def symbol = "/"

  override def eval(input: RowBatch): Any = dataType match {
    case _: FractionalType => f2b(input, left, right, _.div(_, _))
    case _: IntegralType => i2b(input, left , right, _.quot(_, _))
  }

}

case class BatchRemainder(left: BatchExpression, right: BatchExpression) extends BatchBinaryArithmetic {
  def symbol = "%"

  override def eval(input: RowBatch): Any = i2b(input, left, right, _.rem(_, _))
}
