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

import org.apache.spark.sql.catalyst.types._

trait BatchPredicate extends BatchExpression {
  self: Product =>

  def dataType = BooleanType

  type EvaluatedType = ColumnVector
}

abstract class BinaryBatchPredicate extends BinaryBatchExpression with BatchPredicate {
  self: Product =>
  def nullable = left.nullable || right.nullable
}

case class BatchNot(child: BatchExpression) extends UnaryBatchExpression with BatchPredicate {
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"NOT $child"

  override def eval(input: RowBatch): ColumnVector = {
    val childCV = child.eval(input)

    val selector = input.curSelector
    val notNullArray = childCV.notNullArray
    val bitmap = andWithNull(selector, notNullArray, false)

    val rstComple = childCV.asInstanceOf[BooleanColumnVector].bs.complement
    val resultBitmap = andWithNull(bitmap, rstComple, false)

    val resultCV = new BooleanColumnVector(input.curRowNum, resultBitmap)
    resultCV.notNullArray = notNullArray
    resultCV
  }
}

case class BatchAnd(left: BatchExpression, right: BatchExpression) extends BinaryBatchPredicate {
  def symbol = "&&"

  override def eval(input: RowBatch): ColumnVector = {
    val evalLeft = left.eval(input)
    val evalRight = right.eval(input)

    val selector = input.curSelector
    val notNullArrayLeft = evalLeft.notNullArray
    val notNullArrayRight = evalRight.notNullArray

    val notNullArrayResult = andWithNull(notNullArrayLeft, notNullArrayRight, true)
    val usefulPosArray = andWithNull(selector, notNullArrayResult, false)

    val leftBitMap = evalLeft.asInstanceOf[BooleanColumnVector].bs
    val rightBitMap = evalRight.asInstanceOf[BooleanColumnVector].bs

    val resultBitMap = andWithNull(usefulPosArray, leftBitMap & rightBitMap, false)

    val resultCV = new BooleanColumnVector(input.curRowNum, resultBitMap)
    resultCV.notNullArray = notNullArrayResult
    resultCV
  }
}

case class BatchOr(left: BatchExpression, right: BatchExpression) extends BinaryBatchPredicate {
  def symbol = "||"

  override def eval(input: RowBatch): ColumnVector = {
    val evalLeft = left.eval(input)
    val evalRight = right.eval(input)

    val selector = input.curSelector
    val notNullArrayLeft = evalLeft.notNullArray
    val notNullArrayRight = evalRight.notNullArray

    val notNullArrayResult = andWithNull(notNullArrayLeft, notNullArrayRight, true)
    val usefulPosArray = andWithNull(selector, notNullArrayResult, false)

    val leftBitMap = evalLeft.asInstanceOf[BooleanColumnVector].bs
    val rightBitMap = evalRight.asInstanceOf[BooleanColumnVector].bs

    val resultBitMap = andWithNull(usefulPosArray, leftBitMap | rightBitMap, false)

    val resultCV = new BooleanColumnVector(input.curRowNum, resultBitMap)
    resultCV.notNullArray = notNullArrayResult
    resultCV
  }
}

abstract class BinaryBatchComparison extends BinaryBatchPredicate {
  self: Product =>
}

/**
 * Comparison rules: (TODO: check the rules)
 *
 * Types inside numeric type can be compared,
 * types cross numeric type boundary would always yield false
 * types out of numeric type and inside Native type can only compared with self type
 * @param left
 * @param right
 */
case class BatchEquals(left: BatchExpression, right: BatchExpression)
  extends BinaryBatchComparison {
  def symbol = "="
  override def eval(input: RowBatch): ColumnVector = {

    val leftdt = left.dataType
    val rightdt = right.dataType
    val resultType = BooleanType
    (leftdt, rightdt) match {
      case (l: NumericType, r: NumericType) =>
        val leftCV = left.eval(input)
        val rightCV = right.eval(input)
        val leftGet = (leftCV.get _).asInstanceOf[(Int) => l.JvmType]
        val rightGet = (rightCV.get _).asInstanceOf[(Int) => r.JvmType]

        val blOut = new BitSet(input.curRowNum)

        //prepare bitmap for calculation
        val notNullArray1 = leftCV.notNullArray
        val notNullArray2 = rightCV.notNullArray
        val notNullArrayResult = andWithNull(notNullArray1, notNullArray2, true)
        val selector = input.curSelector
        val bitmap = andWithNull(notNullArrayResult, selector, false)

        //iteratively calculate
        if (bitmap != null) {
          bitmap.availableBits = input.curRowNum
          val iter = bitmap.iterator
          var i = 0
          while (iter.hasNext) {
            i = iter.next()
            blOut.set(i, leftGet(i) == rightGet(i))
          }
        } else {
          val rowNum = input.curRowNum
          var i = 0
          while (i < rowNum) {
            blOut.set(i, leftGet(i) == rightGet(i))
            i += 1
          }
        }

        //prepare result
        val outputCV = new BooleanColumnVector(input.curRowNum, blOut)
        if (notNullArrayResult != null) {
          outputCV.notNullArray = notNullArrayResult
        }
        outputCV

      case (_ : NumericType, _ : NativeType) | (_ : NativeType, _ : NumericType) =>
        //always false, don't need to calculate
        val evalLeft = left.eval(input)
        val evalRight = right.eval(input)

        //prepare bitmap for calculation
        val notNullArray1 = evalLeft.notNullArray
        val notNullArray2 = evalRight.notNullArray
        val notNullArrayResult = andWithNull(notNullArray1, notNullArray2, true)

        //prepare result
        val outputCV = new BooleanColumnVector(input.curRowNum, new BitSet(input.curRowNum))
        if (notNullArrayResult != null) {
          outputCV.notNullArray = notNullArrayResult
        }
        outputCV

      case (BooleanType, BooleanType) =>
        val evalLeft = left.eval(input)
        val evalRight = right.eval(input)

        //prepare bitmap for calculation
        val notNullArray1 = evalLeft.notNullArray
        val notNullArray2 = evalRight.notNullArray
        val notNullArrayResult = andWithNull(notNullArray1, notNullArray2, true)
        val selector = input.curSelector
        val bitmap = andWithNull(notNullArrayResult, selector, false)

        val leftBitMap = evalLeft.asInstanceOf[BooleanColumnVector].bs
        val rightBitMap = evalRight.asInstanceOf[BooleanColumnVector].bs

        val xnorResult = (leftBitMap ^ rightBitMap).complement
        val blResult = andWithNull(bitmap, xnorResult, false)

        //prepare result
        val resultCV = new BooleanColumnVector(input.curRowNum, blResult)
        resultCV.notNullArray = notNullArrayResult
        resultCV

      case (StringType, StringType) =>
        sys.error(s"Type String does not support now")
      case (TimestampType, TimestampType) =>
        sys.error(s"Type timestamp does not support now")

    }
  }
}

case class BatchLessThan(left: BatchExpression, right: BatchExpression)
  extends BinaryBatchComparison {
  def symbol = "<"
  override def eval(input: RowBatch): EvaluatedType = c2b(input, left, right, _.lt(_, _))
}

case class BatchLessThanOrEqual(left: BatchExpression, right: BatchExpression)
  extends BinaryBatchComparison {
  def symbol = "<="
  override def eval(input: RowBatch): EvaluatedType = c2b(input, left, right, _.lteq(_, _))
}

case class BatchGreaterThan(left: BatchExpression, right: BatchExpression)
  extends BinaryBatchComparison {
  def symbol = ">"
  override def eval(input: RowBatch): EvaluatedType = c2b(input, left, right, _.gt(_, _))
}

case class BatchGreaterThanOrEqual(left: BatchExpression, right: BatchExpression)
  extends BinaryBatchComparison {
  def symbol = ">="
  override def eval(input: RowBatch): EvaluatedType = c2b(input, left, right, _.gteq(_, _))
}
