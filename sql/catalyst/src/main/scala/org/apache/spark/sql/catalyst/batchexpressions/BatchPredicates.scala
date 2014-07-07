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
    val evalE = child.eval(input)

    val selector = input.curSelector
    val notNullArray = evalE.notNullArray
    val bitmap = andWithNull(selector, notNullArray, false)

    val rstComple = evalE.content.asInstanceOf[BooleanMemory].bs.complement
    val resultBitmap = andWithNull(bitmap, rstComple, false)

    val rcv = input.getVector(dataType, true)
    rcv.setContent(new BooleanMemory(resultBitmap))
    rcv.setNullable(notNullArray)

    rcv
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

    val leftBitMap = evalLeft.content.asInstanceOf[BooleanMemory].bs
    val rightBitMap = evalRight.content.asInstanceOf[BooleanMemory].bs

    val resultBitMap = andWithNull(usefulPosArray, leftBitMap & rightBitMap, false)

    val rcv = input.getVector(dataType, true)
    rcv.setContent(new BooleanMemory(resultBitMap))
    rcv.setNullable(notNullArrayResult)
    rcv
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

    val leftBitMap = evalLeft.content.asInstanceOf[BooleanMemory].bs
    val rightBitMap = evalRight.content.asInstanceOf[BooleanMemory].bs

    val resultBitMap = andWithNull(usefulPosArray, leftBitMap | rightBitMap, false)

    val rcv = input.getVector(dataType, true)
    rcv.setContent(new BooleanMemory(resultBitMap))
    rcv.setNullable(notNullArrayResult)
    rcv
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
case class BatchEquals(left: BatchExpression, right: BatchExpression) extends BinaryBatchComparison {
  def symbol = "="
  override def eval(input: RowBatch): ColumnVector = {

    val leftdt = left.dataType
    val rightdt = right.dataType
    val resultType = BooleanType
    (leftdt, rightdt) match {
      case (l: NumericType, r: NumericType) =>
        val evalLeft = left.eval(input)
        val evalRight = right.eval(input)
        val leftMemGetter = Memory.getValue(l).asInstanceOf[(Long) => l.JvmType]
        val rightMemGetter = Memory.getValue(r).asInstanceOf[(Long) => r.JvmType]

        val memInLeft = evalLeft.content
        val memInRight = evalRight.content
        val leftWidth = evalLeft.typeWidth
        val rightWidth = evalRight.typeWidth
        val blOut = new BitSet(input.rowNum)

        //prepare bitmap for calculation
        val notNullArray1 = evalLeft.notNullArray
        val notNullArray2 = evalRight.notNullArray
        val notNullArrayResult = andWithNull(notNullArray1, notNullArray2, true)
        val selector = input.curSelector
        val bitmap = andWithNull(notNullArrayResult, selector, false)

        //iteratively calculate
        if (bitmap != null) {
          val iter = bitmap.iterator
          var i = 0
          while (iter.hasNext) {
            i = iter.next()
            val bl = (leftMemGetter(memInLeft.peer + i * leftWidth)
              == rightMemGetter(memInRight.peer + i * rightWidth))
            blOut.set(i, bl)
          }
        } else {
          val rowNum = input.rowNum
          var i = 0
          while (i < rowNum) {
            val bl = (leftMemGetter(memInLeft.peer + i * leftWidth)
              == rightMemGetter(memInRight.peer + i * rightWidth))
            blOut.set(i, bl)
            i += 1
          }
        }

        //free redundant memory
        if(evalLeft.isTemp) memInLeft.free()
        if(evalRight.isTemp) memInRight.free()

        //prepare result
        val rcv = input.getVector(resultType, true)
        rcv.setContent(new BooleanMemory(blOut))
        if (notNullArrayResult != null) {
          rcv.setNullable(notNullArrayResult)
        }
        rcv

      case (_ : NumericType, _ : NativeType) | (_ : NativeType, _ : NumericType) =>
        //always false, don't need to calculate
        val evalLeft = left.eval(input)
        val evalRight = right.eval(input)

        //prepare bitmap for calculation
        val notNullArray1 = evalLeft.notNullArray
        val notNullArray2 = evalRight.notNullArray
        val notNullArrayResult = andWithNull(notNullArray1, notNullArray2, true)

        //free redundant memory
        if(evalLeft.isTemp) evalLeft.content.free()
        if(evalRight.isTemp) evalRight.content.free()

        //prepare result
        val rcv = input.getVector(resultType, true)
        rcv.setContent(new BooleanMemory(new BitSet(input.rowNum)))
        if (notNullArrayResult != null) {
          rcv.setNullable(notNullArrayResult)
        }
        rcv

      case (BooleanType, BooleanType) =>
        val evalLeft = left.eval(input)
        val evalRight = right.eval(input)

        //prepare bitmap for calculation
        val notNullArray1 = evalLeft.notNullArray
        val notNullArray2 = evalRight.notNullArray
        val notNullArrayResult = andWithNull(notNullArray1, notNullArray2, true)
        val selector = input.curSelector
        val bitmap = andWithNull(notNullArrayResult, selector, false)

        //calculate
        val blLeft = evalLeft.content.asInstanceOf[BooleanMemory].bs
        val blRight = evalRight.content.asInstanceOf[BooleanMemory].bs

        val xnorResult = (blLeft ^ blRight).complement
        val blResult = andWithNull(bitmap, xnorResult, false)

        //prepare result
        val rcv = input.getVector(resultType, true)
        rcv.setContent(new BooleanMemory(blResult))
        if (notNullArrayResult != null) {
          rcv.setNullable(notNullArrayResult)
        }
        rcv

      case (StringType, StringType) =>
        sys.error(s"Type String does not support now")
      case (TimestampType, TimestampType) =>
        sys.error(s"Type timestamp does not support now")

    }
  }
}

case class BatchLessThan(left: BatchExpression, right: BatchExpression) extends BinaryBatchComparison {
  def symbol = "<"
  override def eval(input: RowBatch): EvaluatedType = c2b(input, left, right, _.lt(_, _))
}

case class BatchLessThanOrEqual(left: BatchExpression, right: BatchExpression) extends BinaryBatchComparison {
  def symbol = "<="
  override def eval(input: RowBatch): EvaluatedType = c2b(input, left, right, _.lteq(_, _))
}

case class BatchGreaterThan(left: BatchExpression, right: BatchExpression) extends BinaryBatchComparison {
  def symbol = ">"
  override def eval(input: RowBatch): EvaluatedType = c2b(input, left, right, _.gt(_, _))
}

case class BatchGreaterThanOrEqual(left: BatchExpression, right: BatchExpression) extends BinaryBatchComparison {
  def symbol = ">="
  override def eval(input: RowBatch): EvaluatedType = c2b(input, left, right, _.gteq(_, _))
}