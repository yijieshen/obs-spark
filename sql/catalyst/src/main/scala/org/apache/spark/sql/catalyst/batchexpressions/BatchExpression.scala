package org.apache.spark.sql.catalyst.batchexpressions

import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._

trait BatchExpression extends Expression {
  self: Product =>

  /** The narrowest possible type that is produced when this expression is evaluated. */
  type EvaluatedType <: ColumnVector

  override final def eval(input: Row = null) =
    throw new UnsupportedOperationException("Batch Expressions don't use this")

  def eval(input: RowBatch): EvaluatedType

  /**
   * A set of helper functions that return the correct descendant of `scala.math.Numeric[T]` type
   * and do any casting necessary of child evaluation.
   */
  @inline
  def n1b(e: BatchExpression, rb: RowBatch, f: ((Numeric[Any], Any) => Any)): ColumnVector = {
    val resultType = e.dataType
    val childCV = e.eval(rb)
    val width = childCV.typeWidth

    resultType match {
      case nt: NumericType =>
        //prepare function
        val get = (childCV.get _).asInstanceOf[(Int) => nt.JvmType]
        val castedF = f.asInstanceOf[(Numeric[nt.JvmType], nt.JvmType) => nt.JvmType]

        //prepare output memory
        val memIn = childCV.content
        val memOut = if (childCV.isTemp) childCV.content else rb.getTmpMemory(width)
        val outputCV = ColumnVector.getOffHeapCV(nt, memOut.asInstanceOf[OffHeapMemory], true)
        val set = (outputCV.set _).asInstanceOf[(Int, nt.JvmType) => Unit]

        //prepare bitmap for calculation
        val selector = rb.curSelector
        val notNullArray = childCV.notNullArray

        val bitmap = andWithNull(selector, notNullArray, false)

        //iteratively calculate
        if (bitmap != null) {
          val iter = bitmap.iterator
          var i = 0
          while (iter.hasNext) {
            i = iter.next()
            set(i, castedF(nt.numeric, get(i)))
          }
        } else {
          val rowNum = rb.rowNum
          var i = 0
          while (i < rowNum) {
            set(i, castedF(nt.numeric, get(i)))
            i += 1
          }
        }

        //free redundant memory

        //prepare result
        if (notNullArray != null) {
          outputCV.notNullArray = notNullArray
        }
        outputCV
      case other => sys.error(s"Type $other does not support numeric operations")
    }
  }

  /**
   * Evaluation helper function for 2 Numeric children expressions. Those expressions are supposed
   * to be in the same data type, and also the return type.
   * Either one of the expressions result is null, the evaluation result should be null.
   */
  @inline
  protected final def n2b(
      rb: RowBatch,
      e1: BatchExpression,
      e2: BatchExpression,
      f: ((Numeric[Any], Any, Any) => Any)): ColumnVector = {

    if (e1.dataType != e2.dataType) {
      throw new TreeNodeException(this, s"Types do not match ${e1.dataType} != ${e2.dataType}")
    }

    val leftCV = e1.eval(rb)
    val rightCV = e2.eval(rb)
    val resultType = e1.dataType
    val width = leftCV.typeWidth

    resultType match {
      case nt: NumericType =>
        //function prepare
        val leftGet = (leftCV.get _).asInstanceOf[(Int) => nt.JvmType]
        val rightGet = (rightCV.get _).asInstanceOf[(Int) => nt.JvmType]
        val castedF = f.asInstanceOf[(Numeric[nt.JvmType], nt.JvmType, nt.JvmType) => nt.JvmType]

        //prepare output memory
        val (memOut, memToFree) = memoryPrepare(leftCV, rightCV, rb, width)
        val outputCV = ColumnVector.getOffHeapCV(nt, memOut.asInstanceOf[OffHeapMemory], true)
        val set = (outputCV.set _).asInstanceOf[(Int, nt.JvmType) => Unit]

        //prepare bitmap for calculation
        val notNullArray1 = leftCV.notNullArray
        val notNullArray2 = rightCV.notNullArray
        val notNullArrayResult = andWithNull(notNullArray1, notNullArray2, true)
        val selector = rb.curSelector
        val bitmap = andWithNull(notNullArrayResult, selector, false)

        //iteratively calculate
        if (bitmap != null) {
          val iter = bitmap.iterator
          var i = 0
          while (iter.hasNext) {
            i = iter.next()
            set(i, castedF(nt.numeric, leftGet(i), rightGet(i)))
          }
        } else {
          val rowNum = rb.rowNum
          var i = 0
          while (i < rowNum) {
            set(i, castedF(nt.numeric, leftGet(i), rightGet(i)))
            i += 1
          }
        }

        //free redundant memory
        if(memToFree != null) rb.returnMemory(width, memToFree.asInstanceOf[OffHeapMemory])

        //prepare result
        if (notNullArrayResult != null) {
          outputCV.notNullArray = notNullArrayResult
        }
        outputCV

      case other => sys.error(s"Type $other does not support numeric operations")
    }
  }

  /**
   * Evaluation helper function for 2 Fractional children expressions. Those expressions are
   * supposed to be in the same data type, and also the return type.
   * Either one of the expressions result is null, the evaluation result should be null.
   */
  @inline
  protected final def f2b(
       rb: RowBatch,
       e1: BatchExpression,
       e2: BatchExpression,
       f: ((Fractional[Any], Any, Any) => Any)): ColumnVector  = {

    if (e1.dataType != e2.dataType) {
      throw new TreeNodeException(this, s"Types do not match ${e1.dataType} != ${e2.dataType}")
    }

    val leftCV = e1.eval(rb)
    val rightCV = e2.eval(rb)
    val resultType = e1.dataType
    val width = leftCV.typeWidth

    resultType match {
      case ft: FractionalType =>
        //function prepare
        val leftGet = (leftCV.get _).asInstanceOf[(Int) => ft.JvmType]
        val rightGet = (rightCV.get _).asInstanceOf[(Int) => ft.JvmType]
        val castedF = f.asInstanceOf[(Fractional[ft.JvmType], ft.JvmType, ft.JvmType) => ft.JvmType]

        //prepare output vector
        val (memOut, memToFree) = memoryPrepare(leftCV, rightCV, rb, width)
        val outputCV = ColumnVector.getOffHeapCV(ft, memOut.asInstanceOf[OffHeapMemory], true)
        val set = (outputCV.set _).asInstanceOf[(Int, ft.JvmType) => Unit]


        //prepare bitmap for calculation
        val notNullArray1 = leftCV.notNullArray
        val notNullArray2 = rightCV.notNullArray
        val notNullArrayResult = andWithNull(notNullArray1, notNullArray2, true)
        val selector = rb.curSelector
        val bitmap = andWithNull(notNullArrayResult, selector, false)

        //iteratively calculate
        if (bitmap != null) {
          val iter = bitmap.iterator
          var i = 0
          while (iter.hasNext) {
            i = iter.next()
            set(i, castedF(ft.fractional, leftGet(i), rightGet(i)))
          }
        } else {
          val rowNum = rb.rowNum
          var i = 0
          while (i < rowNum) {
            set(i, castedF(ft.fractional, leftGet(i), rightGet(i)))
            i += 1
          }
        }

        //free redundant memory
        if(memToFree != null) rb.returnMemory(width, memToFree.asInstanceOf[OffHeapMemory])

        //prepare result
        if (notNullArrayResult != null) {
          outputCV.notNullArray = notNullArrayResult
        }
        outputCV

      case other => sys.error(s"Type $other does not support numeric operations")
    }
  }

  /**
   * Evaluation helper function for 2 Integral children expressions. Those expressions are
   * supposed to be in the same data type, and also the return type.
   * Either one of the expressions result is null, the evaluation result should be null.
   */
  @inline
  protected final def i2b(
      rb: RowBatch,
      e1: BatchExpression,
      e2: BatchExpression,
      f: ((Integral[Any], Any, Any) => Any)): ColumnVector  = {

    if (e1.dataType != e2.dataType) {
      throw new TreeNodeException(this, s"Types do not match ${e1.dataType} != ${e2.dataType}")
    }

    val leftCV = e1.eval(rb)
    val rightCV = e2.eval(rb)
    val resultType = e1.dataType
    val width = leftCV.typeWidth

    resultType match {
      case it: IntegralType =>
        //function prepare
        val leftGet = (leftCV.get _).asInstanceOf[(Int) => it.JvmType]
        val rightGet = (rightCV.get _).asInstanceOf[(Int) => it.JvmType]
        val castedF = f.asInstanceOf[(Integral[it.JvmType], it.JvmType, it.JvmType) => it.JvmType]

        //prepare input & output memory
        val (memOut, memToFree) = memoryPrepare(leftCV, rightCV, rb, width)
        val outputCV = ColumnVector.getOffHeapCV(it, memOut.asInstanceOf[OffHeapMemory], true)
        val set = (outputCV.set _).asInstanceOf[(Int, it.JvmType) => Unit]

        //prepare bitmap for calculation
        val notNullArray1 = leftCV.notNullArray
        val notNullArray2 = rightCV.notNullArray
        val notNullArrayResult = andWithNull(notNullArray1, notNullArray2, true)
        val selector = rb.curSelector
        val bitmap = andWithNull(notNullArrayResult, selector, false)

        //iteratively calculate
        if (bitmap != null) {
          val iter = bitmap.iterator
          var i = 0
          while (iter.hasNext) {
            i = iter.next()
            set(i, castedF(it.integral, leftGet(i), rightGet(i)))
          }
        } else {
          val rowNum = rb.rowNum
          var i = 0
          while (i < rowNum) {
            set(i, castedF(it.integral, leftGet(i), rightGet(i)))
            i += 1
          }
        }

        //free redundant memory
        if(memToFree != null)
          rb.returnMemory(width, memToFree.asInstanceOf[OffHeapMemory])

        //prepare result
        if (notNullArrayResult != null) {
          outputCV.notNullArray = notNullArrayResult
        }
        outputCV

      case other => sys.error(s"Type $other does not support numeric operations")
    }
  }

  /**
   * Evaluation helper function for 2 Comparable children expressions. Those expressions are
   * supposed to be in the same data type, and the return type should be Integer:
   * Negative value: 1st argument less than 2nd argument
   * Zero:  1st argument equals 2nd argument
   * Positive value: 1st argument greater than 2nd argument
   *
   * Either one of the expressions result is null, the evaluation result should be null.
   */
  @inline
  protected final def c2b(
      rb: RowBatch,
      e1: BatchExpression,
      e2: BatchExpression,
      f: ((Ordering[Any], Any, Any) => Boolean)): ColumnVector  = {

    //TODO: Boolean/String/TimeStamp as inputType not supported now
    if (e1.dataType != e2.dataType) {
      throw new TreeNodeException(this, s"Types do not match ${e1.dataType} != ${e2.dataType}")
    }

    val leftCV = e1.eval(rb)
    val rightCV = e2.eval(rb)
    val inputType = leftCV.dt
    val resultType = BooleanType
    val width = leftCV.typeWidth

    inputType match {
      case nt: NativeType =>
        //function prepare
        val leftGet = (leftCV.get _).asInstanceOf[(Int) => nt.JvmType]
        val rightGet = (rightCV.get _).asInstanceOf[(Int) => nt.JvmType]
        val castedF = f.asInstanceOf[(Ordering[nt.JvmType], nt.JvmType, nt.JvmType) => Boolean]

        //prepare input & output memory
        val blOut = new BitSet(rb.rowNum)

        //prepare bitmap for calculation
        val notNullArray1 = leftCV.notNullArray
        val notNullArray2 = rightCV.notNullArray
        val notNullArrayResult = andWithNull(notNullArray1, notNullArray2, true)
        val selector = rb.curSelector
        val bitmap = andWithNull(notNullArrayResult, selector, false)

        //iteratively calculate
        if (bitmap != null) {
          val iter = bitmap.iterator
          var i = 0
          while (iter.hasNext) {
            i = iter.next()
            blOut.set(i, castedF(nt.ordering,leftGet(i),rightGet(i)))
          }
        } else {
          val rowNum = rb.rowNum
          var i = 0
          while (i < rowNum) {
            blOut.set(i, castedF(nt.ordering,leftGet(i),rightGet(i)))
            i += 1
          }
        }

        //free redundant memory
        if(leftCV.isTemp && !leftCV.isInstanceOf[FakeColumnVector])
          rb.returnMemory(width, leftCV.content.asInstanceOf[OffHeapMemory])
        if(rightCV.isTemp && !rightCV.isInstanceOf[FakeColumnVector])
          rb.returnMemory(width, rightCV.content.asInstanceOf[OffHeapMemory])

        //prepare result

        val outputMem = new BooleanMemory(blOut, rb.rowNum)
        val outputCV = new BooleanColumnVector(outputMem, false)
        if (notNullArrayResult != null) {
          outputCV.notNullArray = notNullArrayResult
        }
        outputCV

      case other => sys.error(s"Type $other does not support numeric operations")
    }
  }

  /**
   *
   * @param bs1
   * @param bs2
   * @param cp should return a new BitSet ?
   * @return
   */
  @inline
  protected final def andWithNull(bs1: BitSet, bs2: BitSet, cp: Boolean): BitSet = {
    if (bs1 != null && bs2 != null) {
      bs1 & bs2
    } else if (bs1 != null && bs2 == null) {
      if(cp) bs1.copy else bs1
    } else if (bs1 != null && bs2 == null) {
      if(cp) bs2.copy else bs2
    } else {
      null
    }
  }

  @inline
  protected final def memoryPrepare(
      evalE1: ColumnVector, evalE2: ColumnVector, rb: RowBatch, typeWidth: Int) = {
    val memIn1 = evalE1.content
    val memIn2 = evalE2.content
    val memOut = if (evalE1.isTemp && !evalE1.isInstanceOf[FakeColumnVector]) {
      memIn1
    } else if (evalE2.isTemp && !evalE2.isInstanceOf[FakeColumnVector]) {
      memIn2
    } else {
      rb.getTmpMemory(typeWidth)
    }
    val memToFree = if(evalE1.isTemp && evalE2.isTemp
      && !evalE1.isInstanceOf[FakeColumnVector] && !evalE2.isInstanceOf[FakeColumnVector])
      memIn2 else null
    (memOut, memToFree)
  }
}

abstract class BinaryBatchExpression extends BinaryExpression with BatchExpression {

  self: Product =>

  def symbol: String

  override def foldable = left.foldable && right.foldable

  override def references = left.references ++ right.references

  override def toString = s"($left $symbol $right)"
}

abstract class LeafBatchExpression extends LeafExpression with BatchExpression {

  self: Product =>
}

abstract class UnaryBatchExpression extends UnaryExpression with BatchExpression {

  self: Product =>

  override def references = child.references
}
