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

    resultType match {
      case nt: NumericType =>
        //prepare function
        val get = (childCV.get _).asInstanceOf[(Int) => nt.JvmType]
        val castedF = f.asInstanceOf[(Numeric[nt.JvmType], nt.JvmType) => nt.JvmType]
        val outputCV = ColumnVector(nt, rb.curRowNum)
        val set = (outputCV.set _).asInstanceOf[(Int, nt.JvmType) => Unit]

        //prepare bitmap for calculation
        val selector = rb.curSelector
        val notNullArray = childCV.notNullArray
        val bitmap = andWithNull(selector, notNullArray, false)

        //iteratively calculate
        if (bitmap != null) {
          bitmap.availableBits = rb.curRowNum
          val iter = bitmap.iterator
          var i = 0
          while (iter.hasNext) {
            i = iter.next()
            set(i, castedF(nt.numeric, get(i)))
          }
        } else {
          val rowNum = rb.curRowNum
          var i = 0
          while (i < rowNum) {
            set(i, castedF(nt.numeric, get(i)))
            i += 1
          }
        }

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

    resultType match {
      case nt: NumericType =>
        //function prepare
        val leftGet = (leftCV.get _).asInstanceOf[(Int) => nt.JvmType]
        val rightGet = (rightCV.get _).asInstanceOf[(Int) => nt.JvmType]
        val castedF = f.asInstanceOf[(Numeric[nt.JvmType], nt.JvmType, nt.JvmType) => nt.JvmType]
        val outputCV = ColumnVector(nt, rb.curRowNum)
        val set = (outputCV.set _).asInstanceOf[(Int, nt.JvmType) => Unit]

        //prepare bitmap for calculation
        val notNullArray1 = leftCV.notNullArray
        val notNullArray2 = rightCV.notNullArray
        val notNullArrayResult = andWithNull(notNullArray1, notNullArray2, true)
        val selector = rb.curSelector
        val bitmap = andWithNull(notNullArrayResult, selector, false)

        //iteratively calculate
        if (bitmap != null) {
          bitmap.availableBits = rb.curRowNum
          val iter = bitmap.iterator
          var i = 0
          while (iter.hasNext) {
            i = iter.next()
            set(i, castedF(nt.numeric, leftGet(i), rightGet(i)))
          }
        } else {
          val rowNum = rb.curRowNum
          var i = 0
          while (i < rowNum) {
            set(i, castedF(nt.numeric, leftGet(i), rightGet(i)))
            i += 1
          }
        }

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

    resultType match {
      case ft: FractionalType =>
        //function prepare
        val leftGet = (leftCV.get _).asInstanceOf[(Int) => ft.JvmType]
        val rightGet = (rightCV.get _).asInstanceOf[(Int) => ft.JvmType]
        val castedF = f.asInstanceOf[(Fractional[ft.JvmType], ft.JvmType, ft.JvmType) => ft.JvmType]
        val outputCV = ColumnVector(ft, rb.curRowNum)
        val set = (outputCV.set _).asInstanceOf[(Int, ft.JvmType) => Unit]

        //prepare bitmap for calculation
        val notNullArray1 = leftCV.notNullArray
        val notNullArray2 = rightCV.notNullArray
        val notNullArrayResult = andWithNull(notNullArray1, notNullArray2, true)
        val selector = rb.curSelector
        val bitmap = andWithNull(notNullArrayResult, selector, false)

        //iteratively calculate
        if (bitmap != null) {
          bitmap.availableBits = rb.curRowNum
          val iter = bitmap.iterator
          var i = 0
          while (iter.hasNext) {
            i = iter.next()
            set(i, castedF(ft.fractional, leftGet(i), rightGet(i)))
          }
        } else {
          val rowNum = rb.curRowNum
          var i = 0
          while (i < rowNum) {
            set(i, castedF(ft.fractional, leftGet(i), rightGet(i)))
            i += 1
          }
        }

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

    resultType match {
      case it: IntegralType =>
        //function prepare
        val leftGet = (leftCV.get _).asInstanceOf[(Int) => it.JvmType]
        val rightGet = (rightCV.get _).asInstanceOf[(Int) => it.JvmType]
        val castedF = f.asInstanceOf[(Integral[it.JvmType], it.JvmType, it.JvmType) => it.JvmType]
        val outputCV = ColumnVector(it, rb.curRowNum)
        val set = (outputCV.set _).asInstanceOf[(Int, it.JvmType) => Unit]

        //prepare bitmap for calculation
        val notNullArray1 = leftCV.notNullArray
        val notNullArray2 = rightCV.notNullArray
        val notNullArrayResult = andWithNull(notNullArray1, notNullArray2, true)
        val selector = rb.curSelector
        val bitmap = andWithNull(notNullArrayResult, selector, false)

        //iteratively calculate
        if (bitmap != null) {
          bitmap.availableBits = rb.curRowNum
          val iter = bitmap.iterator
          var i = 0
          while (iter.hasNext) {
            i = iter.next()
            set(i, castedF(it.integral, leftGet(i), rightGet(i)))
          }
        } else {
          val rowNum = rb.curRowNum
          var i = 0
          while (i < rowNum) {
            set(i, castedF(it.integral, leftGet(i), rightGet(i)))
            i += 1
          }
        }

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
    val inputType = e1.dataType

    inputType match {
      case nt: NativeType =>
        //function prepare
        val leftGet = (leftCV.get _).asInstanceOf[(Int) => nt.JvmType]
        val rightGet = (rightCV.get _).asInstanceOf[(Int) => nt.JvmType]
        val castedF = f.asInstanceOf[(Ordering[nt.JvmType], nt.JvmType, nt.JvmType) => Boolean]

        //prepare input & output memory
        val blOut = new BitSet(rb.curRowNum)

        //prepare bitmap for calculation
        val notNullArray1 = leftCV.notNullArray
        val notNullArray2 = rightCV.notNullArray
        val notNullArrayResult = andWithNull(notNullArray1, notNullArray2, true)
        val selector = rb.curSelector
        val bitmap = andWithNull(notNullArrayResult, selector, false)

        //iteratively calculate
        if (bitmap != null) {
          bitmap.availableBits = rb.curRowNum
          val iter = bitmap.iterator
          var i = 0
          while (iter.hasNext) {
            i = iter.next()
            blOut.set(i, castedF(nt.ordering,leftGet(i),rightGet(i)))
          }
        } else {
          val rowNum = rb.curRowNum
          var i = 0
          while (i < rowNum) {
            blOut.set(i, castedF(nt.ordering,leftGet(i),rightGet(i)))
            i += 1
          }
        }

        //prepare result
        val outputCV = new BooleanColumnVector(rb.rowNum, blOut)
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
    } else if (bs2 != null && bs1 == null) {
      if(cp) bs2.copy else bs2
    } else {
      null
    }
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
