package org.apache.spark.sql.catalyst.batchexpressions

import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.{Expression, Row}
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.types._

abstract class BatchExpression extends Expression {
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
    val evalE = e.eval(rb)
    val width = evalE.typeWidth

    resultType match {
      case nt: NumericType =>
        //prepare function
        val memSetter = Memory.setValue(resultType).asInstanceOf[(Long, nt.JvmType) => Unit]
        val memGetter = Memory.getValue(resultType).asInstanceOf[(Long) => nt.JvmType]
        val castedF = f.asInstanceOf[(Numeric[nt.JvmType], nt.JvmType) => nt.JvmType]

        //prepare input & output memory
        val memIn = evalE.content
        val memOut = if (evalE.isTemp) evalE.content else rb.getTmpMemory(width)

        //prepare bitmap for calculation
        val selector = rb.curSelector
        val notNullArray = evalE.notNullArray

        val bitmap = andWithNull(selector, notNullArray, false)

        //iteratively calculate
        if (bitmap != null) {
          val iter = bitmap.iterator
          var i = 0
          while (iter.hasNext) {
            i = iter.next()
            val cr = castedF(nt.numeric, memGetter(memIn.peer + i * width))
            memSetter(memOut.peer + i * width, cr)
          }
        } else {
          val rowNum = evalE.rowNum
          var i = 0
          while (i < rowNum) {
            val cr = castedF(nt.numeric, memGetter(memIn.peer + i * width))
            memSetter(memOut.peer + i * width, cr)
            i += 1
          }
        }

        //free redundant memory

        //prepare result
        val rcv = rb.getVector(resultType, true)
        rcv.setContent(memOut)
        if (notNullArray != null) {
          rcv.setNullable(notNullArray.copy)
        }
        rcv
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
      f: ((Numeric[Any], Any, Any) => Any)): ColumnVector  = {

    if (e1.dataType != e2.dataType) {
      throw new TreeNodeException(this, s"Types do not match ${e1.dataType} != ${e2.dataType}")
    }

    val evalE1 = e1.eval(rb)
    val evalE2 = e2.eval(rb)
    val resultType = e1.dataType
    val width = evalE1.typeWidth

    resultType match {
      case nt: NumericType =>
        //function prepare
        val memSetter = Memory.setValue(resultType).asInstanceOf[(Long, nt.JvmType) => Unit]
        val memGetter = Memory.getValue(resultType).asInstanceOf[(Long) => nt.JvmType]
        val castedF = f.asInstanceOf[(Numeric[nt.JvmType], nt.JvmType, nt.JvmType) => nt.JvmType]

        //prepare input & output memory
        val (memIn1, memIn2, memOut, memToFree) = memoryPrepare(evalE1, evalE2, rb, width)

        //prepare bitmap for calculation
        val notNullArray1 = evalE1.notNullArray
        val notNullArray2 = evalE2.notNullArray
        val notNullArrayResult = andWithNull(notNullArray1, notNullArray2, true)
        val selector = rb.curSelector
        val bitmap = andWithNull(notNullArrayResult, selector, false)

        //iteratively calculate
        if (bitmap != null) {
          val iter = bitmap.iterator
          var i = 0
          while (iter.hasNext) {
            i = iter.next()
            val cr = castedF(nt.numeric,
              memGetter(memIn1.peer + i * width),
              memGetter(memIn2.peer + i * width))
            memSetter(memOut.peer + i * width, cr)
          }
        } else {
          val rowNum = rb.rowNum
          var i = 0
          while (i < rowNum) {
            val cr = castedF(nt.numeric,
              memGetter(memIn1.peer + i * width),
              memGetter(memIn2.peer + i * width))
            memSetter(memOut.peer + i * width, cr)
            i += 1
          }
        }

        //free redundant memory
        if(memToFree != null) memToFree.free()

        //prepare result
        val rcv = rb.getVector(resultType, true)
        rcv.setContent(memOut)
        if (notNullArrayResult != null) {
          rcv.setNullable(notNullArrayResult)
        }
        rcv

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

    val evalE1 = e1.eval(rb)
    val evalE2 = e2.eval(rb)
    val resultType = e1.dataType
    val width = evalE1.typeWidth

    resultType match {
      case ft: FractionalType =>
        //function prepare
        val memSetter = Memory.setValue(resultType).asInstanceOf[(Long, ft.JvmType) => Unit]
        val memGetter = Memory.getValue(resultType).asInstanceOf[(Long) => ft.JvmType]
        val castedF = f.asInstanceOf[(Fractional[ft.JvmType], ft.JvmType, ft.JvmType) => ft.JvmType]

        //prepare input & output memory
        val (memIn1, memIn2, memOut, memToFree) = memoryPrepare(evalE1, evalE2, rb, width)

        //prepare bitmap for calculation
        val notNullArray1 = evalE1.notNullArray
        val notNullArray2 = evalE2.notNullArray
        val notNullArrayResult = andWithNull(notNullArray1, notNullArray2, true)
        val selector = rb.curSelector
        val bitmap = andWithNull(notNullArrayResult, selector, false)

        //iteratively calculate
        if (bitmap != null) {
          val iter = bitmap.iterator
          var i = 0
          while (iter.hasNext) {
            i = iter.next()
            val cr = castedF(ft.fractional,
              memGetter(memIn1.peer + i * width),
              memGetter(memIn2.peer + i * width))
            memSetter(memOut.peer + i * width, cr)
          }
        } else {
          val rowNum = rb.rowNum
          var i = 0
          while (i < rowNum) {
            val cr = castedF(ft.fractional,
              memGetter(memIn1.peer + i * width),
              memGetter(memIn2.peer + i * width))
            memSetter(memOut.peer + i * width, cr)
            i += 1
          }
        }

        //free redundant memory
        if(memToFree != null) memToFree.free()

        //prepare result
        val rcv = rb.getVector(resultType, true)
        rcv.setContent(memOut)
        if (notNullArrayResult != null) {
          rcv.setNullable(notNullArrayResult)
        }
        rcv

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

    val evalE1 = e1.eval(rb)
    val evalE2 = e2.eval(rb)
    val resultType = e1.dataType
    val width = evalE1.typeWidth

    resultType match {
      case it: IntegralType =>
        //function prepare
        val memSetter = Memory.setValue(resultType).asInstanceOf[(Long, it.JvmType) => Unit]
        val memGetter = Memory.getValue(resultType).asInstanceOf[(Long) => it.JvmType]
        val castedF = f.asInstanceOf[(Integral[it.JvmType], it.JvmType, it.JvmType) => it.JvmType]

        //prepare input & output memory
        val (memIn1, memIn2, memOut, memToFree) = memoryPrepare(evalE1, evalE2, rb, width)

        //prepare bitmap for calculation
        val notNullArray1 = evalE1.notNullArray
        val notNullArray2 = evalE2.notNullArray
        val notNullArrayResult = andWithNull(notNullArray1, notNullArray2, true)
        val selector = rb.curSelector
        val bitmap = andWithNull(notNullArrayResult, selector, false)

        //iteratively calculate
        if (bitmap != null) {
          val iter = bitmap.iterator
          var i = 0
          while (iter.hasNext) {
            i = iter.next()
            val cr = castedF(it.integral,
              memGetter(memIn1.peer + i * width),
              memGetter(memIn2.peer + i * width))
            memSetter(memOut.peer + i * width, cr)
          }
        } else {
          val rowNum = rb.rowNum
          var i = 0
          while (i < rowNum) {
            val cr = castedF(it.integral,
              memGetter(memIn1.peer + i * width),
              memGetter(memIn2.peer + i * width))
            memSetter(memOut.peer + i * width, cr)
            i += 1
          }
        }

        //free redundant memory
        if(memToFree != null) memToFree.free()

        //prepare result
        val rcv = rb.getVector(resultType, true)
        rcv.setContent(memOut)
        if (notNullArrayResult != null) {
          rcv.setNullable(notNullArrayResult)
        }
        rcv

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

    val evalE1 = e1.eval(rb)
    val evalE2 = e2.eval(rb)
    val inputType = evalE1.dt
    val resultType = BooleanType
    val width = evalE1.typeWidth

    inputType match {
      case nt: NativeType =>
        //function prepare
        val memGetter = Memory.getValue(inputType).asInstanceOf[(Long) => nt.JvmType]
        val castedF = f.asInstanceOf[(Ordering[nt.JvmType], nt.JvmType, nt.JvmType) => Boolean]

        //prepare input & output memory
        val memIn1 = evalE1.content
        val memIn2 = evalE2.content
        val blOut = new BitSet(rb.rowNum)

        //prepare bitmap for calculation
        val notNullArray1 = evalE1.notNullArray
        val notNullArray2 = evalE2.notNullArray
        val notNullArrayResult = andWithNull(notNullArray1, notNullArray2, true)
        val selector = rb.curSelector
        val bitmap = andWithNull(notNullArrayResult, selector, false)

        //iteratively calculate
        if (bitmap != null) {
          val iter = bitmap.iterator
          var i = 0
          while (iter.hasNext) {
            i = iter.next()
            val cr = castedF(nt.ordering,
              memGetter(memIn1.peer + i * width),
              memGetter(memIn2.peer + i * width))
            blOut.set(i, cr)
          }
        } else {
          val rowNum = rb.rowNum
          var i = 0
          while (i < rowNum) {
            val cr = castedF(nt.ordering,
              memGetter(memIn1.peer + i * width),
              memGetter(memIn2.peer + i * width))
            blOut.set(i, cr)
            i += 1
          }
        }

        //free redundant memory
        if(evalE1.isTemp) memIn1.free()
        if(evalE2.isTemp) memIn2.free()

        //prepare result
        val rcv = rb.getVector(resultType, true)
        rcv.setContent(new BooleanMemory(blOut))
        if (notNullArrayResult != null) {
          rcv.setNullable(notNullArrayResult)
        }
        rcv

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
    val memOut = if (evalE1.isTemp) {
      memIn1
    } else if (evalE2.isTemp) {
      memIn2
    } else {
      rb.getTmpMemory(typeWidth)
    }
    val memToFree = if(evalE1.isTemp && evalE2.isTemp) memIn2 else null
    (memIn1, memIn2, memOut, memToFree)
  }
}

abstract class BinaryBatchExpression extends BatchExpression
    with trees.BinaryNode[BatchExpression] {

  self: Product =>

  def symbol: String

  override def foldable = left.foldable && right.foldable

  override def references = left.references ++ right.references

  override def toString = s"($left $symbol $right)"
}

abstract class LeafBatchExpression extends BatchExpression
    with trees.LeafNode[BatchExpression] {

  self: Product =>
}

abstract class UnaryBatchExpression extends BatchExpression
    with trees.UnaryNode[BatchExpression] {

  self: Product =>

  override def references = child.references
}
