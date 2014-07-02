package org.apache.spark.sql.catalyst.batchexpressions

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.types.NumericType

abstract class BatchExpression extends Expression {
  self: Product =>

  /** The narrowest possible type that is produced when this expression is evaluated. */
  type EvaluatedType <: ColumnVector

  def eval(input: RowBatch = null): EvaluatedType

  /**
   * A set of helper functions that return the correct descendant of `scala.math.Numeric[T]` type
   * and do any casting necessary of child evaluation.
   */
  @inline
  def n1(e: BatchExpression, rb: RowBatch, f: ((Numeric[Any], Any) => Any)): ColumnVector = {
    val resultType = e.dataType
    val cr = e.eval(rb)
    val width = cr.typeWidth

    resultType match {
      case nt: NumericType =>
        val memSetter = Memory.setValue(resultType).asInstanceOf[(Long, nt.JvmType) => Unit]
        val memGetter = Memory.getValue(resultType).asInstanceOf[(Long) => nt.JvmType]

        val castedFunction = f.asInstanceOf[(Numeric[nt.JvmType], nt.JvmType) => nt.JvmType]

        val memIn = cr.content
        val memOut = if (cr.isTemp) cr.content else rb.getTmpMemory(cr.typeWidth)
        val bitmap = if (cr.nullable && cr.withSelector) {
          cr.notNullArray & cr.selector
        } else if (cr.nullable && !cr.withSelector) {
          cr.notNullArray
        } else if (!cr.nullable && cr.withSelector) {
          cr.selector
        } else {
          //!cr.nullable && !cr.withSelector
          null
        }

        if (bitmap != null) {
          val iter = bitmap.iterator
          var i = 0
          while (iter.hasNext) {
            i = iter.next()
            val cr: nt.JvmType = castedFunction(nt.numeric,
              memGetter(memIn.peer + i * width))
            memSetter(memOut.peer + i * width, cr)
          }
        } else {
          val rowNum = cr.rowNum
          var i = 0
          while (i < rowNum) {
            val cr: nt.JvmType = castedFunction(nt.numeric,
              memGetter(memIn.peer + i * width))
            memSetter(memOut.peer + i * width, cr)
            i += 1
          }
        }

        //prepare result
        val rcv = if (cr.isTemp) cr else rb.getTmpVector(resultType)
        rcv.setContent(memOut)
        if (bitmap != null) {
          rcv.setNullable(bitmap)
          rcv.setSelector(bitmap)
        }
        rcv
      case other => throw new
          UnsupportedOperationException(s"Type $other does not support numeric operations")
    }
  }



}
