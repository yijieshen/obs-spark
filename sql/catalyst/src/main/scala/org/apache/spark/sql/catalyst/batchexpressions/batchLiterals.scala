package org.apache.spark.sql.catalyst.batchexpressions

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.types._

case class BatchLiteral(value: Any, dataType: DataType) extends LeafBatchExpression {

  override def foldable = true
  def nullable = value == null
  def references = Set.empty

  override def toString = if (value != null) value.toString else "null"

  type EvaluatedType = ColumnVector

  val v = ColumnVector.getLiteral(value, dataType)

  override def eval(input: RowBatch): ColumnVector = v
}

object BatchLiteral {
  def apply(v: Any): BatchLiteral = v match {
    case i: Int => BatchLiteral(i, IntegerType)
    case l: Long => BatchLiteral(l, LongType)
    case d: Double => BatchLiteral(d, DoubleType)
    case f: Float => BatchLiteral(f, FloatType)
    case b: Byte => BatchLiteral(b, ByteType)
    case s: Short => BatchLiteral(s, ShortType)
    case s: String => BatchLiteral(s, StringType)
    case b: Boolean => BatchLiteral(b, BooleanType)
    case d: BigDecimal => BatchLiteral(d, DecimalType)
    case t: Timestamp => BatchLiteral(t, TimestampType)
    case a: Array[Byte] => BatchLiteral(a, BinaryType)
    case null => BatchLiteral(null, NullType)
  }
}
