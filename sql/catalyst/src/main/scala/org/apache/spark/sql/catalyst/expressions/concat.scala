package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.types._

case class Concat(children: Seq[Expression]) extends Expression {
  type EvaluatedType = Array[Byte]

  /** Returns the result of evaluating this expression on a given input Row */
  def eval(input: Row) = ???

  def references = ???

  def nullable = false

  def dataType = BinaryType
}
