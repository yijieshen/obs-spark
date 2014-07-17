package org.apache.spark.sql.catalyst.batchexpressions

import org.apache.spark.sql.catalyst.expressions.{ExprId, Expression, Attribute}
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.types.DataType

case class BatchBoundReference(ordinal: Int, baseReference: Attribute)
  extends Attribute with BatchExpression with trees.LeafNode[Expression] {

  override def withQualifiers(newQualifiers: Seq[String]): Attribute =
    BatchBoundReference(ordinal, baseReference.withQualifiers(newQualifiers))
  override def newInstance: Attribute =
    BatchBoundReference(ordinal, baseReference.newInstance)

  override type EvaluatedType = ColumnVector
  override def qualifiers: Seq[String] = baseReference.qualifiers
  override def exprId: ExprId = baseReference.exprId
  override def name: String = baseReference.name
  override def nullable: Boolean = baseReference.nullable
  override def dataType: DataType = baseReference.dataType

  override def eval(input: RowBatch): ColumnVector = input.name2Vector(name)
}
