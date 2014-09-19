package org.apache.spark.sql.catalyst.batchexpressions

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.types.DataType

case class BatchBoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
  extends BatchExpression with trees.LeafNode[Expression] {

  override type EvaluatedType = ColumnVector

  override def references = Set.empty

  override def toString = s"input[$ordinal]"

  override def eval(input: RowBatch): ColumnVector = input.vectors(ordinal)

}
