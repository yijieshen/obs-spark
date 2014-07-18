package org.apache.spark.sql.catalyst.batchexpressions

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.types.DataType

case class BatchAlias(child: BatchExpression, name: String)
    (val exprId: ExprId = NamedExpression.newExprId, val qualifiers: Seq[String] = Nil)
  extends NamedExpression with BatchExpression with trees.UnaryNode[Expression] {

  override type EvaluatedType = ColumnVector

  override def eval(input: RowBatch): EvaluatedType = child.eval(input)

  override def nullable: Boolean = child.nullable
  override def dataType: DataType = child.dataType
  override def references: Set[Attribute] = child.references

  override def toAttribute = {
    if (resolved) {
      AttributeReference(name, child.dataType, child.nullable)(exprId, qualifiers)
    } else {
      UnresolvedAttribute(name)
    }
  }

  override def toString: String = s"$child AS $name#${exprId.id}$typeSuffix"

  override protected final def otherCopyArgs = exprId :: qualifiers :: Nil
}
