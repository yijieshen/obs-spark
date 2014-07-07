package org.apache.spark.sql.batchexecution

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.batchexpressions.RowBatch
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.trees

@DeveloperApi
abstract class SparkBatchPlan extends QueryPlan[SparkBatchPlan] with Logging{
  self: Product =>

  /** Specifies how data is partitioned across different nodes in the cluster. */
  def outputPartitioning: Partitioning = UnknownPartitioning(0)
  /** Specifies any partition requirements on the input data for this operator. */
  def requiredChildDistribution: Seq[Distribution] =
    Seq.fill(children.size)(UnspecifiedDistribution)

  /**
   * Runs this query returning the result as an RDD.
   */
  def execute(): RDD[RowBatch]

  /**
   * Runs this query returning the result as an array.
   */
  def executeCollect(): Array[Row] = execute().map(_.expand).collect().flatten
}

private[sql] trait LeafBatchNode extends SparkBatchPlan with trees.LeafNode[SparkBatchPlan] {
  self: Product =>
}

private[sql] trait UnaryBatchNode extends SparkBatchPlan with trees.UnaryNode[SparkBatchPlan] {
  self: Product =>
  override def outputPartitioning: Partitioning = child.outputPartitioning
}

private[sql] trait BinaryBatchNode extends SparkBatchPlan with trees.BinaryNode[SparkBatchPlan] {
  self: Product =>
}