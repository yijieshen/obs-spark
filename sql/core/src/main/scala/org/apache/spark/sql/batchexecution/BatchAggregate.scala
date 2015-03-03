package org.apache.spark.sql.batchexecution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.batchexpressions._
import org.apache.spark.sql.catalyst.batchexpressions.codegen._
import org.apache.spark.sql.catalyst.expressions._

import scala.collection.mutable.MutableList

case class BatchAggregate(
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkBatchPlan) extends UnaryBatchNode {

  val aggregatesToCompute = aggregateExpressions.flatMap { a =>
    a.collect { case agg: AggregateExpression => agg}
  }

  val projs: Seq[Expression] = groupingExpressions ++ getProjection(aggregatesToCompute)

  @transient lazy val projection = NewGenerateBatchMutableProjection(projs, child.output)

  val projectSchema: Seq[Attribute] = projs.map {
    case ar : AttributeReference => ar
    case e => Alias(e, s"projectionExpr:$e")().toAttribute
  }

  @transient lazy val bufferRefGetter = AggregateBufferRefGet(groupingExpressions, projectSchema)

  def batchExecute() = ???

  override def output = ???

  override def execute(): RDD[Row] = {

    if(partial) {
      if (groupingExpressions.isEmpty) {
        null
      } else {
        child.batchExecute().mapPartitions { iter =>
          val buffers = new java.util.HashMap[Row, MutableRow]()
          var currentRowBatch: RowBatch = null
          while (iter.hasNext) {
            currentRowBatch = iter.next()
            val projectedRowBatch = projection()(currentRowBatch)
            val buffersRef: AggregateBufferPrepare = bufferRefGetter()
              (projectedRowBatch, buffers, aggregatesToCompute.length)
          }





          null
        }
      }
    } else { //not partial
      null
    }
  }

  def getProjection(ae: Seq[AggregateExpression]): Seq[Expression] = {
    val resultProjections = new MutableList[Expression]
    var i = 0
    while ( i < ae.length) {
      val curAE = ae(i)
      curAE match {
        case Count(expr) => resultProjections += expr
        case Sum(expr) => resultProjections += expr
        case Max(expr) => resultProjections += expr
        case Min(expr) => resultProjections += expr
        case Average(expr) => resultProjections += expr
      }
      i += 1
    }
    resultProjections.toSeq
  }

}
