package org.apache.spark.sql.catalyst

package object batchexpressions {

  abstract class RBProjection extends (RowBatch => RowBatch)

}
