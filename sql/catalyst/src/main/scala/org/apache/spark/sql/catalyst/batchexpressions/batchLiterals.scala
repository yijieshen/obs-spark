/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
