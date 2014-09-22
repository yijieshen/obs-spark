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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}

class RowBatch(val rowNum: Int, val ordinal: Int) {

  //TODO: substitute name to vector mapping into ordinal to column List?
  // the columns come from table or as result tuple
  val vectors = new Array[ColumnVector](ordinal)

  // selector for the current rowbatch
  var curSelector: BitSet = null

  var curRowNum: Int = _
}

object RowBatch {

  def buildFromAttributes(attrs: Seq[Attribute], rowNum: Int): RowBatch = {
    val rowBatch = new RowBatch(rowNum, attrs.length)
    attrs.zipWithIndex.foreach { case (attr, i) =>
      val ar = attr.asInstanceOf[AttributeReference]
      val cv = ColumnVector(ar.dataType, rowNum)
      rowBatch.vectors(i) = cv
    }
    rowBatch
  }
}
