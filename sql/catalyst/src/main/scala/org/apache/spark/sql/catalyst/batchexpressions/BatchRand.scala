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

import java.util.Random
import org.apache.spark.sql.catalyst.types.DoubleType


case object BatchRand extends LeafBatchExpression {
  type EvaluatedType = ColumnVector

  override def dataType = DoubleType
  override def nullable = false
  override def references = Set.empty

  private[this] lazy val rand = new Random

  override def eval(input: RowBatch): EvaluatedType = {

    val outputCV = ColumnVector(DoubleType, input.curRowNum)
    val set = (outputCV.set _).asInstanceOf[(Int, Double) => Unit]

    val selector = input.curSelector

    if(selector != null) {
      selector.availableBits = input.curRowNum
      val iter = selector.iterator
      var i = 0
      while (iter.hasNext) {
        i = iter.next()
        set(i, rand.nextDouble())
      }
    } else {
      val rowNum = input.curRowNum
      var i = 0
      while (i < rowNum) {
        set(i, rand.nextDouble())
        i += 1
      }
    }
    outputCV
  }

  override def toString = "RAND()"
}
