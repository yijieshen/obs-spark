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
    val memSetter = Memory.setValue(DoubleType).asInstanceOf[(Long, Double) => Unit]
    val typeWidth = 8

    //TODO: what's the right type of random output vector? currently it was set to Double
    val memOut = input.getTmpMemory(typeWidth)

    val selector = input.curSelector

    if(selector != null) {
      val iter = selector.iterator
      var i = 0
      while (iter.hasNext) {
        i = iter.next()
        memSetter(memOut.peer + i * typeWidth, rand.nextDouble())
      }
    } else {
      val rowNum = input.rowNum
      var i = 0
      while (i < rowNum) {
        memSetter(memOut.peer + i * typeWidth, rand.nextDouble())
        i += 1
      }
    }

    val rcv = input.getVector(DoubleType, true)
    rcv.setContent(memOut)
    rcv
  }

  override def toString = "RAND()"
}
