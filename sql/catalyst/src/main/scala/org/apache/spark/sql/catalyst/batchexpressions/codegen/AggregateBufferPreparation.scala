package org.apache.spark.sql.catalyst.batchexpressions.codegen

import org.apache.spark.sql.catalyst.batchexpressions._
import org.apache.spark.sql.catalyst.expressions.Attribute

case class AggregateBufferPreparation(bufferLength: Int)
  extends NewBatchCodeGenerator[RowBatch, () => AggregateBufferPrepare] {

  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  protected def canonicalize(rb: RowBatch) = ???
  protected def bind(rb: RowBatch, inputSchema: Seq[Attribute]) = ???

  protected def create(rb: RowBatch) = {
    val bCode = getBufferCode()
    val code =
      q"""
        () => { new $abPrepareType {
            private[this] var $outputBinaryCV: $binaryColumnVectorType = null
            def apply(inputRB: $rowBatchType, ht: $byteArrayMapType): $binaryColumnVectorType = {
              if ($outputBinaryCV == null) {
                $outputBinaryCV = new $binaryColumnVectorType(inputRB.curRowNum)
              }
              ..$bCode
              outputBCV
            }
          }
        }
       """
    toolBox.eval(code).asInstanceOf[() => AggregateBufferPrepare]
  }

  override def apply(rb: RowBatch) = cache.get(rb)

  def getBufferCode() = {
    val keyCVName = freshName("keyCV")
    val notNullArrayTerm = freshName("notNullArrayTerm")
    val selector = freshName("selector")
    val bitmap = freshName("bitmap")
    val bmIter = freshName("bmIter")
    val rowNum = freshName("curRowNum")
    q"""
      val $keyCVName = input.vectors(0)
      val $notNullArrayTerm = $keyCVName.notNullArray
      val $selector = input.curSelector
      val $bitmap = ${andWithNull(notNullArrayTerm, selector, false)}

      if ($bitmap != null) {
        $bitmap.availableBits = input.curRowNum
        val $bmIter = $bitmap.iterator
        var i = 0
        while ($bmIter.hasNext) {
          i = $bmIter.next()
          $outputBinaryCV.set(i, ht.getValue($keyCVName.get(i), $bufferLength))
        }
      } else {
        val $rowNum = input.curRowNum
        var i = 0
        while (i < $rowNum) {
          $outputBinaryCV.set(i, ht.getValue($keyCVName.get(i), $bufferLength))
          i += 1
        }
      }
    """.children
  }

}
