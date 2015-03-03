package org.apache.spark.sql.catalyst.batchexpressions.codegen

import org.apache.spark.sql.catalyst.batchexpressions.AggregateBufferPrepare
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, BindReferences, Attribute, Expression}

object AggregateBufferRefGet
  extends NewBatchCodeGenerator[Seq[Expression], () => AggregateBufferPrepare]{

  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  override protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer(_))

  override protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  protected def create(in: Seq[Expression]):(() => AggregateBufferPrepare) = {

    val inputRB = newTermName(s"inputRB")
    val selector = freshName("selector")
    val rowNum = freshName("curRowNum")
    val bmIter = freshName("bmIter")

    val genericMutableRowType = typeOf[GenericMutableRow]

    val cvget = in.zipWithIndex.map {
      case (e, i) =>
        val prepare = q"""
           val ${newTermName(s"cv$i")} = ${getCV(inputRB, i)}.asInstanceOf[${getCVType(e.dataType)}]
         """ : Tree
        val calculation =
          q"""
           ${newTermName(s"row")}($i) = ${newTermName(s"cv$i")}.${accessorForType(e.dataType)}(i)
           """ : Tree
        (prepare, calculation)
    }
    val cvPrepare: Seq[ru.Tree] = cvget.map(_._1)
    val cvCalculate: Seq[ru.Tree] = cvget.map(_._2)

    val code =
    q"""
      () => { new $abPrepareType {
          def apply(inputRB: $rowBatchType, ht: $hashMapType, aggSize: Int): $aggregateBufferRefType = {
            val rowArray = new Array[MutableRow](inputRB.rowNum)

            ..$cvPrepare

            //TODO: handle null elems in aggregate key

            val $selector = inputRB.curSelector

            val buffers = new Array[$genericMutableRowType](inputRB.curRowNum)

            if ($selector != null) {
              val $bmIter = $selector.iterator
              var i = 0
              while ($bmIter.hasNext) {
                i = $bmIter.next()
                val row : $genericMutableRowType = new $genericMutableRowType(${in.length})
                ..$cvCalculate
                var currentBuffer = ht.get(row)
                if (currentBuffer == null) {
                  currentBuffer = new $genericMutableRowType(aggSize)
                  ht.put(row, currentBuffer)
                }
                buffers(i) = currentBuffer
              }
            } else {
              $rowNum = inputRB.curRowNum
              var i = 0
              while (i < $rowNum) {
                val row : $genericMutableRowType = new $genericMutableRowType(${in.length})
                ..$cvCalculate
                var currentBuffer = ht.get(row)
                if (currentBuffer == null) {
                  currentBuffer = new $genericMutableRowType(aggSize)
                  ht.put(row, currentBuffer)
                }
                buffers(i) = currentBuffer
              }
            }
            new $aggregateBufferRefType(buffers)
          }
        }
      }
    """
    toolBox.eval(code).asInstanceOf[() => AggregateBufferPrepare]
  }


}
