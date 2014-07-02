package org.apache.spark.sql.catalyst.batchexpressions

import scala.collection.mutable.Stack

/**
 *
 * @param rowNum
 */
class MemoryPool(val rowNum: Int) {

  // one extra slot to make direct (columnType's size -> index) map
  private val freeList = new Array[Stack[Memory]](MemoryPool.MAX_COLUMN_WIDTH + 1)

  freeList(8) = new Stack[Memory]
  freeList(4) = new Stack[Memory]
  freeList(2) = new Stack[Memory]
  freeList(1) = new Stack[Memory]

  private def _allocateNew(width: Int): Memory = {
    val mem = Memory.allocate(width * rowNum)
    mem
  }

  def borrowMemory(width: Int): Memory = {
    val curStack = freeList(width)
    if (curStack.isEmpty) {
      _allocateNew(width)
    }
    else {
      curStack.pop()
    }
  }

  def returnMemory(width: Int, mem: Memory) = {
    freeList(width).push(mem)
  }

  /**
   *
   */
  def free() = {
    freeList(8).foreach(_.free())
    freeList(8) = null
    freeList(4).foreach(_.free())
    freeList(4) = null
    freeList(2).foreach(_.free())
    freeList(2) = null
    freeList(1).foreach(_.free())
    freeList(1) = null
    // var i = 0 while (i < 16) { if (freeList(i) != null)
    // { freeList(i).foreach(_.free()) freeList(i) = null } i += 1 }
  }
}

object MemoryPool {
  val MAX_COLUMN_WIDTH = 8
}
