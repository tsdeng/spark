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

package org.apache.spark.util.collection

import org.apache.spark.Logging
import org.apache.spark.SparkEnv

/**
 * Spills contents of an in-memory collection to disk when the memory threshold
 * has been exceeded.
 */
private[spark] trait Spillable[C] {

  this: Logging =>

  /**
   * Spills the current in-memory collection to disk, and releases the memory.
   *
   * @param collection collection to spill to disk
   */
//  protected def spill(collection: C): Unit


  def spillMySelf: Unit

  // Number of elements read from input since last spill
  protected var elementsRead: Long

  // Memory manager that can be used to acquire/release memory
  private[this] val shuffleMemoryManager = SparkEnv.get.shuffleMemoryManager

  // What threshold of elementsRead we start estimating collection size at
  private[this] val trackMemoryThreshold = 1000

  // How much of the shared memory pool this collection has claimed
  private[this] var myMemoryThreshold = 0L

  // Number of bytes spilled in total
  private[this] var _memoryBytesSpilled = 0L

  // Number of spills
  private[this] var _spillCount = 0

  /**
   * Spills the current in-memory collection to disk if needed. Attempts to acquire more
   * memory before spilling.
   *
   * @param collection collection to spill to disk
   * @param currentMemory estimated size of the collection in bytes
   * @return true if `collection` was spilled to disk; false otherwise
   */
  protected def maybeSpill(currentMemory: Long): Boolean = {
    if (elementsRead > trackMemoryThreshold && elementsRead % 32 == 0 &&
        currentMemory >= myMemoryThreshold) {
      logInfo(s"Thread ${Thread.currentThread().getId}, objID=${System.identityHashCode(this)} in maybeSpill: elementsRead:${elementsRead}, trackMemoryThreshold:${trackMemoryThreshold}, currentMemory:${currentMemory}, myMemoryThreshold:${myMemoryThreshold}")
      // Claim up to double our current memory from the shuffle memory pool
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
      val granted = shuffleMemoryManager.tryToAcquire(this, amountToRequest)
      logInfo(s"Thread ${Thread.currentThread().getId} requsting ${amountToRequest} memory, got ${granted}")
      myMemoryThreshold += granted
      logInfo(s"Thread ${Thread.currentThread().getId}'s memory threshold becomes ${myMemoryThreshold}")
      if (myMemoryThreshold <= currentMemory) {
        doSpill(currentMemory)
        return true
      }
    }
    false
  }

  /*
  Template pattern, subclass need to implement the spillMySelf method
   */
  def doSpill(currentMemory:Long) {
    // We were granted too little memory to grow further (either tryToAcquire returned 0,
    // or we already had more memory than myMemoryThreshold); spill the current collection
    spillMySelf
    logSpillage(currentMemory)
    finishSpill(currentMemory)//release memory from memory manager
  }

  def finishSpill(currentMemory: Long): Unit = {
    _spillCount += 1
    logInfo(s"Thread ${Thread.currentThread().getId} finished spilling")
    // Keep track of spills, and release memory
    _memoryBytesSpilled += currentMemory
    releaseMemoryForThisThread()
  }

  /**
   * @return number of bytes spilled in total
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled

  /**
   * Release our memory back to the shuffle pool so that other threads can grab it.
   */
  protected def releaseMemoryForThisThread(): Unit = {
    logInfo(s"Thread ${Thread.currentThread().getId} is releaseing memory for obj ${System.identityHashCode(this)} of size ${myMemoryThreshold}")
    shuffleMemoryManager.release(this, myMemoryThreshold)
    myMemoryThreshold = 0L
  }

  /**
   * Prints a standard log message detailing spillage.
   *
   * @param size number of bytes spilled
   */
  @inline private def logSpillage(size: Long) {
    val threadId = Thread.currentThread().getId
    logInfo("Thread %d spilling in-memory map of %d to disk (%d time%s so far)"
        .format(threadId, size , _spillCount, if (_spillCount > 1) "s" else ""))
  }
}
