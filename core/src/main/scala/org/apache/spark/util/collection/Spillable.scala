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
   * Spills the current in-memory collection to disk
   */
  def spill: Unit

  // Number of elements read from input since last spill
  private[spark] var elementsRead: Long

  // Memory manager that can be used to acquire/release memory
  private[this] val shuffleMemoryManager = SparkEnv.get.shuffleMemoryManager

  // What threshold of elementsRead we start estimating collection size at
  private[this] val _trackMemoryThreshold = 1000

  private[spark] def trackMemoryThreshold = _trackMemoryThreshold

  // How much of the shared memory pool this collection has claimed
  private[spark] var allocatedMemory = 0L

  // Number of bytes spilled in total
  private[this] var _memoryBytesSpilled = 0L

  // Number of spills
  private[this] var _spillCount = 0

  /**
   * Spills the current in-memory collection to disk if needed. Attempts to acquire more
   * memory before spilling.
   *
   * @return true if `collection` was spilled to disk; false otherwise
   */
  protected def maybeSpill(): Boolean = {
    shuffleMemoryManager.getThreadMemoryManager.maybeSpill(this)
  }

  /**
   * The size of memory used. [[org.apache.spark.shuffle.ShuffleMemoryManager]] uses it to decide
   * if to spill current obj
   * @return
   */
  def memorySize: Long

  private def increaseSpillCount() = { _spillCount += 1 }

  /**
   * @return number of bytes spilled in total
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled

  /**
   * Prints a standard log message detailing spillage.
   *
   * @param size number of bytes spilled
   */
  def logSpillage(size: Long) {
    increaseSpillCount()
    val threadId = Thread.currentThread().getId
    logInfo("Thread %d spilling in-memory map of %d to disk (%d time%s so far)"
        .format(threadId, size , _spillCount, if (_spillCount > 1) "s" else ""))
  }
}
