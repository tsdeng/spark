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

package org.apache.spark.shuffle

import org.apache.spark.util.collection.Spillable

import scala.collection.mutable

import org.apache.spark.{Logging, SparkException, SparkConf}

/**
 * Allocates a pool of memory to task threads for use in shuffle operations. Each disk-spilling
 * collection (ExternalAppendOnlyMap or ExternalSorter) used by these tasks can acquire memory
 * from this pool and release it as it spills data out. When a task ends, all its memory will be
 * released by the Executor.
 *
 * This class tries to ensure that each thread gets a reasonable share of memory, instead of some
 * thread ramping up to a large amount first and then causing others to spill to disk repeatedly.
 * If there are N threads, it ensures that each thread can acquire at least 1 / 2N of the memory
 * before it has to spill, and at most 1 / N. Because N varies dynamically, we keep track of the
 * set of active threads and redo the calculations of 1 / 2N and 1 / N in waiting threads whenever
 * this set changes. This is all done by synchronizing access on "this" to mutate state and using
 * wait() and notifyAll() to signal changes.
 */
private[spark] class ShuffleMemoryManager(maxMemory: Long) extends Logging {
  //The assumption is each thread can have only one occupant at a time, this restriciton can be losen up actually!!TODO
  private val threadMemory = new mutable.HashMap[Long,(Spillable[_], Long)]()  // threadId -> (object, that's holding the memory, memory bytes)

  def this(conf: SparkConf) = this(ShuffleMemoryManager.getMaxMemory(conf))

  /**
   * Try to acquire up to numBytes memory for the current thread, and return the number of bytes
   * obtained, or 0 if none can be allocated. This call may block until there is enough free memory
   * in some situations, to make sure each thread has a chance to ramp up to at least 1 / 2N of the
   * total memory pool (where N is the # of active threads) before it is forced to spill. This can
   * happen if the number of threads increases but an older thread had a lot of memory already.
   */
  def tryToAcquire(asker:Spillable[_], numBytes: Long, expelPrevious: Boolean = true): Long = synchronized {//TODO: remove the expelPrevious flag
    val threadId = Thread.currentThread().getId
    logInfo(s"Thread ${threadId} is trying to acquire ${numBytes}")
    logInfo(s"current memory allocation ${threadMemory.map{case (k, (v1,v2)) => (k, v2)}}")
    assert(numBytes > 0, "invalid number of bytes requested: " + numBytes)

    // may need to kickout current occupant
    if (threadMemory.contains(threadId)) {
      val (currentOccupent, currentMem) = threadMemory(threadId)
      if (currentOccupent != asker && expelPrevious) {
        logInfo(s"Thread ${threadId} is freeing up ${System.identityHashCode(currentOccupent)} to make space for ${System.identityHashCode(asker)}")
        currentOccupent.doSpill(currentMem) // Notice, after spilling the threadMemory may not contain the threadID anymore, that's why following is doing a null check...TODO: to be refactored
      }
    }
    // Add this thread to the threadMemory map just so we can keep an accurate count of the number
    // of active threads, to let other threads ramp down their memory in calls to tryToAcquire
    if (!threadMemory.contains(threadId)) {
      threadMemory(threadId) = (asker, 0L)
      notifyAll()  // Will later cause waiting threads to wake up and check numThreads again
    }



    // Keep looping until we're either sure that we don't want to grant this request (because this
    // thread would have more than 1 / numActiveThreads of the memory) or we have enough free
    // memory to give it (we always let each thread get at least 1 / (2 * numActiveThreads)).
    while (true) {
      val numActiveThreads = threadMemory.keys.size
      val curMem = threadMemory(threadId)._2

      val freeMemory:Long = maxMemory - threadMemory.values.map(_._2).sum

      // How much we can grant this thread; don't let it grow to more than 1 / numActiveThreads
      val maxToGrant = math.min(numBytes, (maxMemory / numActiveThreads) - curMem)

      if (curMem < maxMemory / (2 * numActiveThreads)) {
        // We want to let each thread get at least 1 / (2 * numActiveThreads) before blocking;
        // if we can't give it this much now, wait for other threads to free up memory
        // (this happens if older threads allocated lots of memory before N grew)
        if (freeMemory >= math.min(maxToGrant, maxMemory / (2 * numActiveThreads) - curMem)) {
          val toGrant = math.min(maxToGrant, freeMemory)
          val (occupant, mem) = threadMemory(threadId)
          threadMemory(threadId) = (occupant, mem + toGrant)
          return toGrant
        } else {
          logInfo(s"Thread $threadId waiting for at least 1/2N of shuffle memory pool to be free")
          wait()
        }
      } else {
        // Only give it as much memory as is free, which might be none if it reached 1 / numThreads
        val toGrant = math.min(maxToGrant, freeMemory)
        val (occupant, mem) = threadMemory(threadId)
        threadMemory(threadId) = (occupant, mem + toGrant)
        return toGrant
      }
    }
    0L  // Never reached
  }

  /** Release numBytes bytes for the current thread. */
  def release(asker:Spillable[_], numBytes: Long): Unit = synchronized {
    val threadId = Thread.currentThread().getId
    val (occupant, mem) = threadMemory(threadId)
    if (asker != occupant) {
      throw new RuntimeException(s"can not release ${System.identityHashCode(asker)}, since the current occupant is ${System.identityHashCode(occupant)}")
    }
    if (mem < numBytes) {
      throw new SparkException(
        s"Internal error: release called on ${numBytes} bytes but thread only has ${mem}")
    }
    if (mem == numBytes) {
      threadMemory.remove(threadId)
    } else {
      threadMemory(threadId) = (occupant, mem - numBytes)
    }
    notifyAll()  // Notify waiters who locked "this" in tryToAcquire that memory has been freed
  }

  /** Release all memory for the current thread and mark it as inactive (e.g. when a task ends). */
  def releaseMemoryForThisThread(): Unit = synchronized {
    val threadId = Thread.currentThread().getId
    logInfo(s"Thread $threadId is freed!")
    threadMemory.remove(threadId)
    notifyAll()  // Notify waiters who locked "this" in tryToAcquire that memory has been freed
  }
}

private object ShuffleMemoryManager {
  /**
   * Figure out the shuffle memory limit from a SparkConf. We currently have both a fraction
   * of the memory pool and a safety factor since collections can sometimes grow bigger than
   * the size we target before we estimate their sizes again.
   */
  def getMaxMemory(conf: SparkConf): Long = {
    val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)
    val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", 0.8)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }
}
