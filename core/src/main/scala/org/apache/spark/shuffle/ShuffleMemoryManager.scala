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

import scala.collection.mutable.ListBuffer


class SpillableTaskMemoryManager(val shuffleMemoryManager: ShuffleMemoryManager) extends Logging {

  var allocatedMemory = 0L

  private val objs : ListBuffer[Spillable[_]] = ListBuffer()
  /**
   * After receiving a memory request, ThreadMemoryManager will first ask shuffleMemoryManager for the requested memory
   * If shuffleMemoryManager can not give back enough memory, then ThreadMemoryManager will try to spill objects
   * until all objs are spilled or the memory released satisfies the memory request
   * @param requestedAmount memory to request
   * @return actual memory granted
   */
  def requestMemory(requester:Spillable[_], requestedAmount: Long):Long = {
    //try to ask memory for the thread first
    val newMemForThread = shuffleMemoryManager.tryToAcquire(this, requestedAmount)
    allocatedMemory += newMemForThread

    if (newMemForThread >= requestedAmount) return newMemForThread

    //try to spill objs in current thread to make space for new request
    var addedMemory = newMemForThread
    objs -= requester
    while(addedMemory < requestedAmount && !objs.isEmpty ) {
       val toSpill = objs.remove(0)
       addedMemory += spill(toSpill)
    }

    if (addedMemory > requestedAmount) {
      releaseMemoryForThread(addedMemory - requestedAmount)
      addedMemory = requestedAmount
    }
    objs += requester // put requester to the end of the queue
    return addedMemory
  }


  /**
   * This gives back memory to the ShuffleMemoryManager
   * @param mem
   */
  private[spark] def releaseMemoryForThread(mem: Long) = {
    this.allocatedMemory -= mem
    shuffleMemoryManager.notifyMemoryRelease(this, mem)
  }

  private[spark] def maybeSpill(asker:Spillable[_]): Boolean = {
    if (asker.elementsRead > asker.trackMemoryThreshold && asker.elementsRead % 32 == 0 &&
      asker.memorySize >= asker.allocatedMemory) {
      logInfo(s"Thread ${Thread.currentThread().getId}, objID=${System.identityHashCode(asker)} in maybeSpill: elementsRead:${asker.elementsRead}, trackMemoryThreshold:${asker.trackMemoryThreshold}, currentMemory:${asker.memorySize}, myMemoryThreshold:${asker.allocatedMemory}")
      // Claim up to double our current memory from the shuffle memory pool
      val amountToRequest = 2 * asker.memorySize - asker.allocatedMemory
      val granted = requestMemory(asker, amountToRequest)

      logInfo(s"Thread ${Thread.currentThread().getId} requsting ${amountToRequest} memory, got ${granted}")
      asker.allocatedMemory += granted
      logInfo(s"Thread ${Thread.currentThread().getId}'s memory threshold becomes ${asker.allocatedMemory}")
      if (asker.allocatedMemory <= asker.memorySize) {
        val freed = asker.allocatedMemory
        spill(asker)
        logInfo(s"Thread ${Thread.currentThread().getId}, obj ${System.identityHashCode(asker)} finished spilling")
        releaseMemoryForThread(freed)
        return true
      }
    }
    false
  }

  private def spill(asker: Spillable[_]): Long ={
    val freed = asker.allocatedMemory
    val spilledSize = asker.memorySize
    asker.spill
    asker.logSpillage(spilledSize)
    asker.allocatedMemory = 0
    objs -= asker
    freed
  }
}

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

  private val threadMemory = new mutable.HashMap[Long,(SpillableTaskMemoryManager, Long)]()  // threadId -> (ThreadMemoryManager, memoryAllocatedForThatThread)

  def this(conf: SparkConf) = this(ShuffleMemoryManager.getMaxMemory(conf))

  def getThreadMemoryManager: SpillableTaskMemoryManager = synchronized {
    val threadId = Thread.currentThread().getId
    // Add this thread to the threadMemory map just so we can keep an accurate count of the number
    // of active threads, to let other threads ramp down their memory in calls to tryToAcquire
    if (!threadMemory.contains(threadId)) {
      threadMemory(threadId) = (new SpillableTaskMemoryManager(this), 0L)
      notifyAll()  // Will later cause waiting threads to wake up and check numThreads again
    }
    threadMemory(threadId)._1
  }
  /**
   * Try to acquire up to numBytes memory for the current thread, and return the number of bytes
   * obtained, or 0 if none can be allocated. This call may block until there is enough free memory
   * in some situations, to make sure each thread has a chance to ramp up to at least 1 / 2N of the
   * total memory pool (where N is the # of active threads) before it is forced to spill. This can
   * happen if the number of threads increases but an older thread had a lot of memory already.
   */
  def tryToAcquire(asker:SpillableTaskMemoryManager, numBytes: Long, expelPrevious: Boolean = true): Long = synchronized {//TODO: remove the expelPrevious flag
    val threadId = Thread.currentThread().getId
    logInfo(s"Thread ${threadId} is trying to acquire ${numBytes}")
    logInfo(s"current memory allocation ${threadMemory.map{case (k, (v1,v2)) => (k, v2)}}")
    assert(numBytes > 0, "invalid number of bytes requested: " + numBytes)
    assert(threadMemory(threadId)._1 == asker)

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

  /** Release all memory for the current thread and mark it as inactive (e.g. when a task ends). */
  def releaseMemoryForThisThread(): Unit = synchronized {
    val threadId = Thread.currentThread().getId
    logInfo(s"Thread $threadId is freed!")
    threadMemory.remove(threadId)
    notifyAll()  // Notify waiters who locked "this" in tryToAcquire that memory has been freed
  }

  /** called by [[SpillableTaskMemoryManager]] to refresh memory allocation*/
  private[spark] def notifyMemoryRelease(manager: SpillableTaskMemoryManager, l: Long) = synchronized {
    val threadId = Thread.currentThread().getId
    threadMemory(threadId) = (manager, manager.allocatedMemory) //manager should already have updated/released memory
    notifyAll() // Notify waiters who locked "this" in tryToAcquire that memory has been freed
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
