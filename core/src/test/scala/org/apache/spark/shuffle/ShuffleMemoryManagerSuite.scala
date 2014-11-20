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

import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.apache.spark.util.collection.Spillable
import org.scalatest.FunSuite
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._

class ShuffleMemoryManagerSuite extends FunSuite with Timeouts {
  /** Launch a thread with the given body block and return it. */
  private def startThread(name: String)(body: => Unit): Thread = {
    val thread = new Thread("ShuffleMemorySuite " + name) {
      override def run() {
        body
      }
    }
    thread.start()
    thread
  }

  def mockSpillable = new Spillable[String] with Logging {
    override def spill: Unit = Unit
    override var elementsRead: Long = 0L
    override def memorySize: Long = 0
  }

  def withSparkContext(doWithSc: SparkContext => Unit): Unit ={
    val sc = new SparkContext("local", "test", new SparkConf)
    doWithSc(sc)
  }

  test("single thread requesting memory") {
    withSparkContext { sc =>
      val manager = new ShuffleMemoryManager(1000L)
      val asker = mockSpillable
      assert(manager.getThreadMemoryManager.requestMemory(asker, 100L) === 100L)
      assert(manager.getThreadMemoryManager.requestMemory(asker, 400L) === 400L)
      assert(manager.getThreadMemoryManager.requestMemory(asker, 400L) === 400L)
      assert(manager.getThreadMemoryManager.requestMemory(asker, 200L) === 100L)
      assert(manager.getThreadMemoryManager.requestMemory(asker, 100L) === 0L)
      assert(manager.getThreadMemoryManager.requestMemory(asker, 100L) === 0L)
      manager.getThreadMemoryManager.releaseMemoryForThread(500L)

      assert(manager.getThreadMemoryManager.requestMemory(asker, 300L) === 300L)
      assert(manager.getThreadMemoryManager.requestMemory(asker, 300L) === 200L)

      manager.releaseMemoryForThisThread()
      assert(manager.getThreadMemoryManager.requestMemory(asker, 1000L) === 1000L)
      assert(manager.getThreadMemoryManager.requestMemory(asker, 100L) === 0L)
    }
  }


  test("two threads requesting full memory") {
    withSparkContext { sc =>
      // Two threads request 500 bytes first, wait for each other to get it, and then request
      // 500 more; we should immediately return 0 as both are now at 1 / N

      val manager = new ShuffleMemoryManager(1000L)
      val asker1 = mockSpillable
      val asker2 = mockSpillable
      class State {
        var t1Result1 = -1L
        var t2Result1 = -1L
        var t1Result2 = -1L
        var t2Result2 = -1L
      }
      val state = new State

      val t1 = startThread("t1") {
        val r1 = manager.getThreadMemoryManager.requestMemory(asker1, 500L)
        state.synchronized {
          state.t1Result1 = r1
          state.notifyAll()
          while (state.t2Result1 === -1L) {
            state.wait()
          }
        }
        val r2 = manager.getThreadMemoryManager.requestMemory(asker1, 500L)
        state.synchronized {
          state.t1Result2 = r2
        }
      }

      val t2 = startThread("t2") {
        val r1 = manager.getThreadMemoryManager.requestMemory(asker2, 500L)
        state.synchronized {
          state.t2Result1 = r1
          state.notifyAll()
          while (state.t1Result1 === -1L) {
            state.wait()
          }
        }
        val r2 = manager.getThreadMemoryManager.requestMemory(asker2, 500L)
        state.synchronized {
          state.t2Result2 = r2
        }
      }

      failAfter(20 seconds) {
        t1.join()
        t2.join()
      }

      assert(state.t1Result1 === 500L)
      assert(state.t2Result1 === 500L)
      assert(state.t1Result2 === 0L)
      assert(state.t2Result2 === 0L)
    }
  }

  test("threads can block to get at least 1 / 2N memory") {
    // t1 grabs 1000 bytes and then waits until t2 is ready to make a request. It sleeps
    // for a bit and releases 250 bytes, which should then be greanted to t2. Further requests
    // by t2 will return false right away because it now has 1 / 2N of the memory.
     withSparkContext { sc =>
       val asker1 = mockSpillable
       val asker2 = mockSpillable
       val manager = new ShuffleMemoryManager(1000L)

       class State {
         var t1Requested = false
         var t2Requested = false
         var t1Result = -1L
         var t2Result = -1L
         var t2Result2 = -1L
         var t2WaitTime = 0L
       }
       val state = new State

       val t1 = startThread("t1") {
         state.synchronized {
           state.t1Result = manager.getThreadMemoryManager.requestMemory(asker1, 1000L)
           state.t1Requested = true
           state.notifyAll()
           while (!state.t2Requested) {
             state.wait()
           }
         }
         // Sleep a bit before releasing our memory; this is hacky but it would be difficult to make
         // sure the other thread blocks for some time otherwise
         Thread.sleep(300)
         manager.getThreadMemoryManager.releaseMemoryForThread(250L)
       }

       val t2 = startThread("t2") {
         state.synchronized {
           while (!state.t1Requested) {
             state.wait()
           }
           state.t2Requested = true
           state.notifyAll()
         }
         val startTime = System.currentTimeMillis()
         val result = manager.getThreadMemoryManager.requestMemory(asker2, 250L)
         val endTime = System.currentTimeMillis()
         state.synchronized {
           state.t2Result = result
           // A second call should return 0 because we're now already at 1 / 2N
           state.t2Result2 = manager.getThreadMemoryManager.requestMemory(asker2, 100L)
           state.t2WaitTime = endTime - startTime
         }
       }

       failAfter(20 seconds) {
         t1.join()
         t2.join()
       }

       // Both threads should've been able to acquire their memory; the second one will have waited
       // until the first one acquired 1000 bytes and then released 250
       state.synchronized {
         assert(state.t1Result === 1000L, "t1 could not allocate memory")
         assert(state.t2Result === 250L, "t2 could not allocate memory")
         assert(state.t2WaitTime > 200, s"t2 waited less than 200 ms (${state.t2WaitTime})")
         assert(state.t2Result2 === 0L, "t1 got extra memory the second time")
       }
     }
  }

}
