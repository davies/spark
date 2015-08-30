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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Partition, Partitioner, TaskContext}

/**
 * An RDD that applies a user provided function to every partition of the parent RDD, and
 * additionally allows the user to prepare each partition before computing the parent partition.
 */
private[spark] class MapPartitionsWithPreparationRDD[U: ClassTag, T: ClassTag, M: ClassTag](
    prev: RDD[T],
    preparePartition: () => M,
    executePartition: (TaskContext, Int, M, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

  override val partitioner: Option[Partitioner] = {
    if (preservesPartitioning) firstParent[T].partitioner else None
  }

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  private[this] var preparedArgument: Option[M] = None

  def prepare(): M = {
    // This could be called multiple times, by compute or parent's compute
    if (preparedArgument.isEmpty) {
      preparedArgument = Some(preparePartition())
    }
    preparedArgument.get
  }

  /**
   * Prepare a partition before computing it from its parent.
   */
  override def compute(partition: Partition, context: TaskContext): Iterator[U] = {
    val prepared = prepare()
    // The same RDD could be called multiple times in one task, each call of compute() should
    // have separate prepared argument.
    preparedArgument = None
    val parentIterator = firstParent[T].iterator(partition, context)
    executePartition(context, partition.index, prepared, parentIterator)
  }
}
