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

package org.apache.spark.storage

import java.nio.ByteBuffer

import org.apache.spark.Logging

/**
 * Abstract class to store blocks.
 */
private[spark] abstract class BlockStore(val blockManager: BlockManager) extends Logging {

  def putBytes(blockId: BlockId, bytes: ByteBuffer, level: StorageLevel): Unit

  /**
   * Attempt to store an iterator of values.
   *
   * @return an iterator of values (in case the put failed), or the estimated size of the stored
   *         values if the put succeeded.
   */
  def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel): Either[Iterator[Any], Long]

  /**
   * Return the size of a block in bytes.
   */
  def getSize(blockId: BlockId): Long

  def getBytes(blockId: BlockId): Option[ByteBuffer]

  def getValues(blockId: BlockId): Option[Iterator[Any]]

  /**
   * Remove a block, if it exists.
   *
   * @param blockId the block to remove.
   * @return True if the block was found and removed, False otherwise.
   * @throws IllegalStateException if the block is pinned by a task.
   */
  def remove(blockId: BlockId): Boolean

  def contains(blockId: BlockId): Boolean

  def clear() { }
}
