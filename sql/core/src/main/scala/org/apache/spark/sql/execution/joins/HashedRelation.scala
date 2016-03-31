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

package org.apache.spark.sql.execution.joins

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}
import java.util.{HashMap => JavaHashMap}

import org.apache.spark.{SparkConf, SparkEnv, SparkException, TaskContext}
import org.apache.spark.memory.{StaticMemoryManager, TaskMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.util.{KnownSizeEstimation, Utils}

/**
 * Interface for a hashed relation by some key. Use [[HashedRelation.apply]] to create a concrete
 * object.
 */
private[execution] sealed trait HashedRelation {

  /**
   * Returns the matched single row.
   */
  def getValue(key: InternalRow): InternalRow

  /**
   * Returns the matched single row with key that have only one column of LongType.
   */
  def getValue(key: Long): InternalRow = {
    throw new UnsupportedOperationException
  }

  /**
    * Returns matched rows.
    */
  def get(key: InternalRow): Iterator[InternalRow] = {
    val row = getValue(key)
    if (row != null) {
      Array(row).toIterator
    } else {
      null
    }
  }

  /**
    * Returns matched rows for a key that has only one column with LongType.
    */
  def get(key: Long): Iterator[InternalRow] = {
    val row = getValue(key)
    if (row != null) {
      Array(row).toIterator
    } else {
      null
    }
  }

  /**
   * Returns true iff all the keys are unique.
   */
  def allUnique: Boolean

  /**
   * Returns a read-only copy of this, to be safely used in current thread.
   */
  def copy(): HashedRelation

  /**
    * Returns the size of used memory.
    */
  def getMemorySize: Long = 1L  // to make the test happy

  /**
   * Release any used resources.
   */
  def close(): Unit = {}
}

private[execution] object HashedRelation {

  /**
   * Create a HashedRelation from an Iterator of InternalRow.
   */
  def apply(
      canJoinKeyFitWithinLong: Boolean,
      input: Iterator[InternalRow],
      keyGenerator: Projection,
      sizeEstimate: Int = 64): HashedRelation = {

    if (canJoinKeyFitWithinLong) {
      LongHashedRelation(input, keyGenerator, sizeEstimate)
    } else {
      UnsafeHashedRelation(
        input, keyGenerator.asInstanceOf[UnsafeProjection], sizeEstimate)
    }
  }
}

/**
 * A HashedRelation for UnsafeRow, which is backed BytesToBytesMap.
 *
 * It's serialized in the following format:
 *  [number of keys]
 *  [size of key] [size of value] [key bytes] [bytes for value]
 */
private[joins] class UnsafeHashedRelation(
    private var numFields: Int,
    private var binaryMap: BytesToBytesMap)
  extends HashedRelation with KnownSizeEstimation with Externalizable {

  private[joins] def this() = this(0, null)  // Needed for serialization

  override def copy(): UnsafeHashedRelation = new UnsafeHashedRelation(numFields, binaryMap)

  override def getMemorySize: Long = {
    binaryMap.getTotalMemoryConsumption
  }

  override def estimatedSize: Long = {
    binaryMap.getTotalMemoryConsumption
  }

  // re-used in get()
  private var resultRow = new UnsafeRow(numFields)

  override def get(key: InternalRow): Iterator[UnsafeRow] = {
    val unsafeKey = key.asInstanceOf[UnsafeRow]
    val map = binaryMap  // avoid the compiler error
    val loc = new map.Location  // this could be allocated in stack
    binaryMap.safeLookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
      unsafeKey.getSizeInBytes, loc, unsafeKey.hashCode())
    if (loc.isDefined) {
      new Iterator[UnsafeRow] {
        private var _hasNext = true
        override def hasNext: Boolean = _hasNext
        override def next(): UnsafeRow = {
          resultRow.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
          _hasNext = !loc.nextValue()
          resultRow
        }
      }
    } else {
      null
    }
  }

  def getValue(k: InternalRow): InternalRow = {
    val key = k.asInstanceOf[UnsafeRow]
    val map = binaryMap  // avoid the compiler error
    val loc = new map.Location  // this could be allocated in stack
    binaryMap.safeLookup(key.getBaseObject, key.getBaseOffset, key.getSizeInBytes, loc,
      key.hashCode())
    if (loc.isDefined) {
      resultRow.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
      resultRow
    } else {
      null
    }
  }

  override def allUnique: Boolean = binaryMap.numKeys() == binaryMap.numValues()

  override def close(): Unit = {
    binaryMap.free()
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeInt(numFields)
    // TODO: move these into BytesToBytesMap
    out.writeInt(binaryMap.numKeys())
    out.writeInt(binaryMap.numValues())

    var buffer = new Array[Byte](64)
    def write(base: Object, offset: Long, length: Int): Unit = {
      if (buffer.length < length) {
        buffer = new Array[Byte](length)
      }
      Platform.copyMemory(base, offset, buffer, Platform.BYTE_ARRAY_OFFSET, length)
      out.write(buffer, 0, length)
    }

    val iter = binaryMap.iterator()
    while (iter.hasNext) {
      val loc = iter.next()
      // [key size] [values size] [key bytes] [value bytes]
      out.writeInt(loc.getKeyLength)
      out.writeInt(loc.getValueLength)
      write(loc.getKeyBase, loc.getKeyOffset, loc.getKeyLength)
      write(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    numFields = in.readInt()
    resultRow = new UnsafeRow(numFields)

    val nKeys = in.readInt()
    val nValues = in.readInt()
    // This is used in Broadcast, shared by multiple tasks, so we use on-heap memory
    // TODO(josh): This needs to be revisited before we merge this patch; making this change now
    // so that tests compile:
    val taskMemoryManager = new TaskMemoryManager(
      new StaticMemoryManager(
        new SparkConf().set("spark.memory.offHeap.enabled", "false"),
        Long.MaxValue,
        Long.MaxValue,
        1),
      0)

    val pageSizeBytes = Option(SparkEnv.get).map(_.memoryManager.pageSizeBytes)
      .getOrElse(new SparkConf().getSizeAsBytes("spark.buffer.pageSize", "16m"))

    // TODO(josh): We won't need this dummy memory manager after future refactorings; revisit
    // during code review

    binaryMap = new BytesToBytesMap(
      taskMemoryManager,
      (nKeys * 1.5 + 1).toInt, // reduce hash collision
      pageSizeBytes)

    var i = 0
    var keyBuffer = new Array[Byte](1024)
    var valuesBuffer = new Array[Byte](1024)
    while (i < nValues) {
      val keySize = in.readInt()
      val valuesSize = in.readInt()
      if (keySize > keyBuffer.length) {
        keyBuffer = new Array[Byte](keySize)
      }
      in.readFully(keyBuffer, 0, keySize)
      if (valuesSize > valuesBuffer.length) {
        valuesBuffer = new Array[Byte](valuesSize)
      }
      in.readFully(valuesBuffer, 0, valuesSize)

      val loc = binaryMap.lookup(keyBuffer, Platform.BYTE_ARRAY_OFFSET, keySize)
      val putSuceeded = loc.append(keyBuffer, Platform.BYTE_ARRAY_OFFSET, keySize,
        valuesBuffer, Platform.BYTE_ARRAY_OFFSET, valuesSize)
      if (!putSuceeded) {
        binaryMap.free()
        throw new IOException("Could not allocate memory to grow BytesToBytesMap")
      }
      i += 1
    }
  }
}

private[joins] object UnsafeHashedRelation {

  def apply(
      input: Iterator[InternalRow],
      keyGenerator: UnsafeProjection,
      sizeEstimate: Int): HashedRelation = {

    val taskMemoryManager = if (TaskContext.get() != null) {
      TaskContext.get().taskMemoryManager()
    } else {
      new TaskMemoryManager(
        new StaticMemoryManager(
          new SparkConf().set("spark.memory.offHeap.enabled", "false"),
          Long.MaxValue,
          Long.MaxValue,
          1),
        0)
    }
    val pageSizeBytes = Option(SparkEnv.get).map(_.memoryManager.pageSizeBytes)
      .getOrElse(new SparkConf().getSizeAsBytes("spark.buffer.pageSize", "16m"))

    val binaryMap = new BytesToBytesMap(
      taskMemoryManager,
      // Only 70% of the slots can be used before growing, more capacity help to reduce collision
      (sizeEstimate * 1.5 + 1).toInt,
      pageSizeBytes)

    // Create a mapping of buildKeys -> rows
    var numFields = 0
    while (input.hasNext) {
      val row = input.next().asInstanceOf[UnsafeRow]
      numFields = row.numFields()
      val key = keyGenerator(row)
      if (!key.anyNull) {
        val loc = binaryMap.lookup(key.getBaseObject, key.getBaseOffset, key.getSizeInBytes)
        val success = loc.append(
          key.getBaseObject, key.getBaseOffset, key.getSizeInBytes,
          row.getBaseObject, row.getBaseOffset, row.getSizeInBytes)
        if (!success) {
          binaryMap.free()
          throw new SparkException("There is no enough memory to build hash map")
        }
      }
    }

    new UnsafeHashedRelation(numFields, binaryMap)
  }
}

object LongToUnsafeRowMap {
  val PRIMES = {
    // the largest prime that below 2^n
    // https://primes.utm.edu/lists/2small/0bit.html
    val diffs = Seq(
      0, 1, 1, 3, 1, 3, 1, 5,
      3, 3, 9, 3, 1, 3, 19, 15,
      1, 5, 1, 3, 9, 3, 15, 3,
      39, 5, 39, 57, 3, 35, 1, 5
    )
    val primes = new Array[Int](32)
    primes(0) = 1
    var power2 = 1
    (1 until 32).foreach { i =>
      power2 *= 2
      primes(i) = power2 - diffs(i)
    }
    primes
  }
}

final class LongToUnsafeRowMap(capacity: Int) extends Externalizable {
  import org.apache.spark.sql.execution.joins.LongToUnsafeRowMap.PRIMES
  // The actual capacity of map, is a prime number.
  private var cap = PRIMES.find(_ > capacity).getOrElse{
    sys.error(s"Can't create map with capacity $capacity")
  }
  // The array to store the key and offset of UnsafeRow in the page
  // [key1] [offset1 | size1] [key2] [offset | size2] ...
  private var array = new Array[Long](cap * 2)
  // The page to store all bytes of UnsafeRow
  private var page = new Array[Byte](1 << 20) // 1M
  // Current write cursor in the page
  private var cursor = Platform.BYTE_ARRAY_OFFSET
  private var numValues = 0
  private var numKeys = 0
  // Whether all the keys are unique or not.
  private var isUnique = true

  def this() = this(0)  // needed by serializer

  def getTotalMemoryConsumption: Long = {
    array.length * 8 + page.length
  }

  private def getSlot(key: Long): Int = {
    var s = (key % cap).toInt * 2
    if (s < 0) {
      s += cap * 2
    }
    s
  }

  def getValue(key: Long, resultRow: UnsafeRow): UnsafeRow = {
    var pos = getSlot(key)
    while (array(pos + 1) != 0) {
      if (array(pos) == key) {
        val pointer = array(pos + 1)
        val offset = pointer >>> 32
        val size = pointer & 0xffffffffL
        resultRow.pointTo(page, offset, size.toInt)
        return resultRow
      }
      pos += 2
      if (pos == array.length) {
        pos = 0
      }
    }
    null
  }


  def get(key: Long, resultRow: UnsafeRow): Iterator[UnsafeRow] = {
    var pos = getSlot(key)
    while (array(pos + 1) != 0) {
      if (array(pos) == key) {
        return new Iterator[UnsafeRow] {
          var pointer = array(pos + 1)
          override def hasNext: Boolean = pointer != 0
          override def next(): UnsafeRow = {
            val offset = pointer >>> 32
            val size = pointer & 0xffffffffL
            resultRow.pointTo(page, offset, size.toInt)
            pointer = Platform.getLong(page, offset + size)
            resultRow
          }
        }
      }
      pos += 2
      if (pos == array.length) {
        pos = 0
      }
    }
    null
  }

  def append(key: Long, row: UnsafeRow): Unit = {
    if (cursor + 8 + row.getSizeInBytes > page.length + Platform.BYTE_ARRAY_OFFSET) {
      // TODO: memory manager
      if (page.length > (1L << 31)) {
        sys.error("Can't allocate a page that is larger than 2G")
      }
      val newPage = new Array[Byte](page.length * 2)
      System.arraycopy(page, 0, newPage, 0, cursor - Platform.BYTE_ARRAY_OFFSET)
      page = newPage
    }
    val offset = cursor
    Platform.copyMemory(row.getBaseObject, row.getBaseOffset, page, cursor, row.getSizeInBytes)
    cursor += row.getSizeInBytes
    Platform.putLong(page, cursor, 0)
    cursor += 8
    numValues += 1
    updateIndex(key, (offset.toLong << 32) | row.getSizeInBytes)
  }

  private def updateIndex(key: Long, address: Long): Unit = {
    var pos = getSlot(key)
    while (array(pos + 1) != 0 && array(pos) != key) {
      pos += 2
      if (pos == array.length) {
        pos = 0
      }
    }
    if (array(pos + 1) == 0) {
      array(pos) = key
      array(pos + 1) = address
      numKeys += 1
      if (numKeys * 2 > cap) {
        grow()
      }
    } else {
      var addr = array(pos + 1)
      var pointer = (addr >>> 32) + (addr & 0xffffffffL)
      while (Platform.getLong(page, pointer) != 0) {
        addr = Platform.getLong(page, pointer)
        pointer = (addr >>> 32) + (addr & 0xffffffffL)
      }
      Platform.putLong(page, pointer, address)
      isUnique = false
    }
  }

  private def grow(): Unit = {
    val cur_idx = PRIMES.indexOf(cap)
    if (cur_idx == PRIMES.length - 1) {
      sys.error("Can't grow map any more")
    }
    val old_cap = cap
    val old_array = array
    numKeys = 0
    cap = PRIMES(PRIMES.indexOf(cap) + 1)
    println(s"grow form ${old_cap} to ${cap}")
    array = new Array[Long](cap * 2)
    var i = 0
    while (i < old_cap * 2) {
      if (old_array(i + 1) > 0) {
        updateIndex(old_array(i), old_array(i + 1))
      }
      i += 2
    }
  }

  def allUnique: Boolean = isUnique

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeBoolean(isUnique)

    out.writeInt(array.length)
    val buffer = new Array[Byte](64 << 10)
    var offset = Platform.LONG_ARRAY_OFFSET
    val end = array.length * 8 + Platform.LONG_ARRAY_OFFSET
    while (offset < end) {
      val size = Math.min(buffer.length, end - offset)
      Platform.copyMemory(array, offset, buffer, Platform.BYTE_ARRAY_OFFSET, size)
      out.write(buffer, 0, size)
      offset += size
    }

    val used = cursor - Platform.BYTE_ARRAY_OFFSET
    out.writeInt(used)
    out.write(page, 0, used)
  }
  override def readExternal(in: ObjectInput): Unit = {
  }
}

private[joins] class LongHashedRelation(
  private var nFields: Int,
  private var hashTable: LongToUnsafeRowMap)
  extends HashedRelation with Externalizable {

  private var resultRow: UnsafeRow = new UnsafeRow(nFields)

  // Needed for serialization (it is public to make Java serialization work)
  def this() = this(0, null)

  override def copy(): LongHashedRelation = new LongHashedRelation(nFields, hashTable)

  override def getMemorySize: Long = {
    hashTable.getTotalMemoryConsumption
  }

  override def get(key: InternalRow): Iterator[InternalRow] = {
    if (key.isNullAt(0)) {
      null
    } else {
      get(key.getLong(0))
    }
  }
  override def getValue(key: InternalRow): InternalRow = {
    getValue(key.getLong(0))
  }

  override def get(key: Long): Iterator[InternalRow] =
    hashTable.get(key, resultRow)

  override def getValue(key: Long): InternalRow = {
    hashTable.getValue(key, resultRow)
  }

  override def allUnique: Boolean = hashTable.allUnique

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(nFields)
    out.writeObject(hashTable)
  }

  override def readExternal(in: ObjectInput): Unit = {
    nFields = in.readInt()
    resultRow = new UnsafeRow(nFields)
    hashTable = in.readObject().asInstanceOf[LongToUnsafeRowMap]
  }
}

/**
  * Create hashed relation with key that is long.
  */
private[joins] object LongHashedRelation {

  val DENSE_FACTOR = 0.2

  def apply(
    input: Iterator[InternalRow],
    keyGenerator: Projection,
    sizeEstimate: Int): HashedRelation = {

    val hashTable: LongToUnsafeRowMap = new LongToUnsafeRowMap(sizeEstimate)

    // Create a mapping of key -> rows
    var numFields = 0
    while (input.hasNext) {
      val unsafeRow = input.next().asInstanceOf[UnsafeRow]
      numFields = unsafeRow.numFields()
      val rowKey = keyGenerator(unsafeRow)
      if (!rowKey.anyNull) {
        val key = rowKey.getLong(0)
        hashTable.append(key, unsafeRow)
      }
    }

    new LongHashedRelation(numFields, hashTable)
  }
}

/** The HashedRelationBroadcastMode requires that rows are broadcasted as a HashedRelation. */
private[execution] case class HashedRelationBroadcastMode(
    canJoinKeyFitWithinLong: Boolean,
    keys: Seq[Expression],
    attributes: Seq[Attribute]) extends BroadcastMode {

  override def transform(rows: Array[InternalRow]): HashedRelation = {
    val generator = UnsafeProjection.create(keys, attributes)
    HashedRelation(canJoinKeyFitWithinLong, rows.iterator, generator, rows.length)
  }

  private lazy val canonicalizedKeys: Seq[Expression] = {
    keys.map { e =>
      BindReferences.bindReference(e.canonicalized, attributes)
    }
  }

  override def compatibleWith(other: BroadcastMode): Boolean = other match {
    case m: HashedRelationBroadcastMode =>
      canJoinKeyFitWithinLong == m.canJoinKeyFitWithinLong &&
        canonicalizedKeys == m.canonicalizedKeys
    case _ => false
  }
}
