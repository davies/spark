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

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, StaticMemoryManager, TaskMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.types.LongType
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.util.{KnownSizeEstimation, Utils}

/**
 * Interface for a hashed relation by some key. Use [[HashedRelation.apply]] to create a concrete
 * object.
 */
private[execution] sealed trait HashedRelation {
  /**
   * Returns matched rows.
   *
   * Returns null if there is no matched rows.
   */
  def get(key: InternalRow): Iterator[InternalRow]

  /**
   * Returns matched rows for a key that has only one column with LongType.
   *
   * Returns null if there is no matched rows.
   */
  def get(key: Long): Iterator[InternalRow] = {
    throw new UnsupportedOperationException
  }

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
   * Returns true iff all the keys are unique.
   */
  def keyIsUnique: Boolean

  /**
   * Returns a read-only copy of this, to be safely used in current thread.
   */
  def asReadOnlyCopy(): HashedRelation

  /**
   * Returns the size of used memory.
   */
  def getMemorySize: Long = 1L  // to make the test happy

  /**
   * Release any used resources.
   */
  def close(): Unit
}

private[execution] object HashedRelation {

  /**
   * Create a HashedRelation from an Iterator of InternalRow.
   *
   * Note: The caller should make sure that these InternalRow are different objects.
   */
  def apply(
      input: Iterator[InternalRow],
      key: Seq[Expression],
      sizeEstimate: Int = 64,
      taskMemoryManager: TaskMemoryManager = null): HashedRelation = {
    val mm = Option(taskMemoryManager).getOrElse {
      new TaskMemoryManager(
        new StaticMemoryManager(
          new SparkConf().set("spark.memory.offHeap.enabled", "false"),
          Long.MaxValue,
          Long.MaxValue,
          1),
        0)
    }

    if (key.length == 1 && key.head.dataType == LongType) {
      LongHashedRelation(input, key, sizeEstimate, mm)
    } else {
      UnsafeHashedRelation(input, key, sizeEstimate, mm)
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

  override def keyIsUnique: Boolean = binaryMap.numKeys() == binaryMap.numValues()

  override def asReadOnlyCopy(): UnsafeHashedRelation =
    new UnsafeHashedRelation(numFields, binaryMap)

  override def getMemorySize: Long = {
    binaryMap.getTotalMemoryConsumption
  }

  override def estimatedSize: Long = {
    binaryMap.getTotalMemoryConsumption
  }

  // re-used in get()/getValue()
  var resultRow = new UnsafeRow(numFields)

  override def get(key: InternalRow): Iterator[InternalRow] = {
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
          _hasNext = loc.nextValue()
          resultRow
        }
      }
    } else {
      null
    }
  }

  def getValue(key: InternalRow): InternalRow = {
    val unsafeKey = key.asInstanceOf[UnsafeRow]
    val map = binaryMap  // avoid the compiler error
    val loc = new map.Location  // this could be allocated in stack
    binaryMap.safeLookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
      unsafeKey.getSizeInBytes, loc, unsafeKey.hashCode())
    if (loc.isDefined) {
      resultRow.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
      resultRow
    } else {
      null
    }
  }

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
      key: Seq[Expression],
      sizeEstimate: Int,
      taskMemoryManager: TaskMemoryManager): HashedRelation = {

    val pageSizeBytes = Option(SparkEnv.get).map(_.memoryManager.pageSizeBytes)
      .getOrElse(new SparkConf().getSizeAsBytes("spark.buffer.pageSize", "16m"))

    val binaryMap = new BytesToBytesMap(
      taskMemoryManager,
      // Only 70% of the slots can be used before growing, more capacity help to reduce collision
      (sizeEstimate * 1.5 + 1).toInt,
      pageSizeBytes)

    // Create a mapping of buildKeys -> rows
    val keyGenerator = UnsafeProjection.create(key)
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
  // the largest prime that below 2^n
  val LARGEST_PRIMES = {
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

  val DENSE_FACTOR = 0.2
}

/**
 * An hash map mapping from key of Long to UnsafeRow.
 */
final class LongToUnsafeRowMap(var mm: TaskMemoryManager, capacity: Int)
  extends MemoryConsumer(mm) with Externalizable {
  import org.apache.spark.sql.execution.joins.LongToUnsafeRowMap._

  // Whether the keys are stored in dense mode or not.
  private var isDense = false

  // The minimum value of keys.
  private var minKey = Long.MaxValue

  // The Maxinum value of keys.
  private var maxKey = Long.MinValue

  // Sparse mode: the actual capacity of map, is a prime number.
  private var cap: Int = 0

  // The array to store the key and offset of UnsafeRow in the page.
  //
  // Sparse mode: [key1] [offset1 | size1] [key2] [offset | size2] ...
  // Dense mode: [offset1 | size1] [offset2 | size2]
  private var array: Array[Long] = null

  // The page to store all bytes of UnsafeRow and the pointer to next rows.
  // [row1][pointer1] [row2][pointer2]
  private var page: Array[Byte] = null

  // Current write cursor in the page.
  private var cursor = Platform.BYTE_ARRAY_OFFSET

  // The total number of values of all keys.
  private var numValues = 0

  // The number of unique keys.
  private var numKeys = 0

  def this() = this(null, 0)  // needed by serializer

  private def acquireMemory(size: Long): Unit = {
    // do not support spilling
    val got = mm.acquireExecutionMemory(size, MemoryMode.ON_HEAP, this)
    if (got < size) {
      mm.releaseExecutionMemory(got, MemoryMode.ON_HEAP, this)
      throw new SparkException(s"Can't acquire $size bytes memory to build hash relation")
    }
  }

  private def init(): Unit = {
    if (mm != null) {
      cap = LARGEST_PRIMES.find(_ > capacity).getOrElse{
        sys.error(s"Can't create map with capacity $capacity")
      }
      acquireMemory(cap * 2 * 8 + (1 << 20))
      array = new Array[Long](cap * 2)
      page = new Array[Byte](1 << 20)  // 1M bytes
    }
  }

  init()

  def spill(size: Long, trigger: MemoryConsumer): Long = {
    0L
  }

  /**
   * Returns whether all the keys are unique.
   * @return
   */
  def keyIsUnique: Boolean = numKeys == numValues

  /**
   * Returns total memory consumption.
   */
  def getTotalMemoryConsumption: Long = {
    array.length * 8 + page.length
  }

  /**
   * Returns the slot of array that store the keys (sparse mode).
   */
  private def getSlot(key: Long): Int = {
    var s = (key % cap).toInt
    while (s < 0) {
      s += cap
    }
    s * 2
  }

  /**
   * Returns the single UnsafeRow for given key, or null if not found.
   */
  def getValue(key: Long, resultRow: UnsafeRow): UnsafeRow = {
    if (isDense) {
      val idx = (key - minKey).toInt
      if (idx >= 0 && key <= maxKey && array(idx) > 0) {
        val addr = array(idx)
        val offset = addr >>> 32
        val size = addr & 0xffffffffL
        resultRow.pointTo(page, offset, size.toInt)
        return resultRow
      }
    } else {
      var pos = getSlot(key)
      var step = 1
      while (array(pos + 1) != 0) {
        if (array(pos) == key) {
          val addr = array(pos + 1)
          val offset = addr >>> 32
          val size = addr & 0xffffffffL
          resultRow.pointTo(page, offset, size.toInt)
          return resultRow
        }
        pos += 2 * step
        step += 1
        if (pos >= cap) {
          pos -= cap
        }
      }
    }
    null
  }

  /**
   * Returns an iterator for all the values for the given key, or null if no value found.
   */
  def get(key: Long, resultRow: UnsafeRow): Iterator[UnsafeRow] = {
    if (isDense) {
      val idx = (key - minKey).toInt
      if (idx >=0 && key <= maxKey && array(idx) > 0) {
        return new Iterator[UnsafeRow] {
          var addr = array(idx)
          override def hasNext: Boolean = addr != 0
          override def next(): UnsafeRow = {
            val offset = addr >>> 32
            val size = addr & 0xffffffffL
            resultRow.pointTo(page, offset, size.toInt)
            addr = Platform.getLong(page, offset + size)
            resultRow
          }
        }
      }
    } else {
      var pos = getSlot(key)
      var step = 1
      while (array(pos + 1) != 0) {
        if (array(pos) == key) {
          return new Iterator[UnsafeRow] {
            var addr = array(pos + 1)
            override def hasNext: Boolean = addr != 0
            override def next(): UnsafeRow = {
              val offset = addr >>> 32
              val size = addr & 0xffffffffL
              resultRow.pointTo(page, offset, size.toInt)
              addr = Platform.getLong(page, offset + size)
              resultRow
            }
          }
        }
        pos += 2 * step
        step += 1
        if (pos >= cap) {
          pos -= cap
        }
      }
    }
    null
  }

  /**
   * Appends the key and row into this map.
   */
  def append(key: Long, row: UnsafeRow): Unit = {
    if (key < minKey) {
      minKey = key
    }
    if (key > maxKey) {
      maxKey = key
    }

    if (cursor + 8 + row.getSizeInBytes > page.length + Platform.BYTE_ARRAY_OFFSET) {
      val used = page.length
      if (used * 2L > (1L << 31)) {
        sys.error("Can't allocate a page that is larger than 2G")
      }
      acquireMemory(used * 2)
      val newPage = new Array[Byte](used * 2)
      System.arraycopy(page, 0, newPage, 0, cursor - Platform.BYTE_ARRAY_OFFSET)
      page = newPage
      mm.releaseExecutionMemory(used, MemoryMode.ON_HEAP, this)
    }
    val offset = cursor
    Platform.copyMemory(row.getBaseObject, row.getBaseOffset, page, cursor, row.getSizeInBytes)
    cursor += row.getSizeInBytes
    Platform.putLong(page, cursor, 0)
    cursor += 8
    numValues += 1
    updateIndex(key, (offset.toLong << 32) | row.getSizeInBytes)
  }

  /**
   * Update the address in array for given key.
   */
  private def updateIndex(key: Long, address: Long): Unit = {
    var pos = getSlot(key)
    var step = 1
    while (array(pos + 1) != 0 && array(pos) != key) {
      pos += 2 * step
      step += 1
      if (pos >= cap) {
        pos -= cap
      }
    }
    if (array(pos + 1) == 0) {
      // this is the first value for this key, put the address in array.
      array(pos) = key
      array(pos + 1) = address
      numKeys += 1
      if (numKeys * 2 > cap) {
        // reach half of the capacity
        growArray()
      }
    } else {
      // there is another value for this key, put the address at the end of final value.
      var addr = array(pos + 1)
      var pointer = (addr >>> 32) + (addr & 0xffffffffL)
      while (Platform.getLong(page, pointer) != 0) {
        addr = Platform.getLong(page, pointer)
        pointer = (addr >>> 32) + (addr & 0xffffffffL)
      }
      Platform.putLong(page, pointer, address)
    }
  }

  private def growArray(): Unit = {
    val old_cap = cap
    var old_array = array
    cap = LARGEST_PRIMES.find(_ > cap).getOrElse{
      sys.error(s"Can't grow map any more than $cap")
    }
    numKeys = 0
    acquireMemory(cap * 2 * 8)
    array = new Array[Long](cap * 2)
    var i = 0
    while (i < old_cap * 2) {
      if (old_array(i + 1) > 0) {
        updateIndex(old_array(i), old_array(i + 1))
      }
      i += 2
    }
    old_array = null  // release the reference to old array
    mm.releaseExecutionMemory(old_cap * 2 * 8, MemoryMode.ON_HEAP, this)
  }

  /**
   * Try to turn the map into dense mode, which is faster to probe.
   */
  def optimize(): Unit = {
    val range = maxKey - minKey
    // Convert to dense mode if it does not require more memory or could fit within L1 cache
    if (range < cap * 2 || range < 1024) {
      try {
        acquireMemory((range + 1) * 8)
      } catch {
        case e: SparkException =>
          // there is no enough memory to convert
          return
      }
      val denseArray = new Array[Long]((range + 1).toInt)
      var i = 0
      while (i < array.length) {
        if (array(i + 1) > 0) {
          val idx = (array(i) - minKey).toInt
          denseArray(idx) = array(i + 1)
        }
        i += 2
      }
      array = denseArray
      isDense = true
      mm.releaseExecutionMemory(cap * 2 * 8, MemoryMode.ON_HEAP, this)
    }
  }

  def free(): Unit = {
    if (page != null) {
      mm.releaseExecutionMemory(page.length, MemoryMode.ON_HEAP, this)
      page = null
    }
    if (array != null) {
      mm.releaseExecutionMemory(array.length * 8, MemoryMode.ON_HEAP, this)
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeBoolean(isDense)
    out.writeLong(minKey)
    out.writeLong(maxKey)
    out.writeInt(numKeys)
    out.writeInt(numValues)
    out.writeInt(cap)

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
    isDense = in.readBoolean()
    minKey = in.readLong()
    maxKey = in.readLong()
    numKeys = in.readInt()
    numValues = in.readInt()
    cap = in.readInt()

    val length = in.readInt()
    array = new Array[Long](length)
    val buffer = new Array[Byte](64 << 10)
    var offset = Platform.LONG_ARRAY_OFFSET
    val end = length * 8 + Platform.LONG_ARRAY_OFFSET
    while (offset < end) {
      val size = Math.min(buffer.length, end - offset)
      in.readFully(buffer, 0, size)
      Platform.copyMemory(buffer, Platform.BYTE_ARRAY_OFFSET, array, offset, size)
      offset += size
    }

    val numBytes = in.readInt()
    page = new Array[Byte](numBytes)
    in.readFully(page)
  }
}

private[joins] class LongHashedRelation(
  private var nFields: Int,
  private var map: LongToUnsafeRowMap)
  extends HashedRelation with Externalizable {

  private var resultRow: UnsafeRow = new UnsafeRow(nFields)

  // Needed for serialization (it is public to make Java serialization work)
  def this() = this(0, null)

  override def asReadOnlyCopy(): LongHashedRelation = new LongHashedRelation(nFields, map)

  override def getMemorySize: Long = {
    map.getTotalMemoryConsumption
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
    map.get(key, resultRow)

  override def getValue(key: Long): InternalRow = {
    map.getValue(key, resultRow)
  }

  override def keyIsUnique: Boolean = map.keyIsUnique

  override def close(): Unit = {
    map.free()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(nFields)
    out.writeObject(map)
  }

  override def readExternal(in: ObjectInput): Unit = {
    nFields = in.readInt()
    resultRow = new UnsafeRow(nFields)
    map = in.readObject().asInstanceOf[LongToUnsafeRowMap]
  }
}

/**
 * Create hashed relation with key that is long.
 */
private[joins] object LongHashedRelation {
  def apply(
      input: Iterator[InternalRow],
      key: Seq[Expression],
      sizeEstimate: Int,
      taskMemoryManager: TaskMemoryManager): LongHashedRelation = {

    val map: LongToUnsafeRowMap = new LongToUnsafeRowMap(taskMemoryManager, sizeEstimate)
    val keyGenerator = UnsafeProjection.create(key)

    // Create a mapping of key -> rows
    var numFields = 0
    while (input.hasNext) {
      val unsafeRow = input.next().asInstanceOf[UnsafeRow]
      numFields = unsafeRow.numFields()
      val rowKey = keyGenerator(unsafeRow)
      if (!rowKey.anyNull) {
        val key = rowKey.getLong(0)
        map.append(key, unsafeRow)
      }
    }
    map.optimize()
    new LongHashedRelation(numFields, map)
  }
}

/** The HashedRelationBroadcastMode requires that rows are broadcasted as a HashedRelation. */
private[execution] case class HashedRelationBroadcastMode(key: Seq[Expression])
  extends BroadcastMode {

  override def transform(rows: Array[InternalRow]): HashedRelation = {
    HashedRelation(rows.iterator, canonicalizedKey, rows.length)
  }

  private lazy val canonicalizedKey: Seq[Expression] = {
    key.map { e => e.canonicalized }
  }

  override def compatibleWith(other: BroadcastMode): Boolean = other match {
    case m: HashedRelationBroadcastMode => canonicalizedKey == m.canonicalizedKey
    case _ => false
  }
}
