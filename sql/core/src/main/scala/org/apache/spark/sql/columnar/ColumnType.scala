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

package org.apache.spark.sql.columnar

import java.math.{BigDecimal, BigInteger}
import java.nio.ByteBuffer

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

/**
 * An abstract class that represents type of a column. Used to append/extract Java objects into/from
 * the underlying [[ByteBuffer]] of a column.
 *
 * @tparam JvmType Underlying Java type to represent the elements.
 */
private[sql] sealed abstract class ColumnType[JvmType] {

  // The catalyst data type of this column.
  def dataType: DataType

  // Default size in bytes for one element of type T (e.g. 4 for `Int`).
  def defaultSize: Int

  /**
   * Extracts a value out of the buffer at the buffer's current position.
   */
  def extract(buffer: ByteBuffer): JvmType

  /**
   * Extracts a value out of the buffer at the buffer's current position and stores in
   * `row(ordinal)`. Subclasses should override this method to avoid boxing/unboxing costs whenever
   * possible.
   */
  def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    setField(row, ordinal, extract(buffer))
  }

  /**
   * Appends the given value v of type T into the given ByteBuffer.
   */
  def append(v: JvmType, buffer: ByteBuffer): Unit

  /**
   * Appends `row(ordinal)` of type T into the given ByteBuffer. Subclasses should override this
   * method to avoid boxing/unboxing costs whenever possible.
   */
  def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    append(getField(row, ordinal), buffer)
  }

  /**
   * Returns the size of the value `row(ordinal)`. This is used to calculate the size of variable
   * length types such as byte arrays and strings.
   */
  def actualSize(row: InternalRow, ordinal: Int): Int = defaultSize

  /**
   * Returns `row(ordinal)`. Subclasses should override this method to avoid boxing/unboxing costs
   * whenever possible.
   */
  def getField(row: InternalRow, ordinal: Int): JvmType

  /**
   * Sets `row(ordinal)` to `field`. Subclasses should override this method to avoid boxing/unboxing
   * costs whenever possible.
   */
  def setField(row: MutableRow, ordinal: Int, value: JvmType): Unit

  /**
   * Copies `from(fromOrdinal)` to `to(toOrdinal)`. Subclasses should override this method to avoid
   * boxing/unboxing costs whenever possible.
   */
  def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int): Unit = {
    setField(to, toOrdinal, getField(from, fromOrdinal))
  }

  /**
   * Creates a duplicated copy of the value.
   */
  def clone(v: JvmType): JvmType = v

  override def toString: String = getClass.getSimpleName.stripSuffix("$")
}

private[sql] object NULL extends ColumnType[Any] {

  override def dataType: DataType = NullType
  override def defaultSize: Int = 0
  override def append(v: Any, buffer: ByteBuffer): Unit = {}
  override def extract(buffer: ByteBuffer): Any = null
  override def setField(row: MutableRow, ordinal: Int, value: Any): Unit = row.setNullAt(ordinal)
  override def getField(row: InternalRow, ordinal: Int): Any = null
}

private[sql] abstract class NativeColumnType[T <: AtomicType](
    val dataType: T,
    val defaultSize: Int)
  extends ColumnType[T#InternalType] {

  /**
   * Scala TypeTag. Can be used to create primitive arrays and hash tables.
   */
  def scalaTag: TypeTag[dataType.InternalType] = dataType.tag
}

private[sql] object INT extends NativeColumnType(IntegerType, 4) {
  override def append(v: Int, buffer: ByteBuffer): Unit = {
    buffer.putInt(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putInt(row.getInt(ordinal))
  }

  override def extract(buffer: ByteBuffer): Int = {
    buffer.getInt()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setInt(ordinal, buffer.getInt())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Int): Unit = {
    row.setInt(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Int = row.getInt(ordinal)
}

private[sql] object LONG extends NativeColumnType(LongType, 8) {
  override def append(v: Long, buffer: ByteBuffer): Unit = {
    buffer.putLong(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putLong(row.getLong(ordinal))
  }

  override def extract(buffer: ByteBuffer): Long = {
    buffer.getLong()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setLong(ordinal, buffer.getLong())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Long): Unit = {
    row.setLong(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Long = row.getLong(ordinal)
}

private[sql] object FLOAT extends NativeColumnType(FloatType, 4) {
  override def append(v: Float, buffer: ByteBuffer): Unit = {
    buffer.putFloat(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putFloat(row.getFloat(ordinal))
  }

  override def extract(buffer: ByteBuffer): Float = {
    buffer.getFloat()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setFloat(ordinal, buffer.getFloat())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Float): Unit = {
    row.setFloat(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Float = row.getFloat(ordinal)
}

private[sql] object DOUBLE extends NativeColumnType(DoubleType, 8) {
  override def append(v: Double, buffer: ByteBuffer): Unit = {
    buffer.putDouble(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putDouble(row.getDouble(ordinal))
  }

  override def extract(buffer: ByteBuffer): Double = {
    buffer.getDouble()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setDouble(ordinal, buffer.getDouble())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Double): Unit = {
    row.setDouble(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Double = row.getDouble(ordinal)
}

private[sql] object BOOLEAN extends NativeColumnType(BooleanType, 1) {
  override def append(v: Boolean, buffer: ByteBuffer): Unit = {
    buffer.put(if (v) 1: Byte else 0: Byte)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.put(if (row.getBoolean(ordinal)) 1: Byte else 0: Byte)
  }

  override def extract(buffer: ByteBuffer): Boolean = buffer.get() == 1

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setBoolean(ordinal, buffer.get() == 1)
  }

  override def setField(row: MutableRow, ordinal: Int, value: Boolean): Unit = {
    row.setBoolean(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Boolean = row.getBoolean(ordinal)
}

private[sql] object BYTE extends NativeColumnType(ByteType, 1) {
  override def append(v: Byte, buffer: ByteBuffer): Unit = {
    buffer.put(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.put(row.getByte(ordinal))
  }

  override def extract(buffer: ByteBuffer): Byte = {
    buffer.get()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setByte(ordinal, buffer.get())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Byte): Unit = {
    row.setByte(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Byte = row.getByte(ordinal)
}

private[sql] object SHORT extends NativeColumnType(ShortType, 2) {
  override def append(v: Short, buffer: ByteBuffer): Unit = {
    buffer.putShort(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putShort(row.getShort(ordinal))
  }

  override def extract(buffer: ByteBuffer): Short = {
    buffer.getShort()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setShort(ordinal, buffer.getShort())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Short): Unit = {
    row.setShort(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Short = row.getShort(ordinal)
}

private[sql] object STRING extends NativeColumnType(StringType, 8) {
  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    row.getUTF8String(ordinal).numBytes() + 4
  }

  override def append(v: UTF8String, buffer: ByteBuffer): Unit = {
    buffer.putInt(v.numBytes())
    v.writeTo(buffer)
  }

  override def extract(buffer: ByteBuffer): UTF8String = {
    val length = buffer.getInt()
    assert(buffer.hasArray)
    val base = buffer.array()
    val offset = buffer.arrayOffset()
    val cursor = buffer.position()
    buffer.position(cursor + length)
    UTF8String.fromBytes(base, offset + cursor, length)
  }

  override def setField(row: MutableRow, ordinal: Int, value: UTF8String): Unit = {
    row.update(ordinal, value.clone())
  }

  override def getField(row: InternalRow, ordinal: Int): UTF8String = {
    row.getUTF8String(ordinal)
  }

  override def clone(v: UTF8String): UTF8String = v.clone()
}

private[sql] case class COMPACT_DECIMAL(precision: Int, scale: Int)
  extends NativeColumnType(DecimalType(precision, scale), 8) {

  override def extract(buffer: ByteBuffer): Decimal = {
    Decimal(buffer.getLong(), precision, scale)
  }

  override def append(v: Decimal, buffer: ByteBuffer): Unit = {
    buffer.putLong(v.toUnscaledLong)
  }

  override def getField(row: InternalRow, ordinal: Int): Decimal = {
    row.getDecimal(ordinal, precision, scale)
  }

  override def setField(row: MutableRow, ordinal: Int, value: Decimal): Unit = {
    row.setDecimal(ordinal, value, precision)
  }
}

private[sql] object COMPACT_DECIMAL {
  def apply(dt: DecimalType): COMPACT_DECIMAL = {
    COMPACT_DECIMAL(dt.precision, dt.scale)
  }
}

private[sql] sealed abstract class ByteArrayColumnType[JvmType](val defaultSize: Int)
  extends ColumnType[JvmType] {

  def serialize(value: JvmType): Array[Byte]
  def deserialize(bytes: Array[Byte]): JvmType

  override def append(v: JvmType, buffer: ByteBuffer): Unit = {
    val bytes = serialize(v)
    buffer.putInt(bytes.length).put(bytes, 0, bytes.length)
  }

  override def extract(buffer: ByteBuffer): JvmType = {
    val length = buffer.getInt()
    val bytes = new Array[Byte](length)
    buffer.get(bytes, 0, length)
    deserialize(bytes)
  }
}

private[sql] object BINARY extends ByteArrayColumnType[Array[Byte]](16) {

  def dataType: DataType = BinaryType

  override def setField(row: MutableRow, ordinal: Int, value: Array[Byte]): Unit = {
    row.update(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Array[Byte] = {
    row.getBinary(ordinal)
  }

  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    row.getBinary(ordinal).length + 4
  }

  def serialize(value: Array[Byte]): Array[Byte] = value
  def deserialize(bytes: Array[Byte]): Array[Byte] = bytes
}

private[sql] case class LARGE_DECIMAL(precision: Int, scale: Int)
  extends ByteArrayColumnType[Decimal](12) {

  override val dataType: DataType = DecimalType(precision, scale)

  override def getField(row: InternalRow, ordinal: Int): Decimal = {
    row.getDecimal(ordinal, precision, scale)
  }

  override def setField(row: MutableRow, ordinal: Int, value: Decimal): Unit = {
    row.setDecimal(ordinal, value, precision)
  }

  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    4 + getField(row, ordinal).toJavaBigDecimal.unscaledValue().bitLength() / 8 + 1
  }

  override def serialize(value: Decimal): Array[Byte] = {
    value.toJavaBigDecimal.unscaledValue().toByteArray
  }

  override def deserialize(bytes: Array[Byte]): Decimal = {
    val javaDecimal = new BigDecimal(new BigInteger(bytes), scale)
    Decimal.apply(javaDecimal, precision, scale)
  }
}

private[sql] object LARGE_DECIMAL {
  def apply(dt: DecimalType): LARGE_DECIMAL = {
    LARGE_DECIMAL(dt.precision, dt.scale)
  }
}

private[sql] case class STRUCT(dataType: StructType) extends ColumnType[UnsafeRow] {

  private val numOfFields: Int = dataType.fields.size
  private val unsafeRow = new UnsafeRow

  override def defaultSize: Int = 20

  override def setField(row: MutableRow, ordinal: Int, value: UnsafeRow): Unit = {
    row.update(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): UnsafeRow = {
    row.getStruct(ordinal, numOfFields).asInstanceOf[UnsafeRow]
  }

  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    4 + getField(row, ordinal).getSizeInBytes
  }

  override def append(value: UnsafeRow, buffer: ByteBuffer): Unit = {
    buffer.putInt(value.getSizeInBytes)
    value.writeTo(buffer)
  }

  override def extract(buffer: ByteBuffer): UnsafeRow = {
    val sizeInBytes = buffer.getInt()
    assert(buffer.hasArray)
    val base = buffer.array()
    val offset = buffer.arrayOffset()
    val cursor = buffer.position()
    unsafeRow.pointTo(base, Platform.BYTE_ARRAY_OFFSET + offset + cursor, numOfFields, sizeInBytes)
    buffer.position(cursor + sizeInBytes)
    unsafeRow
  }

  override def clone(v: UnsafeRow): UnsafeRow = v.copy()
}

private[sql] case class ARRAY(dataType: ArrayType) extends ColumnType[UnsafeArrayData] {
  private val array = new UnsafeArrayData

  override def defaultSize: Int = 16

  override def setField(row: MutableRow, ordinal: Int, value: UnsafeArrayData): Unit = {
    row.update(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): UnsafeArrayData = {
    row.getArray(ordinal).asInstanceOf[UnsafeArrayData]
  }

  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    val unsafeArray = getField(row, ordinal)
    4 + 4 + unsafeArray.getSizeInBytes
  }

  override def append(value: UnsafeArrayData, buffer: ByteBuffer): Unit = {
    buffer.putInt(value.numElements())
    buffer.putInt(value.getSizeInBytes)
    value.writeTo(buffer)
  }

  override def extract(buffer: ByteBuffer): UnsafeArrayData = {
    val numElements = buffer.getInt
    val sizeInBytes = buffer.getInt
    assert(buffer.hasArray)
    val base = buffer.array()
    val offset = buffer.arrayOffset()
    val cursor = buffer.position()
    array.pointTo(base, Platform.BYTE_ARRAY_OFFSET + offset + cursor, numElements, sizeInBytes)
    buffer.position(cursor + sizeInBytes)
    array
  }

  override def clone(v: UnsafeArrayData): UnsafeArrayData = v.copy()
}

private[sql] case class MAP(dataType: MapType) extends ColumnType[UnsafeMapData] {

  private val keyArray = new UnsafeArrayData
  private val valueArray = new UnsafeArrayData

  override def defaultSize: Int = 32

  override def setField(row: MutableRow, ordinal: Int, value: UnsafeMapData): Unit = {
    row.update(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): UnsafeMapData = {
    row.getMap(ordinal).asInstanceOf[UnsafeMapData]
  }

  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    val unsafeMap = getField(row, ordinal)
    12 + unsafeMap.keyArray().getSizeInBytes + unsafeMap.valueArray().getSizeInBytes
  }

  override def append(value: UnsafeMapData, buffer: ByteBuffer): Unit = {
    buffer.putInt(value.numElements())
    buffer.putInt(value.keyArray().getSizeInBytes)
    buffer.putInt(value.valueArray().getSizeInBytes)
    value.keyArray().writeTo(buffer)
    value.valueArray().writeTo(buffer)
  }

  override def extract(buffer: ByteBuffer): UnsafeMapData = {
    val numElements = buffer.getInt
    val keyArraySize = buffer.getInt
    val valueArraySize = buffer.getInt
    assert(buffer.hasArray)
    val base = buffer.array()
    val offset = buffer.arrayOffset()
    val cursor = buffer.position()
    keyArray.pointTo(base, Platform.BYTE_ARRAY_OFFSET + offset + cursor, numElements, keyArraySize)
    valueArray.pointTo(base, Platform.BYTE_ARRAY_OFFSET + offset + cursor + keyArraySize,
      numElements, valueArraySize)
    buffer.position(cursor + keyArraySize + valueArraySize)
    new UnsafeMapData(keyArray, valueArray)
  }

  override def clone(v: UnsafeMapData): UnsafeMapData = v.copy()
}

private[sql] object ColumnType {
  def apply(dataType: DataType): ColumnType[_] = {
    dataType match {
      case NullType => NULL
      case BooleanType => BOOLEAN
      case ByteType => BYTE
      case ShortType => SHORT
      case IntegerType | DateType => INT
      case LongType | TimestampType => LONG
      case FloatType => FLOAT
      case DoubleType => DOUBLE
      case StringType => STRING
      case BinaryType => BINARY
      case dt: DecimalType if dt.precision <= Decimal.MAX_LONG_DIGITS => COMPACT_DECIMAL(dt)
      case dt: DecimalType => LARGE_DECIMAL(dt)
      case arr: ArrayType => ARRAY(arr)
      case map: MapType => MAP(map)
      case struct: StructType => STRUCT(struct)
      case udt: UserDefinedType[_] => apply(udt.sqlType)
      case other =>
        throw new Exception(s"Unsupported type: $other")
    }
  }
}
