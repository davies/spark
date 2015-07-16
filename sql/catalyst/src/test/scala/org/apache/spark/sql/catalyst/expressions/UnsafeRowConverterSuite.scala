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

package org.apache.spark.sql.catalyst.expressions

import java.sql.{Date, Timestamp}
import java.util.Arrays

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.UTF8String

class UnsafeRowConverterSuite extends SparkFunSuite with Matchers {

  test("basic conversion with only primitive types") {
    val fieldTypes: Array[DataType] = Array(LongType, LongType, IntegerType)
    val converter = GenerateUnsafeProjection.generate(fieldTypes)

    val row = new SpecificMutableRow(fieldTypes)
    row.setLong(0, 0)
    row.setLong(1, 1)
    row.setInt(2, 2)

    val unsafeRow = converter.apply(row)
    assert(unsafeRow.getLong(0) === 0)
    assert(unsafeRow.getLong(1) === 1)
    assert(unsafeRow.getInt(2) === 2)

    // We can copy UnsafeRows as long as they don't reference ObjectPools
    val unsafeRowCopy = unsafeRow.copy()
    assert(unsafeRowCopy.getLong(0) === 0)
    assert(unsafeRowCopy.getLong(1) === 1)
    assert(unsafeRowCopy.getInt(2) === 2)

    unsafeRow.setLong(1, 3)
    assert(unsafeRow.getLong(1) === 3)
    unsafeRow.setInt(2, 4)
    assert(unsafeRow.getInt(2) === 4)

    // Mutating the original row should not have changed the copy
    assert(unsafeRowCopy.getLong(0) === 0)
    assert(unsafeRowCopy.getLong(1) === 1)
    assert(unsafeRowCopy.getInt(2) === 2)
  }

  test("basic conversion with primitive, string and binary types") {
    val fieldTypes: Array[DataType] = Array(LongType, StringType, BinaryType)
    val converter = GenerateUnsafeProjection.generate(fieldTypes)

    val row = new SpecificMutableRow(fieldTypes)
    row.setLong(0, 0)
    row.update(1, UTF8String.fromString("Hello"))
    row.update(2, "World".getBytes)

    val unsafeRow = converter.apply(row)
    assert(unsafeRow.getSizeInBytes === 8 + (8 * 3) +
      ByteArrayMethods.roundNumberOfBytesToNearestWord("Hello".getBytes.length) +
      ByteArrayMethods.roundNumberOfBytesToNearestWord("World".getBytes.length))

    assert(unsafeRow.getLong(0) === 0)
    assert(unsafeRow.getString(1) === "Hello")
    assert(unsafeRow.get(2) === "World".getBytes)

    unsafeRow.update(1, UTF8String.fromString("World"))
    assert(unsafeRow.getString(1) === "World")
    assert(unsafeRow.getPool.size === 0)
    unsafeRow.update(1, UTF8String.fromString("Hello World"))
    assert(unsafeRow.getString(1) === "Hello World")
    assert(unsafeRow.getPool.size === 1)

    unsafeRow.update(2, "World".getBytes)
    assert(unsafeRow.get(2) === "World".getBytes)
    assert(unsafeRow.getPool.size === 1)
    unsafeRow.update(2, "Hello World".getBytes)
    assert(unsafeRow.get(2) === "Hello World".getBytes)
    assert(unsafeRow.getPool.size === 2)
  }

  test("basic conversion with primitive, decimal and array") {
    val fieldTypes: Array[DataType] = Array(LongType, DecimalType(10, 0), ArrayType(StringType))
    val converter = GenerateUnsafeProjection.generate(fieldTypes)

    val row = new SpecificMutableRow(fieldTypes)
    row.setLong(0, 0)
    row.update(1, Decimal(1))
    row.update(2, Array(2))

    val unsafeRow = converter.apply(row)
    assert(unsafeRow.getSizeInBytes === 8 + (8 * 3))
    assert(unsafeRow.getPool.size === 2)

    assert(unsafeRow.getLong(0) === 0)
    assert(unsafeRow.get(1) === Decimal(1))
    assert(unsafeRow.get(2) === Array(2))

    unsafeRow.update(1, Decimal(2))
    assert(unsafeRow.get(1) === Decimal(2))
    unsafeRow.update(2, Array(3, 4))
    assert(unsafeRow.get(2) === Array(3, 4))
    assert(unsafeRow.getPool.size === 2)
  }

  test("basic conversion with primitive, string, date and timestamp types") {
    val fieldTypes: Array[DataType] = Array(LongType, StringType, DateType, TimestampType)
    val converter = GenerateUnsafeProjection.generate(fieldTypes)

    val row = new SpecificMutableRow(fieldTypes)
    row.setLong(0, 0)
    row.setString(1, "Hello")
    row.update(2, DateTimeUtils.fromJavaDate(Date.valueOf("1970-01-01")))
    row.update(3, DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2015-05-08 08:10:25")))

    val unsafeRow = converter.apply(row)
    assert(unsafeRow.getSizeInBytes === 8 + (8 * 4) +
      ByteArrayMethods.roundNumberOfBytesToNearestWord("Hello".getBytes.length))

    assert(unsafeRow.getLong(0) === 0)
    assert(unsafeRow.getString(1) === "Hello")
    // Date is represented as Int in unsafeRow
    assert(DateTimeUtils.toJavaDate(unsafeRow.getInt(2)) === Date.valueOf("1970-01-01"))
    // Timestamp is represented as Long in unsafeRow
    DateTimeUtils.toJavaTimestamp(unsafeRow.getLong(3)) should be
      (Timestamp.valueOf("2015-05-08 08:10:25"))

    unsafeRow.setInt(2, DateTimeUtils.fromJavaDate(Date.valueOf("2015-06-22")))
    assert(DateTimeUtils.toJavaDate(unsafeRow.getInt(2)) === Date.valueOf("2015-06-22"))
    unsafeRow.setLong(3, DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2015-06-22 08:10:25")))
    DateTimeUtils.toJavaTimestamp(unsafeRow.getLong(3)) should be
      (Timestamp.valueOf("2015-06-22 08:10:25"))
  }

  test("null handling") {
    val fieldTypes: Array[DataType] = Array(
      NullType,
      BooleanType,
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      StringType,
      BinaryType,
      DecimalType.Unlimited,
      ArrayType(IntegerType)
    )
    val converter = GenerateUnsafeProjection.generate(fieldTypes)

    val rowWithAllNullColumns: InternalRow = {
      val r = new SpecificMutableRow(fieldTypes)
      for (i <- 0 to fieldTypes.length - 1) {
        r.setNullAt(i)
      }
      r
    }

    val createdFromNull = converter.apply(rowWithAllNullColumns)

    for (i <- 0 to fieldTypes.length - 1) {
      assert(createdFromNull.isNullAt(i))
    }
    assert(createdFromNull.getBoolean(1) === false)
    assert(createdFromNull.getByte(2) === 0)
    assert(createdFromNull.getShort(3) === 0)
    assert(createdFromNull.getInt(4) === 0)
    assert(createdFromNull.getLong(5) === 0)
    assert(java.lang.Float.isNaN(createdFromNull.getFloat(6)))
    assert(java.lang.Double.isNaN(createdFromNull.getDouble(7)))
    assert(createdFromNull.getString(8) === null)
    assert(createdFromNull.get(9) === null)
    assert(createdFromNull.get(10) === null)
    assert(createdFromNull.get(11) === null)

    // If we have an UnsafeRow with columns that are initially non-null and we null out those
    // columns, then the serialized row representation should be identical to what we would get by
    // creating an entirely null row via the converter
    val rowWithNoNullColumns: InternalRow = {
      val r = new SpecificMutableRow(fieldTypes)
      r.setNullAt(0)
      r.setBoolean(1, false)
      r.setByte(2, 20)
      r.setShort(3, 30)
      r.setInt(4, 400)
      r.setLong(5, 500)
      r.setFloat(6, 600)
      r.setDouble(7, 700)
      r.update(8, UTF8String.fromString("hello"))
      r.update(9, "world".getBytes)
      r.update(10, Decimal(10))
      r.update(11, Array(11))
      r
    }
    val setToNullAfterCreation = converter.apply(rowWithNoNullColumns)

    assert(setToNullAfterCreation.isNullAt(0) === rowWithNoNullColumns.isNullAt(0))
    assert(setToNullAfterCreation.getBoolean(1) === rowWithNoNullColumns.getBoolean(1))
    assert(setToNullAfterCreation.getByte(2) === rowWithNoNullColumns.getByte(2))
    assert(setToNullAfterCreation.getShort(3) === rowWithNoNullColumns.getShort(3))
    assert(setToNullAfterCreation.getInt(4) === rowWithNoNullColumns.getInt(4))
    assert(setToNullAfterCreation.getLong(5) === rowWithNoNullColumns.getLong(5))
    assert(setToNullAfterCreation.getFloat(6) === rowWithNoNullColumns.getFloat(6))
    assert(setToNullAfterCreation.getDouble(7) === rowWithNoNullColumns.getDouble(7))
    assert(setToNullAfterCreation.getString(8) === rowWithNoNullColumns.getString(8))
    assert(setToNullAfterCreation.get(9) === rowWithNoNullColumns.get(9))
    assert(setToNullAfterCreation.get(10) === rowWithNoNullColumns.get(10))
    assert(setToNullAfterCreation.get(11) === rowWithNoNullColumns.get(11))

    for (i <- 0 to fieldTypes.length - 1) {
      if (i >= 8) {
        setToNullAfterCreation.update(i, null)
      }
      setToNullAfterCreation.setNullAt(i)
    }
    // There are some garbage left in the var-length area
    assert(Arrays.equals(createdFromNull.getBaseObject.asInstanceOf[Array[Byte]],
      java.util.Arrays.copyOf(setToNullAfterCreation.getBaseObject.asInstanceOf[Array[Byte]],
        createdFromNull.getSizeInBytes / 8)))

    setToNullAfterCreation.setNullAt(0)
    setToNullAfterCreation.setBoolean(1, false)
    setToNullAfterCreation.setByte(2, 20)
    setToNullAfterCreation.setShort(3, 30)
    setToNullAfterCreation.setInt(4, 400)
    setToNullAfterCreation.setLong(5, 500)
    setToNullAfterCreation.setFloat(6, 600)
    setToNullAfterCreation.setDouble(7, 700)
    setToNullAfterCreation.update(8, UTF8String.fromString("hello"))
    setToNullAfterCreation.update(9, "world".getBytes)
    setToNullAfterCreation.update(10, Decimal(10))
    setToNullAfterCreation.update(11, Array(11))

    assert(setToNullAfterCreation.isNullAt(0) === rowWithNoNullColumns.isNullAt(0))
    assert(setToNullAfterCreation.getBoolean(1) === rowWithNoNullColumns.getBoolean(1))
    assert(setToNullAfterCreation.getByte(2) === rowWithNoNullColumns.getByte(2))
    assert(setToNullAfterCreation.getShort(3) === rowWithNoNullColumns.getShort(3))
    assert(setToNullAfterCreation.getInt(4) === rowWithNoNullColumns.getInt(4))
    assert(setToNullAfterCreation.getLong(5) === rowWithNoNullColumns.getLong(5))
    assert(setToNullAfterCreation.getFloat(6) === rowWithNoNullColumns.getFloat(6))
    assert(setToNullAfterCreation.getDouble(7) === rowWithNoNullColumns.getDouble(7))
    assert(setToNullAfterCreation.getString(8) === rowWithNoNullColumns.getString(8))
    assert(setToNullAfterCreation.get(9) === rowWithNoNullColumns.get(9))
    assert(setToNullAfterCreation.get(10) === rowWithNoNullColumns.get(10))
    assert(setToNullAfterCreation.get(11) === rowWithNoNullColumns.get(11))
  }

}
