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

package org.apache.spark.util.collection.unsafe.sort;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;

/**
 * External sorter based on {@link UnsafeInMemorySorter}.
 */
public final class UnsafeExternalSorter extends MemoryConsumer {

  private final Logger logger = LoggerFactory.getLogger(UnsafeExternalSorter.class);

  private final PrefixComparator prefixComparator;
  private final RecordComparator recordComparator;
  private final TaskMemoryManager taskMemoryManager;
  private final BlockManager blockManager;
  private final TaskContext taskContext;
  private ShuffleWriteMetrics writeMetrics;

  /** The buffer size to use when writing spills using DiskBlockObjectWriter */
  private final int fileBufferSizeBytes;

  /**
   * Memory pages that hold the records being sorted. The pages in this list are freed when
   * spilling, although in principle we could recycle these pages across spills (on the other hand,
   * this might not be necessary if we maintained a pool of re-usable pages in the TaskMemoryManager
   * itself).
   */
  private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<>();

  private final LinkedList<UnsafeSorterSpillWriter> spillWriters = new LinkedList<>();

  // These variables are reset after spilling:
  @Nullable private UnsafeInMemorySorter inMemSorter;
  // The acquired memory for in-memory sorter
  private long acquiredMem = 0L;

  private MemoryBlock currentPage = null;
  private long pageCursor = -1;
  private long peakMemoryUsedBytes = 0;
  private SpillableIterator readingIterator = null;

  public static UnsafeExternalSorter createWithExistingInMemorySorter(
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      TaskContext taskContext,
      RecordComparator recordComparator,
      PrefixComparator prefixComparator,
      int initialSize,
      long pageSizeBytes,
      UnsafeInMemorySorter inMemorySorter) throws IOException {
    return new UnsafeExternalSorter(taskMemoryManager, blockManager,
      taskContext, recordComparator, prefixComparator, initialSize, pageSizeBytes, inMemorySorter);
  }

  public static UnsafeExternalSorter create(
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      TaskContext taskContext,
      RecordComparator recordComparator,
      PrefixComparator prefixComparator,
      int initialSize,
      long pageSizeBytes) throws IOException {
    return new UnsafeExternalSorter(taskMemoryManager, blockManager,
      taskContext, recordComparator, prefixComparator, initialSize, pageSizeBytes, null);
  }

  private UnsafeExternalSorter(
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      TaskContext taskContext,
      RecordComparator recordComparator,
      PrefixComparator prefixComparator,
      int initialSize,
      long pageSizeBytes,
      @Nullable UnsafeInMemorySorter existingInMemorySorter) throws IOException {
    super(taskMemoryManager, pageSizeBytes);
    this.taskMemoryManager = taskMemoryManager;
    this.blockManager = blockManager;
    this.taskContext = taskContext;
    this.recordComparator = recordComparator;
    this.prefixComparator = prefixComparator;
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility for units
    // this.fileBufferSizeBytes = (int) conf.getSizeAsKb("spark.shuffle.file.buffer", "32k") * 1024;
    this.fileBufferSizeBytes = 32 * 1024;
    // TODO: metrics tracking + integration with shuffle write metrics
    // need to connect the write metrics to task metrics so we count the spill IO somewhere.
    this.writeMetrics = new ShuffleWriteMetrics();

    if (existingInMemorySorter == null) {
      this.inMemSorter =
        new UnsafeInMemorySorter(taskMemoryManager, recordComparator, prefixComparator, initialSize);
      acquireMemory(inMemSorter.getMemoryUsage());
      acquiredMem = inMemSorter.getMemoryUsage();
    } else {
      acquiredMem = 0;
      this.inMemSorter = existingInMemorySorter;
    }

    // Register a cleanup task with TaskContext to ensure that memory is guaranteed to be freed at
    // the end of the task. This is necessary to avoid memory leaks in when the downstream operator
    // does not fully consume the sorter's output (e.g. sort followed by limit).
    taskContext.addOnCompleteCallback(new AbstractFunction0<BoxedUnit>() {
      @Override
      public BoxedUnit apply() {
        cleanupResources();
        return null;
      }
    });
  }



  /**
   * Marks the current page as no-more-space-available, and as a result, either allocate a
   * new page or spill when we see the next record.
   */
  @VisibleForTesting
  public void closeCurrentPage() {
    if (currentPage != null) {
      pageCursor = currentPage.getBaseOffset() + currentPage.size();
    }
  }

  /**
   * Sort and spill the current records in response to memory pressure.
   */
  @Override
  public long spill(long size) throws IOException {
    assert(inMemSorter != null);
    logger.info("Thread {} spilling sort data of {} to disk ({} {} so far)",
      Thread.currentThread().getId(),
      Utils.bytesToString(getMemoryUsage()),
      spillWriters.size(),
      spillWriters.size() > 1 ? " times" : " time");

    if (readingIterator != null) {
      return readingIterator.spill();
    }

    // We only write out contents of the inMemSorter if it is not empty.
    if (inMemSorter.numRecords() > 0) {
      final UnsafeSorterSpillWriter spillWriter =
        new UnsafeSorterSpillWriter(blockManager, fileBufferSizeBytes, writeMetrics,
          inMemSorter.numRecords());
      spillWriters.add(spillWriter);
      final UnsafeSorterIterator sortedRecords = inMemSorter.getSortedIterator();
      while (sortedRecords.hasNext()) {
        sortedRecords.loadNext();
        final Object baseObject = sortedRecords.getBaseObject();
        final long baseOffset = sortedRecords.getBaseOffset();
        final int recordLength = sortedRecords.getRecordLength();
        spillWriter.write(baseObject, baseOffset, recordLength, sortedRecords.getKeyPrefix());
      }
      spillWriter.close();

      inMemSorter.reset();
    }

    final long spillSize = freeMemory();
    // Note that this is more-or-less going to be a multiple of the page size, so wasted space in
    // pages will currently be counted as memory spilled even though that space isn't actually
    // written to disk. This also counts the space needed to store the sorter's pointer array.
    taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);

    return spillSize;
  }

  /**
   * Return the total memory usage of this sorter, including the data pages and the sorter's pointer
   * array.
   */
  private long getMemoryUsage() {
    long totalPageSize = 0;
    for (MemoryBlock page : allocatedPages) {
      totalPageSize += page.size();
    }
    return ((inMemSorter == null) ? 0 : inMemSorter.getMemoryUsage()) + totalPageSize;
  }

  private void updatePeakMemoryUsed() {
    long mem = getMemoryUsage();
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem;
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  @VisibleForTesting
  public int getNumberOfAllocatedPages() {
    return allocatedPages.size();
  }

  /**
   * Free this sorter's data pages.
   *
   * @return the number of bytes freed.
   */
  private long freeMemory() {
    updatePeakMemoryUsed();
    long memoryFreed = 0;
    for (MemoryBlock block : allocatedPages) {
      freePage(block);
      memoryFreed += block.size();
    }
    allocatedPages.clear();
    currentPage = null;
    pageCursor = 0;
    return memoryFreed;
  }

  /**
   * Deletes any spill files created by this sorter.
   */
  private void deleteSpillFiles() {
    for (UnsafeSorterSpillWriter spill : spillWriters) {
      File file = spill.getFile();
      if (file != null && file.exists()) {
        if (!file.delete()) {
          logger.error("Was unable to delete spill file {}", file.getAbsolutePath());
        }
      }
    }
  }

  /**
   * Frees this sorter's in-memory data structures and cleans up its spill files.
   */
  public void cleanupResources() {
    deleteSpillFiles();
    freeMemory();
    if (inMemSorter != null) {
      inMemSorter = null;
      releaseMemory(acquiredMem);
      acquiredMem = 0;
    }
  }

  /**
   * Checks whether there is enough space to insert an additional record in to the sort pointer
   * array and grows the array if additional space is required. If the required space cannot be
   * obtained, then the in-memory data will be spilled to disk.
   */
  private void growPointerArrayIfNecessary() throws IOException {
    assert(inMemSorter != null);
    if (!inMemSorter.hasSpaceForAnotherRecord()) {
      // assume that the memory of array will be doubled
      long needed = inMemSorter.getMemoryUsage();
      acquireMemory(needed);  // could trigger spilling
      if (inMemSorter.hasSpaceForAnotherRecord()) {
        releaseMemory(needed);
      } else {
        acquiredMem += needed;
        inMemSorter.expandPointerArray();
      }
    }
  }

  /**
   * Allocates more memory in order to insert an additional record. This will request additional
   * memory from the memory manager and spill if the requested memory can not be obtained.
   *
   * @param required the required space in the data page, in bytes, including space for storing
   *                      the record size. This must be less than or equal to the page size (records
   *                      that exceed the page size are handled via a different code path which uses
   *                      special overflow pages).
   */
  private void acquireNewPageIfNecessary(int required) throws IOException {
    if (currentPage == null ||
      pageCursor + required > currentPage.getBaseOffset() + currentPage.size()) {
      // TODO: try to find space on previous pages
      currentPage = allocatePage(required);
      pageCursor = currentPage.getBaseOffset();
      allocatedPages.add(currentPage);
    }
  }

  /**
   * Write a record to the sorter.
   */
  public void insertRecord(Object recordBase, long recordOffset, int length, long prefix)
    throws IOException {

    growPointerArrayIfNecessary();
    // Need 4 bytes to store the record length.
    final int required = length + 4;
    acquireNewPageIfNecessary(required);

    final Object base = currentPage.getBaseObject();
    final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
    Platform.putInt(base, pageCursor, length);
    pageCursor += 4;
    Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
    pageCursor += length;
    assert(inMemSorter != null);
    inMemSorter.insertRecord(recordAddress, prefix);
  }

  /**
   * Write a key-value record to the sorter. The key and value will be put together in-memory,
   * using the following format:
   *
   * record length (4 bytes), key length (4 bytes), key data, value data
   *
   * record length = key length + value length + 4
   */
  public void insertKVRecord(Object keyBase, long keyOffset, int keyLen,
      Object valueBase, long valueOffset, int valueLen, long prefix) throws IOException {

    growPointerArrayIfNecessary();
    final int required = keyLen + valueLen + 4 + 4;
    acquireNewPageIfNecessary(required);

    final Object base = currentPage.getBaseObject();
    final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
    Platform.putInt(base, pageCursor, keyLen + valueLen + 4);
    pageCursor += 4;
    Platform.putInt(base, pageCursor, keyLen);
    pageCursor += 4;
    Platform.copyMemory(keyBase, keyOffset, base, pageCursor, keyLen);
    pageCursor += keyLen;
    Platform.copyMemory(valueBase, valueOffset, base, pageCursor, valueLen);
    pageCursor += valueLen;

    assert(inMemSorter != null);
    inMemSorter.insertRecord(recordAddress, prefix);
  }

  /**
   * Returns a sorted iterator. It is the caller's responsibility to call `cleanupResources()`
   * after consuming this iterator.
   */
  public UnsafeSorterIterator getSortedIterator() throws IOException {
    assert(inMemSorter != null);
    readingIterator = new SpillableIterator(inMemSorter.getSortedIterator());
    int numIteratorsToMerge = spillWriters.size() + (readingIterator.hasNext() ? 1 : 0);
    if (spillWriters.isEmpty()) {
      return readingIterator;
    } else {
      final UnsafeSorterSpillMerger spillMerger =
        new UnsafeSorterSpillMerger(recordComparator, prefixComparator, numIteratorsToMerge);
      for (UnsafeSorterSpillWriter spillWriter : spillWriters) {
        spillMerger.addSpillIfNotEmpty(spillWriter.getReader(blockManager));
      }
      spillWriters.clear();
      spillMerger.addSpillIfNotEmpty(readingIterator);

      return spillMerger.getSortedIterator();
    }
  }

  class SpillableIterator extends UnsafeSorterIterator {
    private Object baseObject = null;
    private long baseOffset = 0;
    private long keyPrefix = 0;
    private int recordLength = 0;

    private boolean cached = false;
    private MemoryBlock lastPage = null;

    private UnsafeSorterIterator upstream;

    public SpillableIterator(UnsafeInMemorySorter.SortedIterator inMemIterator) {
      this.upstream = inMemIterator;
    }

    public long spill() throws IOException {
      synchronized (this) {
        if (!(upstream instanceof UnsafeInMemorySorter.SortedIterator && upstream.hasNext())) {
          return 0L;
        }

        UnsafeInMemorySorter.SortedIterator inMemIterator =
          (UnsafeInMemorySorter.SortedIterator) upstream;

        // backup the current record
        baseObject = inMemIterator.getBaseObject();
        baseOffset = inMemIterator.getBaseOffset();
        keyPrefix = inMemIterator.getKeyPrefix();
        recordLength = inMemIterator.getRecordLength();

        final UnsafeSorterSpillWriter spillWriter =
          new UnsafeSorterSpillWriter(blockManager, fileBufferSizeBytes, writeMetrics,
            inMemIterator.numRecordsLeft());
        while (inMemIterator.hasNext()) {
          inMemIterator.loadNext();
          final Object baseObject = inMemIterator.getBaseObject();
          final long baseOffset = inMemIterator.getBaseOffset();
          final int recordLength = inMemIterator.getRecordLength();
          spillWriter.write(baseObject, baseOffset, recordLength, inMemIterator.getKeyPrefix());
        }
        spillWriter.close();
        upstream = spillWriter.getReader(blockManager);

        cached = true;
        // release the pages except the one that is used
        long released = 0L;
        for (MemoryBlock block : allocatedPages) {
          if (block.getBaseObject() != baseObject) {
            freePage(block);
            released += block.size();
          } else {
            lastPage = block;
          }
        }
        allocatedPages.clear();
        return released;
      }
    }

    @Override
    public boolean hasNext() {
      synchronized (this) {
        return upstream.hasNext();
      }
    }

    @Override
    public void loadNext() throws IOException {
      synchronized (this) {
        if (cached) {
          // Just consumed the last record from in memory iterator
          if (lastPage != null) {
            freePage(lastPage);
            lastPage = null;
          }
          cached = false;
        }
        upstream.loadNext();
      }
    }

    @Override
    public Object getBaseObject() {
      synchronized (this) {
        if (cached) {
          return baseObject;
        }
        return upstream.getBaseObject();
      }
    }

    @Override
    public long getBaseOffset() {
      synchronized (this) {
        if (cached) {
          return baseOffset;
        }
        return upstream.getBaseOffset();
      }
    }

    @Override
    public int getRecordLength() {
      synchronized (this) {
        if (cached) {
          return recordLength;
        }
        return upstream.getRecordLength();
      }
    }

    @Override
    public long getKeyPrefix() {
      synchronized (this) {
        if (cached) {
          return keyPrefix;
        }
        return upstream.getKeyPrefix();
      }
    }
  }
}
