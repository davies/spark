package org.apache.spark.sql.execution;

import scala.collection.Iterator;

import org.apache.spark.sql.catalyst.InternalRow;

public class BufferedIterator2 {
  protected InternalRow currentRow;
  protected Iterator<InternalRow> input;

  public boolean hasNext() {
    if (currentRow == null) {
      processNext();
    }
    return currentRow != null;
  }

  public InternalRow next() {
    InternalRow r = currentRow;
    currentRow = null;
    return r;
  }

  public void process(Iterator<InternalRow> iter) {
    input = iter;
  }

  protected void processNext() {
    if (input.hasNext()) {
      currentRow = input.next();
    }
  }
}
