package org.howietkl.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class CellPointerArray {
  private static final Logger LOG = LoggerFactory.getLogger(CellPointerArray.class);
  private final int[] offsets;

  private CellPointerArray(int[] offsets) {
    this.offsets = offsets;
  }

  public int[] getOffsets() {
    return offsets;
  }

  public static CellPointerArray get(PageHeader pageHeader, ByteBuffer db) {
    CellPointerArray cellPointerArray = new CellPointerArray(new int[pageHeader.getCells()]);
    // cell position is relative to the page position, except first page which is 0
    int baseOffset = pageHeader.getPosition() > 100 ? pageHeader.getPosition() : 0;
    for (int i = 0; i < cellPointerArray.offsets.length; ++i) {
      cellPointerArray.offsets[i] = Short.toUnsignedInt(db.getShort()) + baseOffset;
      LOG.debug("{} pos={}", i, cellPointerArray.offsets[i]);
    }
    return cellPointerArray;
  }
}
