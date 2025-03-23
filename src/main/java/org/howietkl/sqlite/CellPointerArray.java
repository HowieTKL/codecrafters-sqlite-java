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
    for (int i = 0; i < cellPointerArray.offsets.length; ++i) {
      cellPointerArray.offsets[i] = Short.toUnsignedInt(db.getShort());
      LOG.debug("{} offset={}", i, cellPointerArray.offsets[i]);
    }
    return cellPointerArray;
  }
}
