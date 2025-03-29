package org.howietkl.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CellPointerArray {
  private static final Logger LOG = LoggerFactory.getLogger(CellPointerArray.class);
  private final List<Integer> offsets;

  private CellPointerArray(List<Integer> offsets) {
    this.offsets = offsets;
  }

  public List<Integer> getOffsets() {
    return offsets;
  }

  public static CellPointerArray get(PageHeader pageHeader, ByteBuffer db) {
    List<Integer> offsets = new ArrayList<>();
    // cell position is relative to the page position, except first page which is 0
    int baseOffset = pageHeader.getPosition() > 100 ? pageHeader.getPosition() : 0;
    for (int i = 0; i < pageHeader.getCells(); ++i) {
      int offset = Short.toUnsignedInt(db.getShort()) + baseOffset;
      offsets.add(offset);
      LOG.trace("{} pos={}", i, offset);
    }
    return new CellPointerArray(offsets);
  }
}
