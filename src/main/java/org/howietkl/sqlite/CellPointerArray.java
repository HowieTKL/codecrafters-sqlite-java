package org.howietkl.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CellPointerArray {
  private static final Logger LOG = LoggerFactory.getLogger(CellPointerArray.class);
  private final List<Long> offsets;

  private CellPointerArray(List<Long> offsets) {
    this.offsets = offsets;
  }

  public List<Long> getOffsets() {
    return offsets;
  }

  public static CellPointerArray get(PageHeader pageHeader, Database db) {
    List<Long> offsets = new ArrayList<>();
    // cell position is relative to the page position, except first page which is 0
    long baseOffset = pageHeader.getPosition() > 100 ? pageHeader.getPosition() : 0;
    for (int i = 0; i < pageHeader.getCells(); ++i) {
      long offset = db.getShort() + baseOffset;
      offsets.add(offset);
      LOG.trace("{} pos={}", i, offset);
    }
    return new CellPointerArray(offsets);
  }
}
