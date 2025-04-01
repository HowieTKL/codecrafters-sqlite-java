package org.howietkl.sqlite;

import org.howietkl.sqlite.command.SQLCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CellPointerArray {
  private static final Logger LOG = LoggerFactory.getLogger(CellPointerArray.class);
  private final List<Long> positions;

  private CellPointerArray(List<Long> positions) {
    this.positions = positions;
  }

  public List<Long> getPositions() {
    return positions;
  }

  public static CellPointerArray get(PageHeader pageHeader, Database db) {
    List<Long> positions = new ArrayList<>();
    // cell position is relative to the page position, except first page which is 0
    long baseOffset = pageHeader.getPosition() > 100 ? pageHeader.getPosition() : 0;
    for (int i = 0; i < pageHeader.getCells(); ++i) {
      int offset = db.getShort();
      long pos = offset + baseOffset;
      positions.add(pos);
      LOG.trace("{} pos={} offset={} base={}", i, pos, offset , baseOffset);
    }
    return new CellPointerArray(positions);
  }
}
