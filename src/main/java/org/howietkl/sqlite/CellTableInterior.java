package org.howietkl.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class CellTableInterior implements Cell {
  private static final Logger LOG = LoggerFactory.getLogger(CellTableInterior.class);
  private int leftChildPageNumber;
  private long rowId;

  public static CellTableInterior get(ByteBuffer db) {
    CellTableInterior cell = new CellTableInterior();

    cell.leftChildPageNumber = db.getInt();
    cell.rowId = Utils.getVarint(db);
    LOG.trace("leftChildPage#={} rowId={}", cell.leftChildPageNumber, cell.rowId);
    return cell;
  }

 @Override
  public BTreeType getBTreeType() {
    return BTreeType.INTERIOR_TABLE;
  }

  public int getLeftChildPageNumber() {
    return leftChildPageNumber;
  }

  public long getRowId() {
    return rowId;
  }
}
