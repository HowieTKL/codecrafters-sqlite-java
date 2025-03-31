package org.howietkl.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class CellTableLeaf implements Cell {
  private static final Logger LOG = LoggerFactory.getLogger(CellTableLeaf.class);
  private long rowId;
  private ByteBuffer payload;

  public static CellTableLeaf get(ByteBuffer db) {
    CellTableLeaf cell = new CellTableLeaf();
    int payloadSize = (int) Utils.getVarint(db);
    cell.rowId = Utils.getVarint(db);
    byte[] payloadBytes = new byte[payloadSize];
    int pos = db.position();
    db.get(payloadBytes);
    cell.payload = ByteBuffer.wrap(payloadBytes);
    LOG.trace("payloadOffset={} rowId={} payloadSize={}", pos, cell.rowId, payloadSize);
    return cell;
  }

  public long getRowId() {
    return rowId;
  }

  public ByteBuffer getPayloadRecord() {
    return payload;
  }

  @Override
  public BTreeType getBTreeType() {
    return BTreeType.LEAF_TABLE;
  }
}
