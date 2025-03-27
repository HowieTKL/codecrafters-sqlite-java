package org.howietkl.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class CellTableLeaf {
  private static final Logger LOG = LoggerFactory.getLogger(CellTableLeaf.class);

  private int payloadSize;
  private int rowId;
  private ByteBuffer payload;

  public static CellTableLeaf get(ByteBuffer db) {
    CellTableLeaf cell = new CellTableLeaf();
    cell.payloadSize = (int) Utils.getVarint(db);
    cell.rowId = (int) Utils.getVarint(db);
    byte[] payloadBytes = new byte[cell.payloadSize];
    int pos = db.position();
    db.get(payloadBytes);
    cell.payload = ByteBuffer.wrap(payloadBytes);
    LOG.debug("payloadOffset={} rowId={} payloadSize={}", pos, cell.rowId, cell.payloadSize);
    return cell;
  }

  public int getPayloadSize() {
    return payloadSize;
  }

  public int getRowId() {
    return rowId;
  }

  public ByteBuffer getPayloadRecord() {
    return payload;
  }
}
