package org.howietkl.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class CellIndexLeaf implements Cell {
  private static final Logger LOG = LoggerFactory.getLogger(CellIndexLeaf.class);
  private ByteBuffer payload;
  private int payloadSize;

  public static CellIndexLeaf get(ByteBuffer db) {
    CellIndexLeaf cell = new CellIndexLeaf();

    cell.payloadSize = (int) Utils.getVarint(db);
    byte[] payloadBytes = new byte[cell.payloadSize];
    int pos = db.position();
    db.get(payloadBytes);
    cell.payload = ByteBuffer.wrap(payloadBytes);
    LOG.debug("payloadOffset={} payloadSize={}", pos, cell.payloadSize);
    return cell;
  }

  @Override
  public BTreeType getBTreeType() {
    return BTreeType.LEAF_INDEX;
  }

  public ByteBuffer getPayload() {
    return payload;
  }
}
