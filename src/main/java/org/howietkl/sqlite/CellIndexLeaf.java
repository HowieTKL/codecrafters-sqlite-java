package org.howietkl.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class CellIndexLeaf implements Cell {
  private static final Logger LOG = LoggerFactory.getLogger(CellIndexLeaf.class);
  private ByteBuffer payload;

  public static CellIndexLeaf get(Database db) {
    CellIndexLeaf cell = new CellIndexLeaf();

    int payloadSize = (int) Utils.getVarint(db);
    byte[] payloadBytes = new byte[payloadSize];
    long pos = db.position();
    db.get(payloadBytes);
    cell.payload = ByteBuffer.wrap(payloadBytes);
    LOG.trace("payloadOffset={} payloadSize={}", pos, payloadSize);
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
