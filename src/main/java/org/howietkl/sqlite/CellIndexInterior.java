package org.howietkl.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class CellIndexInterior implements Cell {
  private static final Logger LOG = LoggerFactory.getLogger(CellIndexInterior.class);
  private long leftChildPageNumber;
  private ByteBuffer payload;

  public static CellIndexInterior get(Database db) {
    CellIndexInterior cell = new CellIndexInterior();
    cell.leftChildPageNumber = Integer.toUnsignedLong(db.getInt());

    int payloadSize = (int) Utils.getVarint(db);
    byte[] payloadBytes = new byte[payloadSize];
    long pos = db.position();
    db.get(payloadBytes);
    cell.payload = ByteBuffer.wrap(payloadBytes);
    // LOG.debug("leftChildPage#={} payloadOffset={} payloadSize={}", cell.leftChildPageNumber, pos, payloadSize);

    return cell;
  }

  @Override
  public BTreeType getBTreeType() {
    return BTreeType.INTERIOR_INDEX;
  }

  public long getLeftChildPageNumber() {
    return leftChildPageNumber;
  }

  public ByteBuffer getPayload() {
    return payload;
  }
}
