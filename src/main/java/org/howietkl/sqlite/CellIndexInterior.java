package org.howietkl.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class CellIndexInterior implements Cell {
  private static final Logger LOG = LoggerFactory.getLogger(CellIndexInterior.class);
  private int leftChildPageNumber;
  private int payloadSize;
  private ByteBuffer payload;

  public static CellIndexInterior get(ByteBuffer db) {
    CellIndexInterior cell = new CellIndexInterior();
    cell.leftChildPageNumber = db.getInt();

    cell.payloadSize = (int) Utils.getVarint(db);
    byte[] payloadBytes = new byte[cell.payloadSize];
    int pos = db.position();
    db.get(payloadBytes);
    cell.payload = ByteBuffer.wrap(payloadBytes);
    LOG.debug("leftChildPage#={} payloadOffset={} payloadSize={}", cell.leftChildPageNumber, pos, cell.payloadSize);

    return cell;
  }

  @Override
  public BTreeType getBTreeType() {
    return BTreeType.INTERIOR_INDEX;
  }

  public int getLeftChildPageNumber() {
    return leftChildPageNumber;
  }

  public ByteBuffer getPayload() {
    return payload;
  }
}
