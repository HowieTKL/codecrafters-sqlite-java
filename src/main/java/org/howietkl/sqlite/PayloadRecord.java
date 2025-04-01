package org.howietkl.sqlite;

import org.howietkl.sqlite.command.DBInfoCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PayloadRecord {
  private static final Logger LOG = LoggerFactory.getLogger(PayloadRecord.class);
  private final List<SerialType> serialTypes = new ArrayList<>();
  private final List<Object> rowValues = new ArrayList<>();

  private PayloadRecord() {}

  public static PayloadRecord get(ByteBuffer payload) {
    PayloadRecord rec = new PayloadRecord();
    int prevPos = payload.position();
    int recordHeaderSize = (int) Utils.getVarint(payload);
    recordHeaderSize -= payload.position() - prevPos;
    prevPos = payload.position();

    while (recordHeaderSize > 0) {
      SerialType serialType = SerialType.get((int) Utils.getVarint(payload));
      rec.serialTypes.add(serialType);
      int size = payload.position() - prevPos;
      prevPos = payload.position();
      recordHeaderSize -= size;
    }

    for (SerialType serialType : rec.serialTypes) {
      if (serialType.getType() == 1) {
        rec.getRowValues().add(payload.get());
      } else if (serialType.getType() == 0) {
        rec.getRowValues().add(null);
      } else if (serialType.getType() == 8) {
        rec.getRowValues().add(0);
      } else if (serialType.getType() == 9) {
        rec.getRowValues().add(1);
      } else if (serialType.getType() == 2) {
        rec.getRowValues().add(payload.getShort());
      } else if (serialType.getType() == 3) {
        byte[] bytes = new byte[3];
        payload.get(bytes);
        int val = ((bytes[0] & 0xFF) << 16) |
            ((bytes[1] & 0xFF) << 8) |
            (bytes[2] & 0xFF);

        if ((val & 0x00800000) != 0) {
          val |= 0xFF000000;
        }
        rec.getRowValues().add(val);
      } else if (serialType.getType() == 4) {
        rec.getRowValues().add(payload.getInt());
      } else if (serialType.getType() == 6) {
        rec.getRowValues().add(payload.getLong());
      } else if (serialType.getType() == 7) {
        rec.getRowValues().add(payload.getDouble());
      } else if (serialType.isString()) {
        byte[] bytes = new byte[serialType.getContentSize()];
        payload.get(bytes);
        String str = new String(bytes, DBInfoCommand.TEXT_ENCODING);
        rec.getRowValues().add(str);
      } else if (serialType.isBLOB()) {
        byte[] bytes = new byte[serialType.getContentSize()];
        payload.get(bytes);
        rec.getRowValues().add(bytes);
      } else {
        throw new UnsupportedOperationException("Unsupported serial type: " + serialType);
      }
    }
    LOG.trace("rowValues={} serialTypes={}", rec.rowValues, rec.serialTypes);
    return rec;
  }

  public static PayloadRecord getTableRecord(Database db, PageHeader pageHeader, String table) {
    CellPointerArray cellPointerArray = CellPointerArray.get(pageHeader, db);
    for (long offset: cellPointerArray.getPositions()) {
      db.position(offset);
      CellTableLeaf cell = CellTableLeaf.get(db);
      PayloadRecord record = get(cell.getPayloadRecord());
      if ("table".equals(record.getRowValues().get(SchemaHeaders.type.pos()))
          && table.equals(record.getRowValues().get(SchemaHeaders.tbl_name.pos()))) {
        return record;
      }
    }
    return null;
}

  public List<SerialType> getSerialTypes() {
    return serialTypes;
  }

  public List<Object> getRowValues() {
    return rowValues;
  }
  
}
