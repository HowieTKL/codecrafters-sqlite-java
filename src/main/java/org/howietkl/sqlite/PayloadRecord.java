package org.howietkl.sqlite;

import org.howietkl.sqlite.command.DBInfoCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PayloadRecord {
  private static final Logger LOG = LoggerFactory.getLogger(PayloadRecord.class);
  private final ByteBuffer payload;
  private final List<SerialType> serialTypes = new ArrayList<>();
  private final List<Object> rowValues = new ArrayList<>();
  
  private PayloadRecord(ByteBuffer payload) {
    this.payload = payload;
  }
  
  public static PayloadRecord get(ByteBuffer payload) {
    PayloadRecord rec = new PayloadRecord(payload);
    
    int prevPos = payload.position();
    int recordHeaderSize = (int) Utils.getVarint(payload);
    recordHeaderSize -= payload.position() - prevPos;
    prevPos = payload.position();
    while (recordHeaderSize > 0) {
      SerialType serialType = new SerialType((int) Utils.getVarint(payload));
      rec.serialTypes.add(serialType);
      int size = payload.position() - prevPos;
      prevPos = payload.position();
      recordHeaderSize -= size;
    }
    LOG.debug("serialTypes {}", rec.serialTypes);
    for (SerialType serialType : rec.serialTypes) {
      if (serialType.getType() == 1) {
        rec.getRowValues().add(payload.get());
      } else if (serialType.getType() == 0) {
        rec.getRowValues().add(null);
      } else if (serialType.getType() == 8) {
        rec.getRowValues().add(Integer.valueOf(0));
      } else if (serialType.getType() == 9) {
        rec.getRowValues().add(Integer.valueOf(1));
      } else if (serialType.getType() == 2) {
        rec.getRowValues().add(payload.getShort());
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
    LOG.debug("rowValues {}", rec.rowValues);
    return rec;
  }

  public static PayloadRecord getPayloadRecord(ByteBuffer db, PageHeader pageHeader, String table) {
    CellPointerArray cellPointerArray = CellPointerArray.get(pageHeader, db);
    for (int i = 0; i<cellPointerArray.getOffsets().length; ++i) {
      db.position(cellPointerArray.getOffsets()[i]);
      CellTableLeaf cell = CellTableLeaf.get(db);
      PayloadRecord record = get(cell.getPayloadRecord());
      String tableName = (String) record.getRowValues().get(2); // tbl_name
      if (table.equals(tableName)) {
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
