package org.howietkl.sqlite.command;

import org.howietkl.sqlite.CellPointerArray;
import org.howietkl.sqlite.PageHeader;
import org.howietkl.sqlite.SerialType;
import org.howietkl.sqlite.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class TablesCommand implements Command {
  private static final Logger LOG = LoggerFactory.getLogger(TablesCommand.class);

  @Override
  public void execute(String[] args) throws Exception {
    tables(args[0]);
  }

  private void tables(String databaseFilePath) throws Exception {
    ByteBuffer db = ByteBuffer.wrap(Files.readAllBytes(Path.of(databaseFilePath)))
        .order(ByteOrder.BIG_ENDIAN)
        .asReadOnlyBuffer();

    DBInfoCommand.readTextEncoding(db);

    db.position(100);
    PageHeader pageHeader = PageHeader.get(db);
    CellPointerArray cellPointerArray = CellPointerArray.get(pageHeader, db);

    String[] tableNames = new String[pageHeader.getCells()];
    for (int i = 0; i < cellPointerArray.getOffsets().length; ++i) {
      db.position(cellPointerArray.getOffsets()[i]);
      int payloadSize = (int) Utils.getVarint(db);
      int rowId = (int) Utils.getVarint(db);

      byte[] payloadBytes = new byte[payloadSize];
      db.get(payloadBytes);
      ByteBuffer payload = ByteBuffer.wrap(payloadBytes);
      LOG.debug("offset={} rowId={} payloadSize={} payload={}",
          cellPointerArray.getOffsets()[i],
          rowId,
          payloadSize,
          new String(payload.array(), DBInfoCommand.TEXT_ENCODING));

      int prevPos = payload.position();
      int recordHeaderSize = (int) Utils.getVarint(payload);
      recordHeaderSize -= payload.position() - prevPos;
      prevPos = payload.position();
      List<SerialType> serialTypes = new ArrayList<>();
      while (recordHeaderSize > 0) {
        SerialType serialType = new SerialType((int) Utils.getVarint(payload));
        serialTypes.add(serialType);
        int size = payload.position() - prevPos;
        prevPos = payload.position();
        recordHeaderSize -= size;
      }
      LOG.debug("serialTypes {}", serialTypes);
      payload.position(payload.position()
          + serialTypes.get(0).getContentSize() // schema_type
          + serialTypes.get(1).getContentSize()); // schema_name

      byte[] tableNameBytes = new byte[serialTypes.get(2).getContentSize()];
      payload.get(tableNameBytes);
      String tableName = new String(tableNameBytes, DBInfoCommand.TEXT_ENCODING);
      tableNames[i] = tableName;
      LOG.debug("tbl_name={}", tableName);
    }

    System.out.println(String.join(" ", tableNames));
  }
}
