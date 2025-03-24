package org.howietkl.sqlite.command;

import org.howietkl.sqlite.CellPointerArray;
import org.howietkl.sqlite.CellTableLeaf;
import org.howietkl.sqlite.DBHeader;
import org.howietkl.sqlite.PageHeader;
import org.howietkl.sqlite.PayloadRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

public class SQLCommand implements Command {
  private static final Logger LOG = LoggerFactory.getLogger(SQLCommand.class);

  @Override
  public void execute(String[] args) throws Exception {
    executeSQL(args[0], args[1]);
  }

  static void executeSQL(String databaseFilePath, String sql) throws IOException {
    String[] args = sql.split(" ");
    String table2Count = args[args.length - 1];

    countRows(databaseFilePath, table2Count);


  }

  private static int countRows(String databaseFilePath, String table2Count) throws IOException {
    ByteBuffer db = ByteBuffer.wrap(Files.readAllBytes(Path.of(databaseFilePath)))
        .order(ByteOrder.BIG_ENDIAN)
        .asReadOnlyBuffer();

    DBInfoCommand.readTextEncoding(db);

    DBHeader dbheader = DBHeader.get(db);
    db.position(100);
    PageHeader pageHeader = PageHeader.get(db);
    CellPointerArray cellPointerArray = CellPointerArray.get(pageHeader, db);

    PayloadRecord record = null;
    for (int i = 0; i < cellPointerArray.getOffsets().length; ++i) {
      db.position(cellPointerArray.getOffsets()[i]);
      CellTableLeaf cell = CellTableLeaf.get(db);
      record = PayloadRecord.get(cell.getPayloadRecord());
      String tableName = (String) record.getRowValues().get(2);
      if (table2Count.equals(tableName)) {
        break;
      }
    }

    byte rootPage = (byte) record.getRowValues().get(3);
    int offset = (rootPage - 1) * dbheader.getPageSize();
    LOG.debug("rootPage={} offset={}", rootPage, offset);
    db.position(offset);
    pageHeader = PageHeader.get(db);

    System.out.println(pageHeader.getCells());
    return pageHeader.getCells();
  }
}
