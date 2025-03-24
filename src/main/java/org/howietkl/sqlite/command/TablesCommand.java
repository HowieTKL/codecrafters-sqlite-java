package org.howietkl.sqlite.command;

import org.howietkl.sqlite.CellPointerArray;
import org.howietkl.sqlite.CellTableLeaf;
import org.howietkl.sqlite.PageHeader;
import org.howietkl.sqlite.PayloadRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

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
      CellTableLeaf cell = CellTableLeaf.get(db);
      PayloadRecord record = PayloadRecord.get(cell.getPayloadRecord());
      tableNames[i] = (String) record.getRowValues().get(2);
    }

    System.out.println(String.join(" ", tableNames));
  }
}
