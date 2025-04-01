package org.howietkl.sqlite.command;

import org.howietkl.sqlite.CellPointerArray;
import org.howietkl.sqlite.CellTableLeaf;
import org.howietkl.sqlite.Database;
import org.howietkl.sqlite.PageHeader;
import org.howietkl.sqlite.PayloadRecord;
import org.howietkl.sqlite.SchemaHeaders;
import org.howietkl.sqlite.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.stream.Collectors;

public class TablesCommand implements Command {
  private static final Logger LOG = LoggerFactory.getLogger(TablesCommand.class);

  @Override
  public void execute(String[] args) throws Exception {
    tables(args[0]);
  }

  private void tables(String databaseFilePath) throws Exception {
    Database db = new Database(databaseFilePath);

    DBInfoCommand.readTextEncoding(db);

    db.position(100);
    PageHeader pageHeader = PageHeader.get(db);
    CellPointerArray cellPointerArray = CellPointerArray.get(pageHeader, db);

    LOG.debug("type name tbl_name rootpage sql");
    String tableNames = cellPointerArray.getOffsets().stream()
        .map(offset -> {
          db.position(offset);
          CellTableLeaf cell = CellTableLeaf.get(db);
          PayloadRecord record = PayloadRecord.get(cell.getPayloadRecord());
          LOG.debug("{}", record.getRowValues());
          return record;})
        .filter(record -> "table".equals(record.getRowValues().get(SchemaHeaders.type.pos())))
        .map(record -> (String) record.getRowValues().get(SchemaHeaders.tbl_name.pos()))
        .collect(Collectors.joining(" "));
    System.out.println(tableNames);
  }

}
