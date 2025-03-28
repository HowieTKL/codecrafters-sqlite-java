package org.howietkl.sqlite.command;

import org.howietkl.sqlite.BTreeType;
import org.howietkl.sqlite.CellPointerArray;
import org.howietkl.sqlite.CellTableInterior;
import org.howietkl.sqlite.CellTableLeaf;
import org.howietkl.sqlite.CreateTableParser;
import org.howietkl.sqlite.DBHeader;
import org.howietkl.sqlite.PageHeader;
import org.howietkl.sqlite.PayloadRecord;
import org.howietkl.sqlite.Row;
import org.howietkl.sqlite.SelectParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SQLCommand implements Command {
  private static final Logger LOG = LoggerFactory.getLogger(SQLCommand.class);

  @Override
  public void execute(String[] args) throws Exception {
    executeSQL(args[0], args[1]);
  }

  static void executeSQL(String databaseFilePath, String sql) throws IOException {
    SelectParser parser = SelectParser.parse(sql);

    String[] columns = parser.getColumns();
    if (columns.length == 1 && "COUNT(*)".equalsIgnoreCase(columns[0])) {
      countRows(databaseFilePath, parser.getTableName());
    } else {
      selectColumns(databaseFilePath, parser);
    }
  }

  private static void selectColumns(String databaseFilePath, SelectParser parser) throws IOException {
    String[] columns = parser.getColumns();
    ByteBuffer db = ByteBuffer.wrap(Files.readAllBytes(Path.of(databaseFilePath)))
        .order(ByteOrder.BIG_ENDIAN)
        .asReadOnlyBuffer();

    DBInfoCommand.readTextEncoding(db);

    DBHeader dbheader = DBHeader.get(db);
    db.position(100);
    PageHeader schemaPageHeader = PageHeader.get(db);

    PayloadRecord record = PayloadRecord.getPayloadRecord(db, schemaPageHeader, parser.getTableName());
    configureOffsetFromRootPage(record, dbheader.getPageSize(), db);

    String createTableSQL = (String) record.getRowValues().get(4); // schema - create sql
    CreateTableParser createTableParser = CreateTableParser.parse(createTableSQL);

    Map.Entry<String, String> filter = null;
    if (parser.getFilter().entrySet().iterator().hasNext()) {
      filter = parser.getFilter().entrySet().iterator().next();
    }

    PageHeader tablePageHeader = PageHeader.get(db);
    CellPointerArray cellPointerArray = CellPointerArray.get(tablePageHeader, db);
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < cellPointerArray.getOffsets().length; ++i) {
      db.position(cellPointerArray.getOffsets()[i]);
      if (tablePageHeader.getType() == BTreeType.LEAF_TABLE) {
        CellTableLeaf cell = CellTableLeaf.get(db);
        PayloadRecord tableRowRecord = PayloadRecord.get(cell.getPayloadRecord());

        Row row = new Row();
        row.setColumnMetadata(createTableParser);
        row.setValues(tableRowRecord.getRowValues());
        row.setRowId(cell.getRowId());
        rows.add(row);

        if (filter != null) {
          if (!filter.getValue().equals(row.getColumnValue(filter.getKey()))) {
            continue;
          }
        }
        String[] columnValues = new String[columns.length];
        for (int j = 0; j < columnValues.length; ++j) {
          columnValues[j] = (String) row.getColumnValue(columns[j]);
        }
        System.out.println(String.join("|", columnValues));
      } else if (tablePageHeader.getType() == BTreeType.INTERIOR_TABLE) {
        CellTableInterior cell = CellTableInterior.get(db);
      }
    }
  }

  private static int countRows(String databaseFilePath, String table) throws IOException {
    ByteBuffer db = ByteBuffer.wrap(Files.readAllBytes(Path.of(databaseFilePath)))
        .order(ByteOrder.BIG_ENDIAN)
        .asReadOnlyBuffer();

    DBInfoCommand.readTextEncoding(db);

    DBHeader dbheader = DBHeader.get(db);
    db.position(100);
    PageHeader schemaPageHeader = PageHeader.get(db);

    PayloadRecord record = PayloadRecord.getPayloadRecord(db, schemaPageHeader, table);
    configureOffsetFromRootPage(record, dbheader.getPageSize(), db);

    PageHeader tablePageHeader = PageHeader.get(db);
    System.out.println(tablePageHeader.getCells());
    return tablePageHeader.getCells();
  }

  private static void configureOffsetFromRootPage(PayloadRecord record, int pageSize, ByteBuffer db) {
    byte rootPage = (byte) record.getRowValues().get(3); // schema - root page
    int offset = (rootPage - 1) * pageSize;
    db.position(offset);
    LOG.debug("rootPage={} offset={}", rootPage, offset);
  }

}
