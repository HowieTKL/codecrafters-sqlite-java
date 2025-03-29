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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SQLCommand implements Command {
  private static final Logger LOG = LoggerFactory.getLogger(SQLCommand.class);

  @Override
  public void execute(String[] args) throws Exception {
    executeSQL(args[0], args[1]);
  }

  static void executeSQL(String databaseFilePath, String sql) throws IOException {
    SelectParser parser = SelectParser.parse(sql);

    List<String> columns = parser.getColumns();
    if (columns.size() == 1 && "COUNT(*)".equalsIgnoreCase(columns.get(0))) {
      countRows(databaseFilePath, parser.getTableName());
    } else {
      selectColumns(databaseFilePath, parser);
    }
  }

  private static void selectColumns(String databaseFilePath, SelectParser parser) throws IOException {
    List<String> columns = parser.getColumns();
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

    final Map.Entry<String, String> filter = parser.getFilter().entrySet().iterator().hasNext()
        ? parser.getFilter().entrySet().iterator().next()
        : null;

    PageHeader tablePageHeader = PageHeader.get(db);
    if (tablePageHeader.getType() == BTreeType.LEAF_TABLE) {
      CellPointerArray cellPointerArray = CellPointerArray.get(tablePageHeader, db);

      // map offset -> row
      // with row we can filter WHERE clause
      // and display columns with map
      cellPointerArray.getOffsets().stream()
          .map(offset -> {
            db.position(offset);
            CellTableLeaf cell = CellTableLeaf.get(db);
            PayloadRecord tableRowRecord = PayloadRecord.get(cell.getPayloadRecord());
            return new Row()
                .setColumnMetadata(createTableParser)
                .setValues(tableRowRecord.getRowValues())
                .setRowId(cell.getRowId());})
          .filter(row -> filter == null || filter.getValue().equals(row.getColumnValue(filter.getKey())))
          .map(row -> columns.stream()
                .map(column -> (String) row.getColumnValue(column))
                .collect(Collectors.joining("|")))
          .forEach(System.out::println);

    } else if (tablePageHeader.getType() == BTreeType.INTERIOR_TABLE) {
      CellPointerArray cellPointerArray = CellPointerArray.get(tablePageHeader, db);
      cellPointerArray.getOffsets().forEach(offset -> {
        db.position(offset);
        CellTableInterior cell = CellTableInterior.get(db);

      });
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
