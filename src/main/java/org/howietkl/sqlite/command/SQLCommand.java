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
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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

  private static void selectColumns(String databaseFilePath, SelectParser selectParser) throws IOException {
    long startTime = System.currentTimeMillis();
    Path path = Path.of(databaseFilePath);
    ByteBuffer db;
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      db = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size())
          .order(ByteOrder.BIG_ENDIAN)
          .asReadOnlyBuffer();
    }

    DBInfoCommand.readTextEncoding(db);

    DBHeader dbheader = DBHeader.get(db);
    db.position(100);
    PageHeader schemaPageHeader = PageHeader.get(db);

    PayloadRecord record = PayloadRecord.getPayloadRecord(db, schemaPageHeader, selectParser.getTableName());

    processPage((byte) record.getRowValues().get(3), db, selectParser, record, dbheader);

    LOG.info("Query time={}ms", System.currentTimeMillis() - startTime);
  }

  private static void processPage(long rootPage, ByteBuffer db, SelectParser parser, PayloadRecord schemaRecord, DBHeader dbheader) {
    configureOffsetFromRootPage(rootPage, dbheader.getPageSize(), db);
    PageHeader tablePageHeader = PageHeader.get(db);

    if (tablePageHeader.getType() == BTreeType.LEAF_TABLE) {
      final List<String> columns = parser.getColumns();
      final String createTableSQL = (String) schemaRecord.getRowValues().get(4); // schema - create sql
      final CreateTableParser createTableParser = CreateTableParser.parse(createTableSQL);
      final Map.Entry<String, String> filter = parser.getFilter().entrySet().iterator().hasNext()
          ? parser.getFilter().entrySet().iterator().next()
          : null;
      CellPointerArray cellPointerArray = CellPointerArray.get(tablePageHeader, db);
      // map offset -> row
      // with row we can filter WHERE clause
      // map row -> columns - for display
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
                .map(column -> "id".equals(column)
                    ? Long.toString(row.getRowId())
                    :(String) row.getColumnValue(column))
                .collect(Collectors.joining("|")))
          .forEach(System.out::println);

    } else if (tablePageHeader.getType() == BTreeType.INTERIOR_TABLE) {
      CellPointerArray cellPointerArray = CellPointerArray.get(tablePageHeader, db);
      if (tablePageHeader.hasRightMostPointer()) {
        processPage(tablePageHeader.getRightMostPointer(), db, parser, schemaRecord, dbheader);
      }
      cellPointerArray.getOffsets().forEach(offset -> {
        db.position(offset);
        CellTableInterior cell = CellTableInterior.get(db);
        processPage(cell.getLeftChildPageNumber(), db, parser, schemaRecord, dbheader);
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
    // schema - root page
    configureOffsetFromRootPage((byte) record.getRowValues().get(3), dbheader.getPageSize(), db);

    PageHeader tablePageHeader = PageHeader.get(db);
    System.out.println(tablePageHeader.getCells());
    return tablePageHeader.getCells();
  }

  private static void configureOffsetFromRootPage(long rootPage, int pageSize, ByteBuffer db) {
    long offset = (rootPage - 1) * pageSize;
    db.position((int)offset);
    LOG.trace("rootPage={} offset={}", rootPage, offset);
  }

}
