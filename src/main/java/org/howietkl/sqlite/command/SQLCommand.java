package org.howietkl.sqlite.command;

import org.howietkl.sqlite.BTreeType;
import org.howietkl.sqlite.CellIndexInterior;
import org.howietkl.sqlite.CellIndexLeaf;
import org.howietkl.sqlite.CellPointerArray;
import org.howietkl.sqlite.CellTableInterior;
import org.howietkl.sqlite.CellTableLeaf;
import org.howietkl.sqlite.CreateTableParser;
import org.howietkl.sqlite.DBHeader;
import org.howietkl.sqlite.PageHeader;
import org.howietkl.sqlite.PayloadRecord;
import org.howietkl.sqlite.Row;
import org.howietkl.sqlite.SelectParser;
import org.howietkl.sqlite.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
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
    if (columns.size() == 1 && "COUNT(*)".equalsIgnoreCase(columns.getFirst())) {
      countRows(databaseFilePath, parser.getTableName());
    } else {
      selectColumns(databaseFilePath, parser);
    }
  }

  private static void selectColumns(String databaseFilePath, SelectParser selectParser) throws IOException {
    long startTime = System.currentTimeMillis();

    ByteBuffer db = Utils.getByteBuffer(databaseFilePath);

    DBInfoCommand.readTextEncoding(db);

    DBHeader dbHeader = DBHeader.get(db);

    long indexPage = findIndexPage(db);
    if (indexPage > 0) {
      processIndexPage(indexPage, db, dbHeader.getPageSize());
    }

    db.position(100);
    PageHeader schemaPageHeader = PageHeader.get(db);

    PayloadRecord schemaRecordForTable = PayloadRecord.getPayloadRecord(db, schemaPageHeader, selectParser.getTableName());

    processPage((byte) schemaRecordForTable.getRowValues().get(3), // root page
        db,
        selectParser,
        CreateTableParser.parse((String) schemaRecordForTable.getRowValues().get(4)), // create table sql
        dbHeader.getPageSize());

    LOG.info("Query time={}ms", System.currentTimeMillis() - startTime);
  }

  private static long findIndexPage(ByteBuffer db) {
    db.position(100);
    PageHeader schemaPageHeader = PageHeader.get(db);
    CellPointerArray cellPointerArray = CellPointerArray.get(schemaPageHeader, db);

    for (int offset : cellPointerArray.getOffsets()) {
      db.position(offset);
      CellTableLeaf cell = CellTableLeaf.get(db);
      PayloadRecord record = PayloadRecord.get(cell.getPayloadRecord());
      if ("index".equals(record.getRowValues().get(0))) {
        LOG.debug("index rootpage={}", record.getRowValues().get(3));
        Object rootPage = record.getRowValues().get(3);
        return switch (rootPage) {
          case Byte b -> b;
          case Short s -> s;
          case Long l -> l;
          default -> (int) rootPage;
        };
      }
    }
    return -1;
  }

  private static void processPage(long rootPage, ByteBuffer db, SelectParser selectParser, CreateTableParser createTableParser, int pageSize) {
    configureOffsetFromRootPage(rootPage, pageSize, db);
    PageHeader tablePageHeader = PageHeader.get(db);

    switch (tablePageHeader.getType()) {
      case BTreeType.LEAF_TABLE -> {
        final List<String> columns = selectParser.getColumns();
        final Map.Entry<String, String> filter = selectParser.getFilter().entrySet().iterator().hasNext()
            ? selectParser.getFilter().entrySet().iterator().next()
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
                  .setRowId(cell.getRowId());
            })
            .filter(row -> filter == null || filter.getValue().equals(row.getColumnValue(filter.getKey())))
            .map(row -> columns.stream()
                .map(column -> "id".equals(column)
                    ? Long.toString(row.getRowId())
                    : (String) row.getColumnValue(column))
                .collect(Collectors.joining("|")))
            .forEach(System.out::println);
      }
      case BTreeType.INTERIOR_TABLE -> {
        CellPointerArray cellPointerArray = CellPointerArray.get(tablePageHeader, db);
        if (tablePageHeader.hasRightMostPointer()) {
          processPage(tablePageHeader.getRightMostPointer(), db, selectParser, createTableParser, pageSize);
        }
        cellPointerArray.getOffsets().forEach(offset -> {
          db.position(offset);
          CellTableInterior cell = CellTableInterior.get(db);
          processPage(cell.getLeftChildPageNumber(), db, selectParser, createTableParser, pageSize);
        });
      }
    }
  }

  private static void processIndexPage(long rootPage, ByteBuffer db, int pageSize) {
    configureOffsetFromRootPage(rootPage, pageSize, db);
    PageHeader indexPageHeader = PageHeader.get(db);

    if (indexPageHeader.hasRightMostPointer()) {
      processIndexPage(indexPageHeader.getRightMostPointer(), db, pageSize);
    }

    switch (indexPageHeader.getType()) {
      case BTreeType.LEAF_INDEX -> {
        CellPointerArray cellPointerArray = CellPointerArray.get(indexPageHeader, db);
        cellPointerArray.getOffsets().forEach(offset -> {
          db.position(offset);
          CellIndexLeaf cell = CellIndexLeaf.get(db);

        });
      }
      case BTreeType.INTERIOR_INDEX -> {
        CellPointerArray cellPointerArray = CellPointerArray.get(indexPageHeader, db);
        cellPointerArray.getOffsets().forEach(offset -> {
          db.position(offset);
          CellIndexInterior cell = CellIndexInterior.get(db);
          // processIndexPage(cell.getLeftChildPageNumber(), db, pageSize);
        });
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
