package org.howietkl.sqlite.command;

import org.howietkl.sqlite.BTreeType;
import org.howietkl.sqlite.CellIndexInterior;
import org.howietkl.sqlite.CellIndexLeaf;
import org.howietkl.sqlite.CellPointerArray;
import org.howietkl.sqlite.CellTableInterior;
import org.howietkl.sqlite.CellTableLeaf;
import org.howietkl.sqlite.CreateTableParser;
import org.howietkl.sqlite.DBHeader;
import org.howietkl.sqlite.Database;
import org.howietkl.sqlite.PageHeader;
import org.howietkl.sqlite.PayloadRecord;
import org.howietkl.sqlite.Row;
import org.howietkl.sqlite.SchemaHeaders;
import org.howietkl.sqlite.SelectParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class SQLCommand implements Command {
  private static final Logger LOG = LoggerFactory.getLogger(SQLCommand.class);
  public static boolean isInteriorIndex = false;

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

    Database db = new Database(databaseFilePath);

    DBInfoCommand.readTextEncoding(db);

    DBHeader dbHeader = DBHeader.get(db);

    Object indexPage = findIndexPage(db);
    Set<Long> filteredIndex = new HashSet<>();
    if (indexPage != null) {
      processIndexPage(indexPage, db, dbHeader.getPageSize(), selectParser, filteredIndex);
    }
    LOG.debug("filteredIndex={}", filteredIndex);

    PageHeader schemaPageHeader = PageHeader.get(db, 1, dbHeader.getPageSize());

    PayloadRecord schemaRecordForTable = PayloadRecord.getTableRecord(db, schemaPageHeader, selectParser.getTableName());

    processPage(schemaRecordForTable.getRowValues().get(SchemaHeaders.rootpage.pos()),
        db,
        selectParser,
        CreateTableParser.parse((String) schemaRecordForTable.getRowValues().get(SchemaHeaders.sql.pos())),
        dbHeader.getPageSize(),
        filteredIndex);

    LOG.info("Query time={}ms", System.currentTimeMillis() - startTime);
  }

  private static Object findIndexPage(Database db) {
    DBHeader dbHeader = DBHeader.get(db);
    PageHeader schemaPageHeader = PageHeader.get(db, 1, dbHeader.getPageSize());
    CellPointerArray cellPointerArray = CellPointerArray.get(schemaPageHeader, db);

    for (long offset : cellPointerArray.getPositions()) {
      db.position(offset);
      CellTableLeaf cell = CellTableLeaf.get(db);
      PayloadRecord record = PayloadRecord.get(cell.getPayloadRecord());
      LOG.debug("{} {}", record.getRowValues(), record.getSerialTypes());
      if ("index".equals(record.getRowValues().get(SchemaHeaders.type.pos()))) {
        Object rootPage = record.getRowValues().get(SchemaHeaders.rootpage.pos());
        LOG.debug("index rootpage={}", rootPage);
        return rootPage;
      }
    }
    return null;
  }

  private static void processPage(Object rootPage, Database db, SelectParser selectParser, CreateTableParser createTableParser, int pageSize, Set<Long> indexes) {
    PageHeader tablePageHeader = PageHeader.get(db, rootPage, pageSize);

    switch (tablePageHeader.getType()) {
      case BTreeType.LEAF_TABLE -> {
        final List<String> columns = selectParser.getColumns();
        final Map.Entry<String, String> filter = selectParser.getFilter().entrySet().iterator().hasNext()
            ? selectParser.getFilter().entrySet().iterator().next()
            : null;
        CellPointerArray cellPointerArray = CellPointerArray.get(tablePageHeader, db);
        // map offset -> row
        // with row we can filter WHERE clause
        // finally map row -> columns for display
        cellPointerArray.getPositions().stream()
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
            .map(row -> {
              indexes.remove(row.getRowId());
              return columns.stream()
                  .map(column -> "id".equals(column)
                      ? Long.toString(row.getRowId())
                      : (String) row.getColumnValue(column))
                  .collect(Collectors.joining("|"));
            })
            .forEach(System.out::println);
      }
      case BTreeType.INTERIOR_TABLE -> {
        CellPointerArray cellPointerArray = CellPointerArray.get(tablePageHeader, db);
        AtomicReference<CellTableInterior> lastCell = new AtomicReference<>();
        cellPointerArray.getPositions().forEach(offset -> {
          db.position(offset);
          CellTableInterior cell = CellTableInterior.get(db);

          if (isLessThanOrEquals(cell.getRowId(), indexes)) {
            processPage(cell.getLeftChildPageNumber(), db, selectParser, createTableParser, pageSize, indexes);
          }
          lastCell.set(cell);
        });

        if (isGreaterThanOrEquals(lastCell.get().getRowId(), indexes)) {
          processPage(tablePageHeader.getRightMostPointer(), db, selectParser, createTableParser, pageSize, indexes);
        }
      }
    }
  }

  private static boolean isGreaterThanOrEquals(long rowId, Set<Long> indexes) {
    if (indexes.isEmpty()) {
      return true;
    }
    for (Long index: indexes) {
      if (index >= rowId) {
        return true;
      }
    }
    return false;
  }

  private static boolean isLessThanOrEquals(long rowId, Set<Long> indexes) {
    if (indexes.isEmpty()) {
      return true;
    }
    for (Long index: indexes) {
      if (index <= rowId) {
        return true;
      }
    }
    return false;
  }

  private static void processIndexPage(Object rootPage, Database db, int pageSize, SelectParser selectParser, Set<Long> rowIds) {
    final Map.Entry<String, String> filter = selectParser.getFilter().entrySet().iterator().hasNext()
        ? selectParser.getFilter().entrySet().iterator().next()
        : null;
    PageHeader indexPageHeader = PageHeader.get(db, rootPage, pageSize);

    switch (indexPageHeader.getType()) {
      case BTreeType.LEAF_INDEX -> {
        CellPointerArray cellPointerArray = CellPointerArray.get(indexPageHeader, db);
        cellPointerArray.getPositions().forEach(offset -> {
          db.position(offset);
          CellIndexLeaf cell = CellIndexLeaf.get(db);
          processIndex(cell.getPayload(), filter, offset, rowIds);
        });
      }
      case BTreeType.INTERIOR_INDEX -> {
        CellPointerArray cellPointerArray = CellPointerArray.get(indexPageHeader, db);
        cellPointerArray.getPositions().forEach(offset -> {
          db.position(offset);
          CellIndexInterior cell = CellIndexInterior.get(db);
          processIndexPage(cell.getLeftChildPageNumber(), db, pageSize, selectParser,rowIds);
          processIndex(cell.getPayload(), filter, offset, rowIds);
        });
        processIndexPage(indexPageHeader.getRightMostPointer(), db, pageSize, selectParser, rowIds);
      }
    }
  }

  private static void processIndex(ByteBuffer cell, Map.Entry<String, String> filter, Long offset, Set<Long> rowIds) {
    if (filter == null) {
      return;
    }
    PayloadRecord indexRowRecord = PayloadRecord.get(cell);
    Object indexed = indexRowRecord.getRowValues().get(0);
    if (filter.getValue().equals(indexed)) {
      Object rowIdObj = indexRowRecord.getRowValues().get(1);
      long rowId = switch (rowIdObj) {
        case Integer i -> i;
        case Short s -> s;
        case Byte b -> b;
        case Long l -> l;
        default -> throw new IllegalStateException("Unexpected value: " + rowIdObj);
      };
      rowIds.add(rowId);
    }
  }

  private static void countRows(String databaseFilePath, String table) throws IOException {
    Database db = new Database(databaseFilePath);

    DBInfoCommand.readTextEncoding(db);

    DBHeader dbHeader = DBHeader.get(db);
    PageHeader schemaPageHeader = PageHeader.get(db, 1, dbHeader.getPageSize());

    PayloadRecord record = PayloadRecord.getTableRecord(db, schemaPageHeader, table);
    Object rootPage = record != null ? record.getRowValues().get(SchemaHeaders.rootpage.pos()) : null;
    PageHeader tablePageHeader = PageHeader.get(db, rootPage, dbHeader.getPageSize());
    System.out.println(tablePageHeader.getCells());
  }

}
