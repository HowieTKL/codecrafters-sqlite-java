package org.howietkl.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateTableParser {
  private static final Logger LOG = LoggerFactory.getLogger(CreateTableParser.class);
  private String tableName;
  private ColumnDef[] columns;

  public static class ColumnDef {
    public final String name;
    public final String type;

    private ColumnDef(String name, String type) {
      this.name = name;
      this.type = type;
    }

    @Override
    public String toString() {
      return String.format("%s:%s", name, type);
    }
  }

  public static CreateTableParser parse(String sql) {
    // e.g. create table apples(id integer, name text, description text)
    int firstSpace = sql.indexOf(' ');
    String expectCreate = sql.substring(0, firstSpace);
    int secondSpace = sql.indexOf(' ', firstSpace + 1);
    String expectTable = sql.substring(firstSpace + 1, secondSpace).trim();
    int firstBracket = sql.indexOf('(', secondSpace + 1);
    if (!"create".equalsIgnoreCase(expectCreate) || !"table".equalsIgnoreCase(expectTable)) {
      throw new UnsupportedOperationException("Failed to parse create table sql: " + sql);
    }
    CreateTableParser createTable = new CreateTableParser();
    createTable.tableName = sql.substring(secondSpace + 1, firstBracket).trim() ;

    int closingBracket = sql.indexOf(')', firstBracket + 1);
    String colDefsStr = sql.substring(firstBracket + 1, closingBracket).trim();
    String[] colDefs = colDefsStr.split(",");
    createTable.columns = new ColumnDef[colDefs.length];
    for (int i = 0; i < colDefs.length; i++) {
      String[] colDef = colDefs[i].trim().split(" ");
      ColumnDef col;
      if (colDef.length == 1) {
        col = new ColumnDef(colDef[0], null);
      } else {
        col = new ColumnDef(colDef[0], colDef[1]);

      }
      createTable.columns[i] = col;
    }

    LOG.debug("{} {} {} {}", expectCreate, expectTable, createTable.tableName, createTable.columns);

    return createTable;
  }

  public String getTableName() {
    return tableName;
  }

  public ColumnDef[] getColumns() {
    return columns;
  }

}
