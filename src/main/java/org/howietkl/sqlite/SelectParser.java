package org.howietkl.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.AuthenticationNotSupportedException;
import java.lang.reflect.Array;

public class SelectParser {
  private static final Logger LOG = LoggerFactory.getLogger(SelectParser.class);
  private final String tableName;
  private final String[] columns;

  private SelectParser(String tableName, String[] columns) {
    this.tableName = tableName;
    this.columns = columns;
  }

  public static SelectParser parse(String sql) {
    String[] stmt = sql.split(" ");
    if (!"select".equalsIgnoreCase(stmt[0])) {
      throw new UnsupportedOperationException("Failed to parse SELECT: " + sql);
    }

    int fromIndex = - 1;
    int whereIndex = -1;
    for (int i = 1; i < stmt.length; i++) {
      if ("from".equalsIgnoreCase(stmt[i])) {
        fromIndex = i;
      }
      if ("where".equalsIgnoreCase(stmt[i])) {
        whereIndex = i;
      }
    }

    String[] columns = new String[fromIndex - 1];
    System.arraycopy(stmt, 1, columns, 0, columns.length);
    SelectParser parser = new SelectParser(
        stmt[fromIndex + 1],
        String.join("", columns).split(","));
    LOG.debug("SELECT {} FROM {}", parser.columns, parser.tableName);
    return parser;
  }

  public String getTableName() {
    return tableName;
  }

  public String[] getColumns() {
    return columns;
  }
}
