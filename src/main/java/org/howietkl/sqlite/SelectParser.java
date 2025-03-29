package org.howietkl.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SelectParser {
  private static final Logger LOG = LoggerFactory.getLogger(SelectParser.class);
  private final String tableName;
  private final List<String> columns;
  private final Map<String, String> filter = new HashMap<>();

  private SelectParser(String tableName, String[] columns) {
    this.tableName = tableName;
    this.columns = Arrays.asList(columns);
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
        stmt[fromIndex + 1], // table name
        String.join("", columns).split(",")); // columns

    if (whereIndex > 0) {
      String[] whereFilter = new String[stmt.length - whereIndex - 1];
      System.arraycopy(stmt, whereIndex + 1, whereFilter, 0, whereFilter.length);
      // join by " " for cases like: WHERE color = 'Light Green'
      whereFilter = String.join(" ", whereFilter).split("=");
      whereFilter = Arrays.stream(whereFilter).map(String::trim).toArray(String[]::new);
      if (whereFilter[1].contains("'")) {
        parser.filter.put(whereFilter[0],
            whereFilter[1].substring(1, whereFilter[1].length() - 1));
      }

      LOG.trace("whereFilter={}", Arrays.asList(whereFilter));
    }

    LOG.trace("SELECT {} FROM {} WHERE {}", parser.columns, parser.tableName, parser.filter);
    return parser;
  }

  public String getTableName() {
    return tableName;
  }

  public List<String> getColumns() {
    return columns;
  }

  public Map<String, String> getFilter() {
    return filter;
  }
}
