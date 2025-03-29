package org.howietkl.sqlite;

import java.util.List;

public class Row {
  private CreateTableParser columnMetadata;
  private long rowId;
  private List<Object> values;

  public CreateTableParser getColumnMetadata() {
    return columnMetadata;
  }

  public Row setColumnMetadata(CreateTableParser columnMetadata) {
    this.columnMetadata = columnMetadata;
    return this;
  }

  public long getRowId() {
    return rowId;
  }

  public Row setRowId(long rowId) {
    this.rowId = rowId;
    return this;
  }

  public List<Object> getValues() {
    return values;
  }

  public Row setValues(List<Object> values) {
    this.values = values;
    return this;
  }

  public Object getColumnValue(String columnName) {
    var index = columnMetadata.getColumnIndexLookup().get(columnName);
    return values.get(index);
  }

}
