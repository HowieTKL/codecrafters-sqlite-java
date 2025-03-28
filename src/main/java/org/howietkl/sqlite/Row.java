package org.howietkl.sqlite;

import java.util.List;

public class Row {
  private CreateTableParser columnMetadata;
  private long rowId;
  private List<Object> values;

  public CreateTableParser getColumnMetadata() {
    return columnMetadata;
  }

  public void setColumnMetadata(CreateTableParser columnMetadata) {
    this.columnMetadata = columnMetadata;
  }

  public long getRowId() {
    return rowId;
  }

  public void setRowId(long rowId) {
    this.rowId = rowId;
  }

  public List<Object> getValues() {
    return values;
  }

  public void setValues(List<Object> values) {
    this.values = values;
  }

  public Object getColumnValue(String columnName) {
    var index = columnMetadata.getColumnIndexLookup().get(columnName);
    return values.get(index);
  }

}
