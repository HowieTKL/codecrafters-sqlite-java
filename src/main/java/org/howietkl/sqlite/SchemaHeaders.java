package org.howietkl.sqlite;

public enum SchemaHeaders {
  type,
  name,
  tbl_name,
  rootpage,
  sql;

  public int pos() {
    return ordinal();
  }
}
