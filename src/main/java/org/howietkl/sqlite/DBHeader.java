package org.howietkl.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBHeader {
  private static final Logger LOG = LoggerFactory.getLogger(DBHeader.class);
  private int pageSize;
  private int pageCount;

  private DBHeader() {
  }

  public static DBHeader get(Database db) {
    DBHeader dbHeader = new DBHeader();

    db.position(16); // Skip the first 16 bytes of the header
    dbHeader.pageSize = db.getShort();

    db.position(28);
    dbHeader.pageCount = db.getInt();

    return dbHeader;
  }

  public int getPageSize() {
    return pageSize;
  }

  public int getPageCount() {
    return pageCount;
  }



}
