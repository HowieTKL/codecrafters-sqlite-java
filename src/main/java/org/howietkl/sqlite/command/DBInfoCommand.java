package org.howietkl.sqlite.command;

import org.howietkl.sqlite.Database;
import org.howietkl.sqlite.PageHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class DBInfoCommand implements Command {
  private static final Logger LOG = LoggerFactory.getLogger(DBInfoCommand.class);
  public static Charset TEXT_ENCODING;

  private static void dbInfo(String databaseFilePath) throws Exception {
    Database db = new Database(databaseFilePath);

    db.position(16); // Skip the first 16 bytes of the header
    int pageSize = db.getShort();

    db.position(28);
    int pages = db.getInt();

    readTextEncoding(db);

    PageHeader pageHeader = PageHeader.get(db, 1, pageSize);

    System.out.println("database page size: " + pageSize);
    System.out.println("number of tables: " + pageHeader.getCells());
  }

  static void readTextEncoding(Database db) {
    db.position(56);
    int textEncodingValue = db.getInt();
    switch (textEncodingValue) {
      case 1 -> TEXT_ENCODING = StandardCharsets.UTF_8;
      case 2 -> TEXT_ENCODING = StandardCharsets.UTF_16LE;
      case 3 -> TEXT_ENCODING = StandardCharsets.UTF_16BE;
      default -> throw new IllegalStateException("Unexpected text encoding: " + textEncodingValue);
    }
    LOG.trace("Text encoding: {} [{}]", TEXT_ENCODING, textEncodingValue);
  }

  @Override
  public void execute(String[] args) throws Exception {
    dbInfo(args[0]);
  }

}
