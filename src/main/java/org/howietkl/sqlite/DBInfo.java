package org.howietkl.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class DBInfo implements Command {
  private static final Logger LOG = LoggerFactory.getLogger(DBInfo.class);
  private static Charset TEXT_ENCODING;

  @Override
  public void execute(String[] args) throws Exception {
    dbInfo(args[0]);
  }

  private static void dbInfo(String databaseFilePath) {
    try {
      ByteBuffer db = ByteBuffer.wrap(Files.readAllBytes(Path.of(databaseFilePath)))
          .order(ByteOrder.BIG_ENDIAN)
          .asReadOnlyBuffer();

      db.position(16); // Skip the first 16 bytes of the header
      int pageSize = Short.toUnsignedInt(db.getShort());

      db.position(28);
      int pages = db.getInt();
      LOG.info("Pages: {}", pages);

      configureTextEncoding(db);

      db.position(100);
      PageInfo pageInfo = PageInfo.get(db);

      System.out.println("database page size: " + pageSize);
      System.out.println("number of tables: " + pageInfo.getCells());
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private static void configureTextEncoding(ByteBuffer db) {
    db.position(56);
    int textEncodingValue = db.getInt();
    switch (textEncodingValue) {
      case 1 -> TEXT_ENCODING = StandardCharsets.UTF_8;
      case 2 -> TEXT_ENCODING = StandardCharsets.UTF_16LE;
      case 3 -> TEXT_ENCODING = StandardCharsets.UTF_16BE;
      default -> throw new IllegalStateException("Unexpected text encoding: " + textEncodingValue);
    }
    LOG.info("Text encoding: {} [{}]", TEXT_ENCODING, textEncodingValue);
  }

}
