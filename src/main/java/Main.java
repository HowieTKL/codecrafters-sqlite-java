import org.howietkl.sqlite.DBInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Main {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args){
    if (args.length < 2) {
      System.out.println("Missing <database path> and <command>");
      return;
    }

    String command = args[1];

    try {
      switch (command) {
        case ".dbinfo" -> new DBInfo().execute(args);
        default -> System.out.println("Missing or invalid command passed: " + command);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

}
