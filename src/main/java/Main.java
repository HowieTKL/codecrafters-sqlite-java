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

    String databaseFilePath = args[0];
    String command = args[1];

    switch (command) {
      case ".dbinfo" -> {
        try {
          FileInputStream databaseFile = new FileInputStream(new File(databaseFilePath));
          
          databaseFile.skip(16); // Skip the first 16 bytes of the header
          byte[] pageSizeBytes = new byte[2]; // The following 2 bytes are the page size
          databaseFile.read(pageSizeBytes);
          short pageSizeSigned = ByteBuffer.wrap(pageSizeBytes).getShort();
          int pageSize = Short.toUnsignedInt(pageSizeSigned);
          LOG.info("Logs from your program will appear here!");
          // Uncomment this block to pass the first stage
          System.out.println("database page size: " + pageSize);
        } catch (IOException e) {
          LOG.error(e.getMessage(), e);
        }
      }
      default -> System.out.println("Missing or invalid command passed: " + command);
    }
  }
}
