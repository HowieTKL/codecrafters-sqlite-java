package org.howietkl.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class Utils {
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  /**
   * Signed varint represented by 1-9 bytes.
   * @return 64-bit signed long
   */
  public static long getVarint(ByteBuffer buf) {
    long result = 0;
    int i = 0;
    byte current;
    boolean done = false;
    for (i = 0; i < 8; ++i) {
      current = buf.get();
      result = (result << 7) | (current & 0b01111111);
      if ((current & 0b10000000) == 0) {
        done = true;
        break;
      }
    }
    if (!done) {
      current = buf.get();
      result = (result << 7) | (current & 0b01111111);
      if ((current & 0b10000000) != 0) {
        // 9th byte can be signed
        result = result | 1L << 63;
      }
    }
    return result;
  }

  public static long getVarint(Database db) {
    long result = 0;
    int i = 0;
    byte current;
    boolean done = false;
    for (i = 0; i < 8; ++i) {
      current = db.get();
      result = (result << 7) | (current & 0b01111111);
      if ((current & 0b10000000) == 0) {
        done = true;
        break;
      }
    }
    if (!done) {
      current = db.get();
      result = (result << 7) | (current & 0b01111111);
      if ((current & 0b10000000) != 0) {
        // 9th byte can be signed
        result = result | 1L << 63;
      }
    }
    return result;
  }


  public static ByteBuffer getByteBuffer(String databaseFilePath) throws IOException {
    try (FileChannel channel = FileChannel.open(Path.of(databaseFilePath), StandardOpenOption.READ)) {
      LOG.debug("channel size={} properties={}", channel.size(), Runtime.getRuntime().maxMemory());
      return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size())
          .order(ByteOrder.BIG_ENDIAN)
          .asReadOnlyBuffer();
    }
  }
}
