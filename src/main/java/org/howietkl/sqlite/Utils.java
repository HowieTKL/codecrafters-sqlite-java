package org.howietkl.sqlite;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class Utils {

  public static long getVarint(ByteBuffer buf) {
    byte b;
    if ((b = buf.get()) >= 0) {
      return b;
    }
    long result = (b & 0b01111111) << 7;
    if ((b = buf.get()) >= 0) {
      return result | (b & 0b01111111);
    } else {
      result |= (b & 0b01111111);
      result <<= 7;
      if ((b = buf.get()) >= 0) {
        return result | (b & 0b01111111);
      } else {
        result |= (b & 0b01111111);
        result <<= 7;
        if ((b = buf.get()) >= 0) {
          return result | (b & 0b01111111);
        } else {
          result |= (b & 0b01111111);
          result <<= 7;
          if ((b = buf.get()) >= 0) {
            return result | (b & 0b01111111);
          } else {
            result |= (b & 0b01111111);
            result <<= 7;
            if ((b = buf.get()) >= 0) {
              return result | (b & 0b01111111);
            } else {
              result |= (b & 0b01111111);
              result <<= 7;
              if ((b = buf.get()) >= 0) {
                return result | (b & 0b01111111);
              } else {
                result |= (b & 0b01111111);
                result <<= 7;
                if ((b = buf.get()) >= 0) {
                  return result | (b & 0b01111111);
                } else {
                  result |= (b & 0b01111111);
                  result <<= 7;
                  if ((b = buf.get()) >= 0) {
                    return result | (b & 0b01111111);
                  } else {
                    result |= (b & 0b01111111);
                    result <<= 7;
                    if ((b = buf.get()) >= 0) {
                      return result | (b & 0b01111111);
                    } else {
                      throw new UnsupportedOperationException();
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  public static ByteBuffer getByteBuffer(String databaseFilePath) throws IOException {
    try (FileChannel channel = FileChannel.open(Path.of(databaseFilePath), StandardOpenOption.READ)) {
      return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size())
          .order(ByteOrder.BIG_ENDIAN)
          .asReadOnlyBuffer();
    }
  }
}
