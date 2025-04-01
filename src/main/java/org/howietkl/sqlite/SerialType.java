package org.howietkl.sqlite;

import java.util.HashMap;
import java.util.Map;

/**
 * Record format serial type.
 * @see <a href="">https://www.sqlite.org/fileformat.html#record_format</a>
 */
public class SerialType {
  private final static Map<Integer, SerialType> serialTypes = new HashMap<>();
  private final int type;

  private SerialType(int type) {
    this.type = type;
  }

  public static SerialType get(int type) {
    if (!serialTypes.containsKey(type)) {
      SerialType serialType = new SerialType(type);
      serialTypes.put(type, serialType);
    }
    return serialTypes.get(type);
  }

  public int getContentSize() {
    if (type >= 12 && type % 2 == 0) { // even
      return (type - 12) / 2;
    }
    if (type >= 13) { // odd, type % 2 == 1
      return (type - 13) / 2;
    }
    switch (type) {
      case 0, 8, 9 -> { return 0; } // null, 0, 1
      case 1 -> { return 1; } // 8-bit integer signed
      case 2 -> { return 2; } // 16-bit integer signed
      case 3 -> { return 3; } // 24-bit integer signed
      case 4 -> { return 4; } // 32-bit integer signed
      case 5 -> { return 6; } // 48-bit integer signed
      case 6, 7 -> { return 8; } // 64-bit integer signed, float
      default -> throw new IllegalStateException("Unexpected value: " + type);
    }
  }

  public int getType() {
    return type;
  }

  public boolean isBLOB() {
    return type >= 12 && type % 2 == 0;
  }

  public boolean isString() {
    return type >= 13 && type % 2 == 1;
  }

  public String toString() {
    return String.format("%s[%s]", type, getContentSize());
  }
}
