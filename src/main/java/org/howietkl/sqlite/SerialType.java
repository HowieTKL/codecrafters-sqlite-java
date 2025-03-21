package org.howietkl.sqlite;

public class SerialType {
  public final int type;
  public SerialType(int type) {
    this.type = type;
  }

  public int getContentSize() {
    if (type >= 12 && type % 2 == 0) {
      return (type - 12) / 2;
    }
    if (type >= 13 && type % 2 == 1) {
      return (type - 13) / 2;
    }
    switch (type) {
      case 0 -> { return 0; }
      case 1 -> { return 1; }
      case 2 -> { return 2; }
      case 3 -> { return 3; }
      case 4 -> { return 4; }
      case 5 -> { return 6; }
      case 6 -> { return 8; }
      case 7 -> { return 8; }
      case 8 -> { return 0; }
      case 9 -> { return 0; }
      default -> throw new IllegalStateException("Unexpected value: " + type);
    }
  }

  public String toString() {
    return String.format("%s[%s]", type, getContentSize());
  }
}
