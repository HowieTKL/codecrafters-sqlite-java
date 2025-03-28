package org.howietkl.sqlite;

import java.util.HashMap;
import java.util.Map;

public enum BTreeType {
  INTERIOR_INDEX(2),
  INTERIOR_TABLE(5),
  LEAF_INDEX(10),
  LEAF_TABLE(13);
  public final int value;
  private static final Map<Integer, BTreeType> lookup = new HashMap<>();

  static {
    for (BTreeType type : BTreeType.values()) {
      lookup.put(type.value, type);
    }
  }

  BTreeType(int value) {
    this.value = value;
  }

  public static BTreeType get(int value) {
    return lookup.get(value);
  }
}
