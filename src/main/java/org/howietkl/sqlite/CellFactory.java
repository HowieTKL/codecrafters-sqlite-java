package org.howietkl.sqlite;

import java.nio.ByteBuffer;

public class CellFactory {
    public Cell create(BTreeType type, final ByteBuffer db) {
      return switch (type) {
        case LEAF_TABLE -> CellTableLeaf.get(db);
        case INTERIOR_TABLE -> CellTableInterior.get(db);
        default -> throw new IllegalStateException("Unexpected value: " + type);
      };
    }
}
