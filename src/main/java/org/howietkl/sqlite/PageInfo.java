package org.howietkl.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * @see <a href="https://www.sqlite.org/fileformat.html#b_tree_pages">B-tree Pages</a>
 */
public class PageInfo {
  private static final Logger LOG = LoggerFactory.getLogger(PageInfo.class);

  private final Type type;
  private final int firstFreeBlock;
  private final int cells;
  private final int cellContentStart;
  private final int fragmentedBytes;
  private long rightMostPointer = -1;

  public enum Type {
    INTERIOR_INDEX(2),
    INTERIOR_TABLE(5),
    LEAF_INDEX(10),
    LEAF_TABLE(13);
    public final int value;
    private static final Map<Integer, Type> lookup = new HashMap<>();
    static {
      for (Type type : Type.values()) {
        lookup.put(type.value, type);
      }
    }
    Type(int value) {
      this.value = value;
    }
    public static Type get(int value) {
      return lookup.get(value);
    }
  }

  private PageInfo(byte type, int firstFreeBlock, int cells, int cellContentStart, int fragmentedBytes) {
    this.type = Type.get(type);
    this.firstFreeBlock = firstFreeBlock;
    this.cells = cells;
    this.cellContentStart = cellContentStart == 0 ? 65536 : cellContentStart;
    this.fragmentedBytes = fragmentedBytes;
  }

  /**
   * Returns populated PageInfo, and advances ByteBuffer position accordingly.
   * Note that existence of RightMostPointer depends upon whether page is interior.
   * @param db should be set to appropriate position before calling this method
   */
  public static PageInfo get(ByteBuffer db) {
    PageInfo page = new PageInfo(
        db.get(),
        Short.toUnsignedInt(db.getShort()),
        Short.toUnsignedInt(db.getShort()),
        Short.toUnsignedInt(db.getShort()),
        Byte.toUnsignedInt(db.get()));

    switch (page.type) {
      case Type.INTERIOR_TABLE, Type.INTERIOR_INDEX -> {
        page.rightMostPointer = Integer.toUnsignedLong(db.getInt());
      }
    }

    LOG.info("PAGE type b-tree: {} [{}]", page.getType(), page.getType().value);
    LOG.info("PAGE first freeblock: {}", page.getFirstFreeBlock());
    LOG.info("PAGE cells: {}", page.getCells());
    LOG.info("PAGE cell content start: {}", page.getCellContentStart());
    LOG.info("PAGE fragmented bytes: {}", page.getFragmentedBytes());
    LOG.info("PAGE right most pointer: {}", page.getRightMostPointer());

    return page;
  }

  public Type getType() {
    return type;
  }

  public int getFirstFreeBlock() {
    return firstFreeBlock;
  }

  public int getCells() {
    return cells;
  }

  public int getCellContentStart() {
    return cellContentStart;
  }

  public int getFragmentedBytes() {
    return fragmentedBytes;
  }

  public boolean hasRightMostPointer() {
    return type == Type.INTERIOR_TABLE || type == Type.INTERIOR_INDEX;
  }

  public long getRightMostPointer() {
    return rightMostPointer;
  }
}
