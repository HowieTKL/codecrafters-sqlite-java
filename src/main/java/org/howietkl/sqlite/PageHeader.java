package org.howietkl.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * @see <a href="https://www.sqlite.org/fileformat.html#b_tree_pages">B-tree Pages</a>
 */
public class PageHeader {
  private static final Logger LOG = LoggerFactory.getLogger(PageHeader.class);

  private final int position;
  private final BTreeType type;
  private final int firstFreeBlock;
  private final int cells;
  private final int cellContentStart;
  private final int fragmentedBytes;
  private long rightMostPointer = -1;

  private PageHeader(int pos, byte type, int firstFreeBlock, int cells, int cellContentStart, int fragmentedBytes) {
    this.position = pos;
    this.type = BTreeType.get(type);
    this.firstFreeBlock = firstFreeBlock;
    this.cells = cells;
    this.cellContentStart = cellContentStart == 0 ? 65536 : cellContentStart;
    this.fragmentedBytes = fragmentedBytes;
  }

  /**
   * Returns populated object, and advances ByteBuffer position accordingly.
   * Note that existence of RightMostPointer depends upon whether page is interior.
   * @param db should be set to appropriate position before calling this method
   */
  public static PageHeader get(ByteBuffer db) {
    PageHeader page = new PageHeader(
        db.position(),
        db.get(), // type
        Short.toUnsignedInt(db.getShort()), // first freeblock
        Short.toUnsignedInt(db.getShort()), // #cells
        Short.toUnsignedInt(db.getShort()), // cell content area start
        Byte.toUnsignedInt(db.get())); // fragmented free bytes

    switch (page.type) {
      case BTreeType.INTERIOR_TABLE, BTreeType.INTERIOR_INDEX -> {
        page.rightMostPointer = Integer.toUnsignedLong(db.getInt());
      }
    }

    LOG.debug("pos={}", page.getPosition());
    LOG.debug("type b-tree: {} [{}]", page.getType(), page.getType().value);
    LOG.debug("first freeblock: {}", page.getFirstFreeBlock());
    LOG.debug("cells: {}", page.getCells());
    LOG.debug("cell content start: {}", page.getCellContentStart());
    LOG.debug("fragmented bytes: {}", page.getFragmentedBytes());
    if (page.hasRightMostPointer()) {
      LOG.debug("right most pointer: {}", page.getRightMostPointer());
    }
    return page;
  }

  public int getPosition() {
    return position;
  }

  public BTreeType getType() {
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
    return type == BTreeType.INTERIOR_TABLE || type == BTreeType.INTERIOR_INDEX;
  }

  public long getRightMostPointer() {
    return rightMostPointer;
  }
}
