package org.howietkl.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * @see <a href="https://www.sqlite.org/fileformat.html#b_tree_pages">B-tree Pages</a>
 */
public class PageHeader {
  private static final Logger LOG = LoggerFactory.getLogger(PageHeader.class);

  private final long position;
  private final BTreeType type;
  private final int firstFreeBlock;
  private final int cells;
  private final int cellContentStart;
  private final int fragmentedBytes;
  private long rightMostPointer = -1;

  private PageHeader(long pos, byte type, int firstFreeBlock, int cells, int cellContentStart, int fragmentedBytes) {
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
  public static PageHeader get(Database db) {
    PageHeader page = new PageHeader(
        db.position(),
        db.get(), // type
        db.getShort(), // first freeblock
        db.getShort(), // #cells
        db.getShort(), // cell content area start
        db.get()); // fragmented free bytes

    switch (page.type) {
      case BTreeType.INTERIOR_TABLE, BTreeType.INTERIOR_INDEX -> {
        page.rightMostPointer = Integer.toUnsignedLong(db.getInt());
      }
    }

    LOG.trace("pos={}", page.getPosition());
    LOG.trace("type b-tree: {} [{}]", page.getType(), page.getType().value);
    LOG.trace("first freeblock: {}", page.getFirstFreeBlock());
    LOG.trace("cells: {}", page.getCells());
    LOG.trace("cell content start: {}", page.getCellContentStart());
    LOG.trace("fragmented bytes: {}", page.getFragmentedBytes());
    if (page.hasRightMostPointer()) {
      LOG.trace("right most pointer: {}", page.getRightMostPointer());
    }
    return page;
  }

  public long getPosition() {
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
