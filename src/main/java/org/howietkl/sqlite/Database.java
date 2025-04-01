package org.howietkl.sqlite;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * Wrapper around {@link RandomAccessFile} to provide same signatures as {@link ByteBuffer}
 * in order to reduce refactoring. Running into limitations with int-based
 * {@link ByteBuffer#position(int)}.
 */
public class Database {

  private interface ByteBufferWrapper {
    long position();
    void position(long position);
    int getShort();
    int getInt();
    long getLong();
    void get(byte[] buf);
    byte get();
  }

  private static class RandomAccessFileByteBuffer implements ByteBufferWrapper {
    private final RandomAccessFile db;
    private RandomAccessFileByteBuffer(String dbFile) {
      try {
        db = new RandomAccessFile(dbFile, "r");
      } catch (FileNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    @Override
    public long position() {
      try {
        return db.getFilePointer();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    @Override
    public void position(long pos) {
      try {
        db.seek(pos);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    @Override
    public int getShort() {
      try {
        return db.readUnsignedShort();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    @Override
    public byte get() {
      try {
        return db.readByte();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    @Override
    public void get(byte[] payload) {
      try {
        db.read(payload);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    @Override
    public int getInt() {
      try {
        return db.readInt();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    @Override
    public long getLong() {
      try {
        return db.readLong();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class ByteBufferActual implements ByteBufferWrapper {
    private final ByteBuffer db;
    private ByteBufferActual(String dbFile) {
      try {
        this.db = Utils.getByteBuffer(dbFile);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public long position() {
      return db.position();
    }

    @Override
    public void position(long position) {
      db.position((int) position);
    }

    @Override
    public int getShort() {
      return Short.toUnsignedInt(db.getShort());
    }

    @Override
    public int getInt() {
      return db.getInt();
    }

    @Override
    public long getLong() {
      return db.getLong();
    }

    @Override
    public void get(byte[] buf) {
      db.get(buf);
    }

    @Override
    public byte get() {
      return db.get();
    }
  }


  private final ByteBufferWrapper db;
  public Database(String databaseFilename) throws IOException {
    //db = new RandomAccessFileByteBuffer(databaseFilename);
    db = new ByteBufferActual(databaseFilename);
  }

  public long position() {
      return db.position();
  }

  public void position(long pos) {
    db.position(pos);
  }

  public int getShort() {
      return db.getShort();
  }

  public byte get() {
    return db.get();
  }

  public void get(byte[] payload) {
    db.get(payload);
  }

  public int getInt() {
    return db.getInt();
  }

  public long getLong() {
    return db.getLong();
  }

}
