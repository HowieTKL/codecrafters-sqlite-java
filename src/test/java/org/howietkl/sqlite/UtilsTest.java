package org.howietkl.sqlite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;
class UtilsTest {

  @BeforeEach
  void setUp() {
  }

  @AfterEach
  void tearDown() {
  }

  @Test
  void getVarint() {
    byte[] val = new byte[] {0b00000001};
    ByteBuffer buf = ByteBuffer.wrap(val);
    assertEquals(1, Utils.getVarint(buf));

    val = new byte[] {0b01111111};
    buf = ByteBuffer.wrap(val);
    assertEquals(127, Utils.getVarint(buf));

    val = new byte[] {(byte) 0b10000001, 0b01000111};
    buf = ByteBuffer.wrap(val);
    assertEquals(199, Utils.getVarint(buf));

    val = new byte[] {(byte) 0b10110011, 0b00110011}; //
    buf = ByteBuffer.wrap(val);
    assertEquals(6579, Utils.getVarint(buf));

    val = new byte[] {(byte) 0b10000001, (byte) 0b10111111, (byte) 0b10000001, 0b00111111}; //
    buf = ByteBuffer.wrap(val);
    assertEquals(3129535, Utils.getVarint(buf));

  }

}