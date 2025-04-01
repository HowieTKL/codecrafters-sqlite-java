package org.howietkl.sqlite;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SerialTypeTest {

  @Test
  void getContentSize() {
    assertEquals(5, SerialType.get(23).getContentSize());
    assertEquals(7, SerialType.get(27).getContentSize());
    assertEquals(1, SerialType.get(1).getContentSize());
    assertEquals(2, SerialType.get(2).getContentSize());
    assertEquals(3, SerialType.get(3).getContentSize());
    assertEquals(93, SerialType.get(199).getContentSize());
  }
}