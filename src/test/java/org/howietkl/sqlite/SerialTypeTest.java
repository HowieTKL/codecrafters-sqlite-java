package org.howietkl.sqlite;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SerialTypeTest {

  @Test
  void getContentSize() {

    assertEquals(5, new SerialType(23).getContentSize());
    assertEquals(7, new SerialType(27).getContentSize());
    assertEquals(1, new SerialType(1).getContentSize());
    assertEquals(93, new SerialType(199).getContentSize());
  }
}