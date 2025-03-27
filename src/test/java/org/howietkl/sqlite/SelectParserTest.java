package org.howietkl.sqlite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SelectParserTest {

  @BeforeEach
  void setUp() {
  }

  @AfterEach
  void tearDown() {
  }

  @Test
  void parse() {
    SelectParser parser = SelectParser.parse("select name from apples");
    assertEquals("apples", parser.getTableName());
    assertEquals(1, parser.getColumns().length);
    assertEquals("name", parser.getColumns()[0]);

    parser = SelectParser.parse("select name from apples where name = 'gala'");
    assertEquals("apples", parser.getTableName());
    assertEquals(1, parser.getColumns().length);

    parser = SelectParser.parse("select name, color  , description from apples where name = 'gala'");
    assertEquals("apples", parser.getTableName());
    assertEquals(3, parser.getColumns().length);
    assertEquals("name", parser.getColumns()[0]);
    assertEquals("color", parser.getColumns()[1]);
    assertEquals("description", parser.getColumns()[2]);

  }
}