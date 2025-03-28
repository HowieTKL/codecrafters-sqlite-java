package org.howietkl.sqlite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CreateTableTest {

  @BeforeEach
  void setUp() {
  }

  @AfterEach
  void tearDown() {
  }

  @Test
  void parse() {
    CreateTableParser createTable = CreateTableParser.parse("create table apples(id integer primary key, name text, description text)");
    assertEquals("apples", createTable.getTableName());
    assertEquals(3, createTable.getColumns().length);
    assertEquals("id", createTable.getColumns()[0].name());
    assertEquals("integer", createTable.getColumns()[0].type());
    assertEquals("name", createTable.getColumns()[1].name());
    assertEquals("text", createTable.getColumns()[1].type());
    assertEquals("description", createTable.getColumns()[2].name());
    assertEquals("text", createTable.getColumns()[2].type());

    createTable = CreateTableParser.parse("create table oranges\n(\nid integer primary key,\nname text,\ndescription text\n)");
    assertEquals("oranges", createTable.getTableName());
    assertEquals(3, createTable.getColumns().length);

    createTable = CreateTableParser.parse("CREATE TABLE sqlite_sequence(name,seq)");
    assertEquals("sqlite_sequence", createTable.getTableName());
    assertEquals(2, createTable.getColumns().length);
  }
}