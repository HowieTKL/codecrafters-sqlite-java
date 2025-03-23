package org.howietkl.sqlite.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLCommand implements Command {
  private static final Logger LOG = LoggerFactory.getLogger(SQLCommand.class);

  @Override
  public void execute(String[] args) throws Exception {
    executeSQL(args[0], args[1]);
  }

  static void executeSQL(String db, String sql) {
    String[] args = sql.split(" ");
    String table = args[args.length - 1];

    System.out.println(table);

  }
}
