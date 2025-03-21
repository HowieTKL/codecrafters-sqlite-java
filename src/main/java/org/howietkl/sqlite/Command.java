package org.howietkl.sqlite;

import java.io.IOException;

public interface Command {
  void execute(String[] args) throws Exception;
}
