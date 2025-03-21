package org.howietkl.sqlite.command;

public interface Command {
  void execute(String[] args) throws Exception;
}
