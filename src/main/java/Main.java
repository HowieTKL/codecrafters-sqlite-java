import org.howietkl.sqlite.command.DBInfoCommand;
import org.howietkl.sqlite.command.SQLCommand;
import org.howietkl.sqlite.command.TablesCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args){
    if (args.length < 2) {
      System.out.println("Missing <database path> and <command>");
      return;
    }

    String command = args[1];

    try {
      switch (command) {
        case ".dbinfo" -> new DBInfoCommand().execute(args);
        case ".tables" -> new TablesCommand().execute(args);
        default -> new SQLCommand().execute(args);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

}
