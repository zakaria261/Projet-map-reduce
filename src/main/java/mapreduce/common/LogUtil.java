package mapreduce.common;

import java.util.logging.*;

/** Configuration du logger Java pour un affichage lisible. */
public class LogUtil {

    public static void setup() {
        Logger root = Logger.getLogger("");
        root.setLevel(Level.INFO);
        for (Handler h : root.getHandlers()) {
            h.setFormatter(new SimpleFormatter() {
                @Override
                public String format(LogRecord r) {
                    return String.format("%tT [%-7s] %s: %s%n",
                            r.getMillis(), r.getLevel(), r.getLoggerName(), r.getMessage());
                }
            });
        }
    }
}
