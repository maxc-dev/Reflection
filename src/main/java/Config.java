import java.time.format.DateTimeFormatter;

public class Config {
    private boolean isCaseSensitive = true;
    private boolean wrapBooleansInQuotes = false;
    private DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public boolean isCaseSensitive() {
        return isCaseSensitive;
    }

    public Config setCaseSensitive(boolean caseSensitive) {
        isCaseSensitive = caseSensitive;
        return this;
    }

    public boolean isWrapBooleansInQuotes() {
        return wrapBooleansInQuotes;
    }

    public Config setWrapBooleansInQuotes(boolean wrapBooleansInQuotes) {
        this.wrapBooleansInQuotes = wrapBooleansInQuotes;
        return this;
    }

    public DateTimeFormatter getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = DateTimeFormatter.ofPattern(dateFormat);
    }

    public DateTimeFormatter getDateTimeFormat() {
        return dateTimeFormatter;
    }

    public void setDateTimeFormatter(String dateTimeFormatter) {
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormatter);
    }
}
