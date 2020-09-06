package hiveudfs.udf.date;

import java.text.ParseException;
import java.text.SimpleDateFormat;

final class DatePatternType {
    public static final int DATE = 1;
    public static final int TIMESTAMP = 2;
    public static final int TIMESTAMP_TZ = 3;
}

public enum DatePattern {
    DATE_NO_DASH("DATE_NO_DASH", "yyyyMMdd", DatePatternType.DATE),
    DATE_WITH_HYPHEN("DATE_WITH_HYPHEN", "yyyy-MM-dd", DatePatternType.DATE),
    DATE_WITH_SLASH("DATE_WITH_SLASH", "yyyy/MM/dd", DatePatternType.DATE),
    TIMESTAMP_ISO_NO_DASH("TIMESTAMP_ISO_NO_DASH", "yyyyMMdd'T'HHmmss", DatePatternType.TIMESTAMP),
    TIMESTAMP_ISO_WITH_HYPHEN("TIMESTAMP_ISO_WITH_HYPHEN", "yyyy-MM-dd'T'HH:mm:ss", DatePatternType.TIMESTAMP),
    TIMESTAMP_ISO_WITH_SLASH("TIMESTAMP_ISO_WITH_SLASH", "yyyy/MM/dd'T'HH:mm:ss", DatePatternType.TIMESTAMP),
    TIMESTAMP_NO_DASH("TIMESTAMP_NO_DASH", "yyyyMMdd HHmmss", DatePatternType.TIMESTAMP),
    TIMESTAMP_WITH_HYPHEN("TIMESTAMP_WITH_HYPHEN", "yyyy-MM-dd HH:mm:ss", DatePatternType.TIMESTAMP),
    TIMESTAMP_WITH_SLASH("TIMESTAMP_WITH_SLASH", "yyyy/MM/dd HH:mm:ss", DatePatternType.TIMESTAMP),
    TIMESTAMP_TZ_ISO_NO_DASH("TIMESTAMP_TZ_ISO_NO_DASH", "yyyyMMdd'T'HHmmssZ", DatePatternType.TIMESTAMP_TZ),
    TIMESTAMP_TZ_ISO_WITH_HYPHEN("TIMESTAMP_TZ_ISO_WITH_HYPHEN", "yyyy-MM-dd'T'HH:mm:ssZ", DatePatternType.TIMESTAMP_TZ),
    TIMESTAMP_TZ_ISO_WITH_SLASH("TIMESTAMP_TZ_ISO_WITH_SLASH", "yyyy/MM/dd'T'HH:mm:ssZ", DatePatternType.TIMESTAMP_TZ),
    TIMESTAMP_TZ_NO_DASH("TIMESTAMP_TZ_NO_DASH", "yyyyMMdd HHmmssZ", DatePatternType.TIMESTAMP_TZ),
    TIMESTAMP_TZ_WITH_HYPHEN("TIMESTAMP_TZ_WITH_HYPHEN", "yyyy-MM-dd HH:mm:ssZ", DatePatternType.TIMESTAMP_TZ),
    TIMESTAMP_TZ_WITH_SLASH("TIMESTAMP_TZ_WITH_SLASH", "yyyy/MM/dd HH:mm:ssZ", DatePatternType.TIMESTAMP_TZ),
    ;

    private final String name;
    private final String pattern;
    private final int type;

    DatePattern(String name, String pattern, int type) {
        this.name = name;
        this.pattern = pattern;
        this.type = type;
    }

    public String getName() {
        return this.name;
    }
    public String getPattern() {
        return this.pattern;
    }
    public int getType() {
        return this.type;
    }

    public static boolean isMember(String format) {
        for (DatePattern dateFormat : values()) {
            if (dateFormat.getPattern().equals(format)) {
                return true;
            }
        }
        return false;
    }
    public static boolean isMember(String format, int formatType) {
        for (DatePattern dateFormat : values()) {
            if (dateFormat.getType() == formatType && dateFormat.getPattern().equals(format)) {
                return true;
            }
        }
        return false;
    }

    public static String matchPattern(String text) {
        if (text == null) {
            return null;
        }
        for (DatePattern datePattern : values()) {
            SimpleDateFormat formatter = new SimpleDateFormat(datePattern.getPattern());
            String reformat = null;
            try {
                reformat = formatter.format(formatter.parse(text));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            if (text.equals(reformat)) {
                return datePattern.getPattern();
            }
        }
        return null;
    }
}
