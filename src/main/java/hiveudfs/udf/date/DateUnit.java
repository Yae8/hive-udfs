package hiveudfs.udf.date;

final class DateUnitType {
    public static final int YEAR = 1;
    public static final int MONTH = 2;
    public static final int DATE = 3;
    public static final int HOUR = 4;
    public static final int MINUTE = 5;
    public static final int SECOND = 6;
    public static final int QUARTER = 7;
    private DateUnitType (){}
}

public enum DateUnit {
    YEAR("YEAR", DateUnitType.YEAR),
    YYYY("YYYY", DateUnitType.YEAR),
    YY("YY", DateUnitType.YEAR),
    QUARTER("QUARTER", DateUnitType.QUARTER),
    Q("Q", DateUnitType.QUARTER),
    MONTH("MONTH", DateUnitType.MONTH),
    MON("MON", DateUnitType.MONTH),
    MM("MM", DateUnitType.MONTH),
    DAY("DAY", DateUnitType.DATE),
    DD("DD", DateUnitType.DATE),
    HOUR("HOUR", DateUnitType.HOUR),
    HH("HH", DateUnitType.HOUR),
    MINUTE("MINUTE", DateUnitType.MINUTE),
    MIN("MIN", DateUnitType.MINUTE),
    SECOND("SECOND", DateUnitType.SECOND),
    SS("SS", DateUnitType.SECOND),
    ;

    private final String name;
    private final int type;

    DateUnit(String format, int formatType) {
        this.name = format;
        this.type = formatType;
    }

    public String getName() {
        return this.name;
    }
    public int getType() {
        return this.type;
    }

    public static boolean isMember(String name) {
        for (DateUnit dateUnit : values()) {
            if (dateUnit.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }
    public static boolean isMember(String name, int formatType) {
        for (DateUnit dateUnit : values()) {
            if (dateUnit.getType() == formatType && dateUnit.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }
}
