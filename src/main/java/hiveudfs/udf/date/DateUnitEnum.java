package hiveudfs.udf.date;

public enum DateUnitEnum {
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

    DateUnitEnum(String format, int formatType) {
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
        for (DateUnitEnum DateUnitEnum : values()) {
            if (DateUnitEnum.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }
    public static boolean isMember(String name, int formatType) {
        for (DateUnitEnum DateUnitEnum : values()) {
            if (DateUnitEnum.getType() == formatType && DateUnitEnum.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }
}
