package hiveudfs.udf.matcher;

public enum MatchMethodEnum {
    PERFECT(0),
    PARTIAL(1),
    SUFFIX(2),
    PREFIX(3),
    SUBSET(10),
    SUPERSET(11),
    NO_PARTICULAR_ORDER(20),
    ;

    private final int num;

    MatchMethodEnum(int num) {
        this.num = num;
    }

    public int getNum() { return this.num; }

    public static boolean isMember(int num) {
        for (MatchMethodEnum matchMethod : values()) {
            if (matchMethod.getNum() == num) {
                return true;
            }
        }
        return false;
    }

    public static MatchMethodEnum findByNum(int num) {
        for (MatchMethodEnum matchMethod : values()) {
            if (matchMethod.getNum() == num) {
                return matchMethod;
            }
        }
        return null;
    }
}
