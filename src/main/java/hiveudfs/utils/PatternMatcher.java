package hiveudfs.utils;

public class PatternMatcher {
    private static String regex;

    public static boolean isPerfectMatch(String str1, String str2) {
        regex = "^" + str2 + "$";
        return str1.matches(regex);
    }

    public static boolean isPartialMatch(String str1, String str2) {
        regex = ".*" + str2 + ".*";
        return str1.matches(regex);
    }

    public static boolean isPrefixMatch(String str1, String str2) {
        regex = "^" + str2 + ".*";
        return str1.matches(regex);
    }

    public static boolean isSuffixMatch(String str1, String str2) {
        regex = ".*" + str2 + "$";
        return str1.matches(regex);
    }
}
