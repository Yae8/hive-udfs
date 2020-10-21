package hiveudfs.utils;

import java.util.Arrays;
import java.util.HashSet;

public class SetMatcher {

    public static boolean isSubset(HashSet<?> set1, HashSet<?> set2) {
        return set2.containsAll(set1);
    }

    public static boolean isSubset(String[] strings1, String[] strings2) {
        return isSubset(new HashSet<String>(Arrays.asList(strings1)), new HashSet<String>(Arrays.asList(strings2)));
    }

    public static boolean isSuperset(HashSet<?> set1, HashSet<?> set2) {
        return set1.containsAll(set2);
    }

    public static boolean isSuperset(String[] strings1, String[] strings2) {
        return isSuperset(new HashSet<String>(Arrays.asList(strings1)), new HashSet<String>(Arrays.asList(strings2)));
    }

    public static boolean isEqual(HashSet<?> set1, HashSet<?> set2) {
        return set1.equals(set2);
    }

    public static boolean isEqual(String[] strings1, String[] strings2) {
        return isEqual(new HashSet<String>(Arrays.asList(strings1)), new HashSet<String>(Arrays.asList(strings2)));
    }
}
