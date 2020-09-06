package hiveudfs.udf.setmetric;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.HashSet;

public class SetSubset extends SetMetricBase {

    @Override
    Object evaluateList(DeferredObject[] arguments) throws HiveException {
        result.set(false);

        HashSet<?> set1 = new HashSet<>(list1OI.getList(arguments[0].get()));
        HashSet<?> set2 = new HashSet<>(list1OI.getList(arguments[1].get()));

        result.set(set2.containsAll(set1));
        return result;
    }

    @Override
    Object evaluateMap(DeferredObject[] arguments) throws HiveException {
        result.set(false);

        HashSet<?> set1 = new HashSet<>(map1OI.getMap(arguments[0].get()).keySet());
        HashSet<?> set2 = new HashSet<>(map1OI.getMap(arguments[1].get()).keySet());

        result.set(set2.containsAll(set1));
        return result;
    }

    @Override
    protected String getFuncName() {
        return getClass().getSimpleName().toLowerCase();
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 1);
        return getStandardDisplayString(getFuncName(), children);
    }
}
