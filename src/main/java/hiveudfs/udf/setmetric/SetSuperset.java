package hiveudfs.udf.setmetric;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import java.util.HashSet;

public class SetSuperset extends SetMetricBase {

    @Override
    Object evaluateList(GenericUDF.DeferredObject[] arguments) throws HiveException {
        result.set(false);

        HashSet<?> set1 = new HashSet<>(list1OI.getList(arguments[0].get()));
        HashSet<?> set2 = new HashSet<>(list1OI.getList(arguments[1].get()));

        result.set(set1.containsAll(set2));
        return result;
    }

    @Override
    Object evaluateMap(GenericUDF.DeferredObject[] arguments) throws HiveException {
        result.set(false);

        HashSet<?> set1 = new HashSet<>(map1OI.getMap(arguments[0].get()).keySet());
        HashSet<?> set2 = new HashSet<>(map1OI.getMap(arguments[1].get()).keySet());

        result.set(set1.containsAll(set2));
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
