package hiveudfs.udf.base;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

public class ToOctalString extends ToBinaryString {

    public Object intEvaluate(DeferredObject[] arguments) throws HiveException {
        int value;
        value = PrimitiveObjectInspectorUtils.getInt(arguments[0].get(), inputIntOI);
        result.set(Integer.toOctalString(value));
        return result;
    }

    public Object longEvaluate(DeferredObject[] arguments) throws HiveException {
        long value;
        value = PrimitiveObjectInspectorUtils.getLong(arguments[0].get(), inputLongOI);
        result.set(Long.toOctalString(value));
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
