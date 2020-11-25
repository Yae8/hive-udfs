package hiveudfs.udf.arrayutils;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import java.util.ArrayList;
import java.util.List;

public class ArrayUnique extends ArrayUtilsBase {

    protected PrimitiveObjectInspector[] primOIs;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("The function " + getFuncName() + " accepts one argument.");
        }
        return super.initialize(arguments);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        List<?> list = listOI.getList(arguments[0].get());
        if (list == null) {
            return null;
        }

        List<Object> result = new ArrayList<>();
        for (Object object : list) {
            if (!result.contains(object)) {
                result.add(object);
            }
        }

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
