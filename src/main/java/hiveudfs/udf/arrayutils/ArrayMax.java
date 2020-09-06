package hiveudfs.udf.arrayutils;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import java.util.List;

public class ArrayMax extends ArrayMetricBase {

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("The function " + getUdfName() + " takes one arguments.");
        }
        return super.initialize(arguments);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        List<?> list = listOI.getList(arguments[0].get());

        if (list == null) {
            return null;
        }
        if (list.size() <= 0) {
            return null;
        }
        Object result = null;
        for (int i = 0; i < list.size(); i++) {
            Object element = listOI.getListElement(list, i);
            if (element == null) {
                continue;
            }
            int res = ObjectInspectorUtils.compare(result, elementOI, element, elementOI);
            if (result == null || res < 0) {
                result = element;
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
