package hiveudfs.udf.arrayutils;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.HashMap;
import java.util.List;

public class ArrayCountFreq extends GenericUDF {

    protected ListObjectInspector listOI;
    protected PrimitiveObjectInspector elementOI;
    protected PrimitiveObjectInspector.PrimitiveCategory elementCategory;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("The function " + getUdfName() + " takes a list.");
        }
        ObjectInspector inputOI = arguments[0];
        if (inputOI.getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentException("The function " + getUdfName() + " takes a list.");
        }
        listOI = (ListObjectInspector) inputOI;
        if (listOI.getListElementObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("The function " + getUdfName() + " takes only map or list of primitives.");
        }
        elementOI = (PrimitiveObjectInspector) listOI.getListElementObjectInspector();
        return ObjectInspectorFactory.getStandardMapObjectInspector(elementOI, PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        List<?> list = listOI.getList(arguments[0].get());
        HashMap<Object, Long> result = new HashMap<>();
        if (list == null) {
            return result;
        }
        if (list.size() <= 0) {
            return result;
        }

        for (int i = 0; i < list.size(); i++) {
            Object element = listOI.getListElement(list, i);
            result.put(element, result.getOrDefault(element, 0L) + 1L);
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
