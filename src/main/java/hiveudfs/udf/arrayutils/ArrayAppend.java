package hiveudfs.udf.arrayutils;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import java.util.ArrayList;
import java.util.List;

public class ArrayAppend extends ArrayUtilsBase {

    protected transient PrimitiveObjectInspector[] primOIs;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        ObjectInspector resultOI = super.initialize(arguments);

        ObjectInspector elementOI = listOI.getListElementObjectInspector();
        primOIs = new PrimitiveObjectInspector[arguments.length - 1];
        for (int i = 1; i < arguments.length; i++) {
            ObjectInspector inputOI = arguments[i];
            if (arguments[i].getCategory() != Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i,
                    "Only primitive type arguments are accepted" +
                    ", but " + arguments[i].getCategory() + " is passed.");
            }
            if (!ObjectInspectorUtils.compareTypes(elementOI, inputOI)) {
                throw new UDFArgumentTypeException(i,
                    "Argument " + (i + 1) + " of function " + getUdfName() + " must be of the same type as the element of the first argument list" +
                    ", but " + arguments[1].getTypeName() + ".");
            }
            primOIs[i - 1] = (PrimitiveObjectInspector) arguments[i];
        }

        return resultOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        List<?> list = listOI.getList(arguments[0].get());
        if (list == null) {
            return null;
        }

        List<Object> result = new ArrayList<>(list);
        for (int i = 1; i < arguments.length; i++) {
            Object element = arguments[i].get();
            result.add(element);
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
