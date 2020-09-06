package hiveudfs.udf.arrayutils;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

abstract class ArrayUtilsBase extends GenericUDF {
    protected ListObjectInspector listOI;
    protected PrimitiveObjectInspector primitiveOI;
    protected PrimitiveObjectInspector.PrimitiveCategory elementCategory;
    protected String FUNC;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("The function " + FUNC + " takes a list.");
        }

        ObjectInspector inputOI = arguments[0];
        if (inputOI.getCategory() != Category.LIST) {
            throw new UDFArgumentException("The function " + FUNC + " takes a list.");
        }

        listOI = (ListObjectInspector) inputOI;
        if (listOI.getListElementObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("The function " + FUNC + " takes only list of primitives.");
        }

        primitiveOI = (PrimitiveObjectInspector) listOI.getListElementObjectInspector();
        elementCategory = primitiveOI.getPrimitiveCategory();
        return ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(elementCategory));
    }
}
