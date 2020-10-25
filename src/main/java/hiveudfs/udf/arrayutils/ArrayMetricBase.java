package hiveudfs.udf.arrayutils;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

abstract class ArrayMetricBase extends GenericUDF {

    protected ListObjectInspector listOI;
    protected Category category;
    protected PrimitiveObjectInspector elementOI;
    protected PrimitiveCategory elementCategory;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 1) {
            throw new UDFArgumentLengthException("The function " + getUdfName() + " takes arguments at least one.");
        }

        ObjectInspector inputOI = arguments[0];
        if (inputOI.getCategory() != Category.LIST) {
            throw new UDFArgumentTypeException(0, "Argument " + 1 + " of function " + getUdfName() + " only takes a list" +
                    ", but " + arguments[0].getTypeName() + ".");
        }

        listOI = (ListObjectInspector) inputOI;
        if (listOI.getListElementObjectInspector().getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Argument " + 1 + " of function " + getUdfName() + " only takes a list of primitives" +
                    ", but " + arguments[0].getTypeName() + ".");
        }
        elementOI = (PrimitiveObjectInspector) listOI.getListElementObjectInspector();
        elementCategory = elementOI.getPrimitiveCategory();
        return elementOI;
    }
}
