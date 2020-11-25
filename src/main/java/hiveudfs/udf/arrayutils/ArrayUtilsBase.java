package hiveudfs.udf.arrayutils;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

abstract class ArrayUtilsBase extends GenericUDF {

    protected transient ListObjectInspector listOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 1) {
            throw new UDFArgumentException("The function " + this.getFuncName() + " takes arguments at least one.");
        }

        ObjectInspector inputOI = arguments[0];
        if (inputOI.getCategory() != Category.LIST) {
            throw new UDFArgumentTypeException(0,
                "Argument " + 1 + " of function " + this.getFuncName() + " only takes a list" +
                ", but " + arguments[0].getTypeName() + ".");
        }
        listOI = (ListObjectInspector) inputOI;
        if (listOI.getListElementObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                "Argument " + 1 + " of function " + this.getFuncName() + " only takes a list of primitives" +
                ", but " + arguments[0].getTypeName() + ".");
        }

        return ObjectInspectorFactory.getStandardListObjectInspector(
            listOI.getListElementObjectInspector());
    }
}
