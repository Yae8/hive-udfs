package hiveudfs.udf.base;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.IntWritable;

public class ParseInt extends GenericUDF {

    protected transient StringObjectInspector inputStringOI;
    protected transient IntObjectInspector inputIntOI;
    private IntWritable result;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 1)  {
            throw new UDFArgumentLengthException("The function " + getUdfName() + " requests at least one argument.");
        }
        if (arguments.length > 2) {
            throw new UDFArgumentLengthException("The function " + getUdfName() + " accepts two arguments.");
        }

        for (int i=0; i<arguments.length; i++) {
            if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i, "Argument " + (i + 1) + " of function " + getUdfName() + " only takes " +
                        "primitive types" +
                        ", but " + arguments[i].getTypeName() + ".");
            }
        }

        switch (((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory()) {
            case STRING:
            case VARCHAR:
            case CHAR:
                break;
            default:
                throw new UDFArgumentTypeException(0, "Argument " + 1 + " of function must be " +
                        serdeConstants.STRING_TYPE_NAME +
                        ", but " + arguments[0].getTypeName() + ".");
        }
        inputStringOI = (StringObjectInspector) arguments[0];

        switch (((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
                break;
            default:
                throw new UDFArgumentTypeException(1, "Argument " + 2 + " of function must be " +
                        serdeConstants.INT_TYPE_NAME +
                        ", but " + arguments[1].getTypeName() + ".");
        }
        inputIntOI = (IntObjectInspector) arguments[1];
        result = new IntWritable(0);
        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0] == null) {
            return null;
        }
        if (arguments[1] == null) {
            return null;
        }

        String s = PrimitiveObjectInspectorUtils.getString(arguments[0].get(), inputStringOI);
        int radix = PrimitiveObjectInspectorUtils.getInt(arguments[1].get(), inputIntOI);

        result.set(Integer.parseInt(s, radix));
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
