package hiveudfs.udf.base;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.Text;

public class ToBinaryString extends GenericUDF {
    protected transient IntObjectInspector inputIntOI;
    protected transient LongObjectInspector inputLongOI;
    protected final Text result = new Text();

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 1) {
            throw new UDFArgumentException("The function " + getUdfName() + " requires at least one argument.");
        }
        if (arguments.length > 1) {
            throw new UDFArgumentException("Too many arguments for the function " + getUdfName() + ".");
        }

        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Argument " + 1 + " of function " + getUdfName() + " only takes " +
                    "primitive types" +
                    ", but " + arguments[0].getTypeName() + ".");
        }
        switch (((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
                inputIntOI = (IntObjectInspector) arguments[0];
                break;
            case LONG:
                inputLongOI = (LongObjectInspector) arguments[0];
                break;
            default:
                throw new UDFArgumentTypeException(0, "Argument " + 1 + " of " + getUdfName() + " must be " +
                        serdeConstants.INT_TYPE_NAME + ", " +
                        serdeConstants.BIGINT_TYPE_NAME +
                        ", but " + arguments[0].getTypeName() + ".");
        }
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (inputIntOI != null) {
            return intEvaluate(arguments);
        } else if (inputLongOI != null) {
            return longEvaluate(arguments);
        }
        return null;
    }

    public Object intEvaluate(DeferredObject[] arguments) throws HiveException {
        int value;
        value = PrimitiveObjectInspectorUtils.getInt(arguments[0].get(), inputIntOI);
        result.set(Integer.toBinaryString(value));
        return result;
    }

    public Object longEvaluate(DeferredObject[] arguments) throws HiveException {
        long value;
        value = PrimitiveObjectInspectorUtils.getLong(arguments[0].get(), inputLongOI);
        result.set(Long.toBinaryString(value));
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
