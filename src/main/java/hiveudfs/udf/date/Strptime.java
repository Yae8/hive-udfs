package hiveudfs.udf.date;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Strptime extends GenericUDF {

    protected transient Converter[] converters = new Converter[2];
    protected transient String pattern;
    protected transient String constPattern;
    protected transient SimpleDateFormat parser;
    protected transient SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    protected final Text result = new Text();

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2)  {
            throw new UDFArgumentLengthException("The function " + getUdfName() + " accepts two arguments");
        }
        for (int i=0; i<arguments.length; i++) {
            if (arguments[i].getCategory() != Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i, "Argument " + (i + 1) + " of function " + getUdfName() + " only takes " +
                        "primitive types" +
                        ", but " + arguments[i].getTypeName() + ".");
            }
        }

        for (int i=0; i<arguments.length; i++) {
            PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) arguments[i];
            ObjectInspector outputOI;
            switch (inputOI.getPrimitiveCategory()) {
                case STRING:
                case VARCHAR:
                case CHAR:
                    break;
                default:
                    throw new UDFArgumentTypeException(i, "Argument " + (i + 1) + " of function must be " +
                            serdeConstants.STRING_TYPE_NAME +
                            ", but " + arguments[i].getTypeName() + ".");
            }
            outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
            converters[i] = ObjectInspectorConverters.getConverter(inputOI, outputOI);
        }

        if (arguments[1] instanceof ConstantObjectInspector) {
            constPattern = ((ConstantObjectInspector) arguments[1]).getWritableConstantValue().toString();
        }

        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("The function" + getUdfName() + " accepts two arguments");
        }
        if (arguments[0].get() == null || arguments[1].get() == null) {
            return null;
        }

        if (constPattern != null) {
            pattern = constPattern;
        } else {
            pattern = arguments[1].get().toString();
        }
        parser = new SimpleDateFormat(pattern);

        String strDate = converters[0].convert(arguments[0].get()).toString();
        try {
            result.set(formatter.format(parser.parse(strDate)));
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
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
