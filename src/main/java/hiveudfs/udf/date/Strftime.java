package hiveudfs.udf.date;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Strftime extends GenericUDF {

    protected transient Converter[] converters = new Converter[2];
    protected transient PrimitiveCategory primitiveCategory;
    protected transient SimpleDateFormat formatter;
    protected transient String format;
    protected transient String constFormat;
    protected transient SimpleDateFormat parser;
    protected transient String pattern;
    private final Text result = new Text();

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("The function" + getUdfName() + " accepts two arguments");
        }
        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i, "Argument " + (i + 1) + " of function " + getUdfName() + " only takes " +
                        "primitive types" +
                        ", but " + arguments[i].getTypeName() + ".");
            }
        }

        PrimitiveObjectInspector inputOI;
        ObjectInspector outputOI;

        inputOI = (PrimitiveObjectInspector) arguments[0];
        switch (inputOI.getPrimitiveCategory()) {
            case STRING:
            case VARCHAR:
            case CHAR:
                outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
                break;
            case DATE:
                outputOI = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
                break;
            case TIMESTAMP:
                outputOI = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
                break;
            default:
                throw new UDFArgumentTypeException(0, "Argument " + 1 + " of function " + getUdfName() + " must be " +
                        serdeConstants.STRING_TYPE_NAME + ", " +
                        serdeConstants.DATE_TYPE_NAME + " or " +
                        serdeConstants.TIMESTAMP_TYPE_NAME +
                        ", but " + arguments[0].getTypeName() + ".");
        }
        primitiveCategory = inputOI.getPrimitiveCategory();
        converters[0] = ObjectInspectorConverters.getConverter(inputOI, outputOI);

        inputOI = (PrimitiveObjectInspector) arguments[1];
        switch (inputOI.getPrimitiveCategory()) {
            case STRING:
            case VARCHAR:
            case CHAR:
                outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
                break;
            default:
                throw new UDFArgumentTypeException(1, "Argument " + 2 + " of function " + getUdfName() + " must be " +
                        serdeConstants.STRING_TYPE_NAME +
                        ", but " + arguments[1].getTypeName() + ".");
        }
        converters[1] = ObjectInspectorConverters.getConverter(inputOI, outputOI);
        if (arguments[1] instanceof ConstantObjectInspector) {
            constFormat = ((ConstantObjectInspector) arguments[1]).getWritableConstantValue().toString();
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

        Long epochMilli = null;
        switch (primitiveCategory) {
            case STRING:
                String strDate = converters[0].convert(arguments[0].get()).toString();
                pattern = DatePattern.matchPattern(strDate);
                parser = new SimpleDateFormat(pattern);
                try {
                    Date date = parser.parse(strDate);
                    epochMilli = date.getTime();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                break;
            case DATE:
                DateWritable dateWritable = (DateWritable) converters[0].convert(arguments[0].get());
                epochMilli = dateWritable.getTimeInSeconds() * 1000;
                break;
            case TIMESTAMP:
                TimestampWritable timestampWritable = (TimestampWritable) converters[0].convert(arguments[0].get());
                epochMilli = timestampWritable.getSeconds() * 1000;
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + primitiveCategory);
        }

        if (constFormat != null) {
            format = constFormat;
        } else {
            format = converters[1].convert(arguments[1].get()).toString();
        }
        formatter = new SimpleDateFormat(format);

        if (epochMilli == null) {
            return null;
        }
        result.set(formatter.format(new Date(epochMilli)));
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
