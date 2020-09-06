package hiveudfs.udtf.series;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class DateRangeND extends GenericUDTF {

    protected transient PrimitiveObjectInspector[] inputOIs;
    protected transient Converter[] converters;
    protected transient Object[] forwardObjects = new Object[1];
    protected transient final SimpleDateFormat parser = new SimpleDateFormat("yyyyMMdd");

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length < 2 || argOIs.length > 3) {
            throw new UDFArgumentLengthException("The function" + getFuncName() + " accepts two to three arguments");
        }

        inputOIs = new PrimitiveObjectInspector[argOIs.length];
        converters = new Converter[argOIs.length];
        for (int i = 0; i < argOIs.length; i++) {
            if (argOIs[i].getCategory() != Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(0, "Argument " + 1 + " of function " + getFuncName() + " must be primitive.");
            }
            inputOIs[i] = (PrimitiveObjectInspector) argOIs[i];
            if (i < 2) {
                switch (inputOIs[i].getPrimitiveCategory()) {
                    case STRING:
                    case VARCHAR:
                    case CHAR:
                        break;
                    default:
                        throw new UDFArgumentTypeException(i, "Argument " + (i + 1) + " of function " + getFuncName() + " must be " +
                                serdeConstants.STRING_TYPE_NAME +
                                ", but " + argOIs[i].getTypeName() + ".");
                }
                converters[i] = ObjectInspectorConverters.getConverter(inputOIs[i], PrimitiveObjectInspectorFactory.writableStringObjectInspector);
            }
            else {
                switch (inputOIs[i].getPrimitiveCategory()) {
                    case BYTE:
                    case SHORT:
                    case INT:
                        break;
                    default:
                        throw new UDFArgumentTypeException(i, "Argument " + (i + 1) + " of function " + getFuncName() + " must be " +
                                serdeConstants.INT_TYPE_NAME +
                                ", but " + argOIs[i].getTypeName() + ".");
                }
                converters[i] = ObjectInspectorConverters.getConverter(inputOIs[i], PrimitiveObjectInspectorFactory.writableIntObjectInspector);
            }
        }
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("date");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {

        Date start;
        Date end;
        int interval;

        switch (args.length) {
            case 2:
                try {
                    start = parser.parse(converters[0].convert(args[0]).toString());
                    end = parser.parse(converters[1].convert(args[1]).toString());
                } catch (ParseException ignore) {
                    forwardObjects[0] = new Text(args[0].toString());
                    forward(forwardObjects);
                    return;
                }
                interval = 1;
                break;
            case 3:
                try {
                    start = parser.parse(converters[0].convert(args[0]).toString());
                    end = parser.parse(converters[1].convert(args[1]).toString());
                } catch (ParseException ignore) {
                    forwardObjects[0] = new Text(args[0].toString());
                    forward(forwardObjects);
                    return;
                }
                interval = ((IntWritable) converters[2].convert(args[2])).get();
                if (interval == 0) {
                    throw new UDFArgumentException("Argument " + 3 + " of function " + getFuncName() + " must not be zero.");
                }
                break;
            default:
                throw new UDFArgumentLengthException("The function" + getFuncName() + " accepts one to three arguments");
        }
        long diffMillis = end.getTime() - start.getTime();
        long diffDates = TimeUnit.DAYS.convert(diffMillis, TimeUnit.MILLISECONDS);
        if (interval > 0 && diffDates > 0) {
            for (long i = 0; i < diffDates; i+=interval) {
                Date newDate = new Date(start.getTime() + 24 * 60 * 60 * 1000 * i);
                forwardObjects[0] = new Text(parser.format(newDate));
                forward(forwardObjects);
            }
        } else if (interval < 0 && diffDates < 0) {
            for (long i = 0; i > diffDates; i+=interval) {
                Date newDate = new Date(start.getTime() - 24 * 60 * 60 * 1000 * i);
                forwardObjects[0] = new Text(parser.format(newDate));
                forward(forwardObjects);
            }
        }
    }

    @Override
    public void close() throws HiveException {
    }

    @Override
    public String toString() {
        return getFuncName();
    }

    protected String getFuncName() {
        return getClass().getSimpleName().toLowerCase();
    }
}
