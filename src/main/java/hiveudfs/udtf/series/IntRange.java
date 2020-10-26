package hiveudfs.udtf.series;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;

public class IntRange extends GenericUDTF {

    protected transient PrimitiveObjectInspector[] inputOIs;
    protected transient Converter[] converters;
    protected transient Object[] forwardObjects = new Object[1];

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length < 1 || argOIs.length > 3) {
            throw new UDFArgumentLengthException("The function" + getFuncName() + " accepts one to three arguments");
        }

        inputOIs = new PrimitiveObjectInspector[argOIs.length];
        converters = new Converter[argOIs.length];
        for (int i = 0; i < argOIs.length; i++) {
            if (argOIs[i].getCategory() != Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i, "Argument " + (i + 1) + " of function " + getFuncName() + " must be primitive.");
            }
            inputOIs[i] = (PrimitiveObjectInspector) argOIs[i];
            switch (inputOIs[i].getPrimitiveCategory()) {
                case BYTE:
                case SHORT:
                case INT:
                case LONG:
                    break;
                default:
                    throw new UDFArgumentTypeException(i, "Argument " + (i + 1) + " of function " + getFuncName() + " must be " +
                            serdeConstants.INT_TYPE_NAME +
                            ", but " + argOIs[i].getTypeName() + ".");
            }
            converters[i] = ObjectInspectorConverters.getConverter(inputOIs[i], PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        }
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("value");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        int start;
        int end;
        int step;

        switch (args.length) {
            case 1:
                start = 0;
                end = ((IntWritable) converters[0].convert(args[0])).get();
                step = 1;
                break;
            case 2:
                start = ((IntWritable) converters[0].convert(args[0])).get();
                end = ((IntWritable) converters[1].convert(args[1])).get();
                step = 1;
                break;
            case 3:
                start = ((IntWritable) converters[0].convert(args[0])).get();
                end = ((IntWritable) converters[1].convert(args[1])).get();
                step = ((IntWritable) converters[2].convert(args[2])).get();
                if (step == 0) {
                    throw new UDFArgumentException("Argument " + 3 + " of function " + getFuncName() + " must not be zero.");
                }
                break;
            default:
                throw new UDFArgumentLengthException("The function" + getFuncName() + " accepts one to three arguments");
        }
        if (step > 0 && start < end) {
            for (int i = start; i < end; i += step) {
                forwardObjects[0] = new IntWritable(i);
                forward(forwardObjects);
            }
        } else if (step < 0 && start > end) {
            for (int i = start; i > end; i += step) {
                forwardObjects[0] = new IntWritable(i);
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
