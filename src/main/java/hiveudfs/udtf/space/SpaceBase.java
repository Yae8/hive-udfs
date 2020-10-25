package hiveudfs.udtf.space;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;

abstract class SpaceBase extends GenericUDTF {
    protected transient PrimitiveObjectInspector[] inputOIs;
    protected transient ObjectInspectorConverters.Converter[] converters;
    protected transient Object[] forwardObjects = new Object[1];

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length < 2 || argOIs.length > 3) {
            throw new UDFArgumentLengthException("The function" + getFuncName() + " accepts two to three arguments");
        }

        inputOIs = new PrimitiveObjectInspector[argOIs.length];
        converters = new ObjectInspectorConverters.Converter[argOIs.length];
        for (int i = 0; i < argOIs.length; i++) {
            if (argOIs[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i, "Argument " + (i + 1) + " of function " + getFuncName() + " must be primitive.");
            }
            inputOIs[i] = (PrimitiveObjectInspector) argOIs[i];
            PrimitiveObjectInspector.PrimitiveCategory primCategory = inputOIs[i].getPrimitiveCategory();
            if (i < 2) {
                switch (primCategory) {
                    case BYTE:
                    case SHORT:
                    case INT:
                    case LONG:
                    case DECIMAL:
                    case FLOAT:
                    case DOUBLE:
                    case VOID:
                        break;
                    default:
                        throw new UDFArgumentTypeException(i, "Argument " + (i + 1) + " of function " + getFuncName() + " must be " +
                                serdeConstants.INT_TYPE_NAME + ", " +
                                serdeConstants.DOUBLE_TYPE_NAME + " or " +
                                serdeConstants.DECIMAL_TYPE_NAME +
                                ", but " + argOIs[i].getTypeName() + ".");
                }
                converters[i] = ObjectInspectorConverters.getConverter(inputOIs[i], PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
            } else {
                switch (primCategory) {
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
        }
        return this.getResultStructOI();
    }

    protected StandardStructObjectInspector getResultStructOI() {
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("sample");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        Double start = null;
        Double stop = null;
        int num = 50;

        switch (args.length) {
            case 2:
                if (args[0] != null) {
                    start = ((DoubleWritable) converters[0].convert(args[0])).get();
                }
                if (args[1] != null) {
                    stop = ((DoubleWritable) converters[1].convert(args[1])).get();
                }
                break;
            case 3:
                if (args[0] != null) {
                    start = ((DoubleWritable) converters[0].convert(args[0])).get();
                }
                if (args[1] != null) {
                    stop = ((DoubleWritable) converters[1].convert(args[1])).get();
                }
                if (args[2] != null) {
                    num = ((IntWritable) converters[2].convert(args[2])).get();
                }
                break;
            default:
                throw new UDFArgumentLengthException("The function" + getFuncName() + " accepts one to three arguments");
        }

        if (start == null | stop == null) {
            return;
        }
        if (num == 0) {
            throw new UDFArgumentException("Argument " + 3 + " of function " + getFuncName() + " must not be zero.");
        }

        List<Double> samples = this.getSamples(start, stop, num);
        for (int i = 0; i < num; i++) {
            forwardObjects[0] = new DoubleWritable(samples.get(i));
            forward(forwardObjects);
        }
    }

    abstract List<Double> getSamples(double start, double stop, int num);

    @Override
    public void close() throws HiveException {
    }

    @Override
    public String toString() {
        return this.getFuncName();
    }

    protected String getFuncName() {
        return this.getClass().getSimpleName().toLowerCase();
    }
}
