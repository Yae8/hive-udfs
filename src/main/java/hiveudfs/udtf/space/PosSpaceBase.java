package hiveudfs.udtf.space;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;

abstract class PosSpaceBase extends SpaceBase {
    protected transient Object[] forwardObjects = new Object[2];

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        return super.initialize(argOIs);
    }

    @Override
    protected StandardStructObjectInspector getResultStructOI() {
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("pos");
        fieldNames.add("sample");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        double start;
        double stop;
        int num;

        switch (args.length) {
            case 2:
                start = ((DoubleWritable) converters[0].convert(args[0])).get();
                stop = ((DoubleWritable) converters[1].convert(args[1])).get();
                num = 50;
                break;
            case 3:
                start = ((DoubleWritable) converters[0].convert(args[0])).get();
                stop = ((DoubleWritable) converters[1].convert(args[1])).get();
                num = ((IntWritable) converters[2].convert(args[2])).get();
                if (num == 0) {
                    throw new UDFArgumentException("Argument " + 3 + " of function " + getFuncName() + " must not be zero.");
                }
                break;
            default:
                throw new UDFArgumentLengthException("The function" + getFuncName() + " accepts one to three arguments");
        }

        List<Double> samples = this.getSamples(start, stop, num);
        for (int i = 0; i < num; i++) {
            forwardObjects[0] = new IntWritable(i + 1);
            forwardObjects[1] = new DoubleWritable(samples.get(i));
            forward(forwardObjects);
        }
    }
}
