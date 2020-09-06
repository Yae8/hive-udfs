package hiveudfs.udf.arrayutils;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.LongWritable;

import java.util.List;

public class ArraySum extends ArrayMetricBase {

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("The function " + getUdfName() + " takes one arguments.");
        }
        super.initialize(arguments);
        switch (elementCategory) {
            case SHORT:
            case INT:
            case LONG:
                return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
            case FLOAT:
            case DOUBLE:
                return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            case DECIMAL:
                return PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector;
            default:
                throw new UDFArgumentException("The function " + getUdfName() + " takes only array of numeric.");
        }
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        switch (elementCategory) {
            case SHORT:
            case INT:
            case LONG:
                return evaluateLong(arguments);
            case FLOAT:
            case DOUBLE:
                return evaluateDouble(arguments);
            case DECIMAL:
                return evaluateDecimal(arguments);
            default:
                throw new UDFArgumentException("The function " + getUdfName() + " takes only array of numeric.");
        }
    }

    public Object evaluateLong(DeferredObject[] arguments) throws HiveException {
        List<?> list = listOI.getList(arguments[0].get());
        if (list == null) {
            return null;
        }
        if (list.size() <= 0) {
            return null;
        }

        boolean empty = true;
        long sum = 0;
        for (int i = 0; i < list.size(); i++) {
            Object element = listOI.getListElement(list, i);
            if (element == null) {
                continue;
            }
            long value = PrimitiveObjectInspectorUtils.getLong(listOI.getListElement(list, i), elementOI);
            empty = false;
            sum += value;
        }
        if (empty) {
            return null;
        }
        return new LongWritable(sum);
    }

    public Object evaluateDouble(DeferredObject[] arguments) throws HiveException {
        List<?> list = listOI.getList(arguments[0].get());
        if (list == null) {
            return null;
        }
        if (list.size() <= 0) {
            return null;
        }

        boolean empty = true;
        double sum = .0;
        for (int i = 0; i < list.size(); i++) {
            Object element = listOI.getListElement(list, i);
            if (element == null) {
                continue;
            }
            double value = PrimitiveObjectInspectorUtils.getDouble(listOI.getListElement(list, i), elementOI);
            empty = false;
            sum += value;
        }
        if (empty) {
            return null;
        }
        return new DoubleWritable(sum);
    }

    public Object evaluateDecimal(DeferredObject[] arguments) throws HiveException {
        List<?> list = listOI.getList(arguments[0].get());
        if (list == null) {
            return null;
        }
        if (list.size() <= 0) {
            return null;
        }

        boolean empty = true;
        HiveDecimal sum = HiveDecimal.create(0);
        for (int i = 0; i < list.size(); i++) {
            Object element = listOI.getListElement(list, i);
            if (element == null) {
                continue;
            }
            HiveDecimal value = PrimitiveObjectInspectorUtils.getHiveDecimal(listOI.getListElement(list, i), elementOI);
            empty = false;
            sum.add(value);
        }
        if (empty) {
            return null;
        }
        return new HiveDecimalWritable(sum);
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
