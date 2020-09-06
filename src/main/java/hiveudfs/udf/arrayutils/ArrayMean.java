package hiveudfs.udf.arrayutils;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import java.util.List;

public class ArrayMean extends ArrayMetricBase {

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
    public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {
        switch (elementCategory) {
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                return evaluateDouble(arguments);
            case DECIMAL:
                return evaluateDecimal(arguments);
            default:
                throw new UDFArgumentException("The function " + getUdfName() + " takes only array of numeric.");
        }
    }

    public Object evaluateDouble(GenericUDF.DeferredObject[] arguments) throws HiveException {
        List<?> list = listOI.getList(arguments[0].get());
        if (list == null) {
            return null;
        }
        if (list.size() <= 0) {
            return null;
        }

        int count = 0;
        double sum = .0;
        for (int i = 0; i < list.size(); i++) {
            Object element = listOI.getListElement(list, i);
            if (element == null) {
                continue;
            }
            double value = PrimitiveObjectInspectorUtils.getDouble(listOI.getListElement(list, i), elementOI);
            count += 1;
            sum += value;
        }
        if (count == 0) {
            return null;
        }
        return new DoubleWritable(sum / count);
    }

    public Object evaluateDecimal(GenericUDF.DeferredObject[] arguments) throws HiveException {
        List<?> list = listOI.getList(arguments[0].get());
        if (list == null) {
            return null;
        }
        if (list.size() <= 0) {
            return null;
        }
        if (list.size() == 1) {
            HiveDecimal value = PrimitiveObjectInspectorUtils.getHiveDecimal(listOI.getListElement(list, 0), elementOI);
            return new HiveDecimalWritable(value);
        }

        int count = 0;
        HiveDecimal sum = HiveDecimal.ZERO;
        for (int i = 0; i < list.size(); i++) {
            Object element = listOI.getListElement(list, i);
            if (element == null) {
                continue;
            }
            HiveDecimal value = PrimitiveObjectInspectorUtils.getHiveDecimal(listOI.getListElement(list, i), elementOI);
            count += 1;
            sum.add(value);
        }
        if (count == 0) {
            return null;
        }
        return new HiveDecimalWritable(sum.divide(HiveDecimal.create(count)));
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
