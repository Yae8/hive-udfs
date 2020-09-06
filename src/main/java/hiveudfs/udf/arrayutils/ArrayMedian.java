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
import org.apache.hadoop.io.BooleanWritable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ArrayMedian extends ArrayMetricBase {

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("The function " + getUdfName() + " takes one arguments.");
        }
        super.initialize(arguments);
        switch (elementCategory) {
            case BOOLEAN:
                return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case STRING:
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
            case BOOLEAN:
                return evaluateBoolean(arguments);
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case STRING:
                return evaluateNumeric(arguments);
            case DECIMAL:
                return evaluateDecimal(arguments);
            default:
                throw new UDFArgumentException("The function " + getUdfName() + " takes only array of numeric.");
        }
    }

    public Object evaluateBoolean(DeferredObject[] arguments) throws HiveException {
        List<?> list = listOI.getList(arguments[0].get());
        if (list == null) {
            return null;
        }
        if (list.size() <= 0) {
            return null;
        }

        int countTrue = 0;
        int countFalse = 0;
        for (int i = 0; i < list.size(); i++) {
            Object element = listOI.getListElement(list, i);
            if (element == null) {
                continue;
            }
            boolean value = PrimitiveObjectInspectorUtils.getBoolean(listOI.getListElement(list, i), elementOI);
            if (value) {
                countTrue += 1;
            } else {
                countFalse += 1;
            }
        }
        if (countTrue == 0 && countFalse == 0) {
            return null;
        } else if (countTrue > countFalse) {
            return new BooleanWritable(true);
        } else if (countTrue < countFalse) {
            return new BooleanWritable(false);
        } else {
            return new BooleanWritable(false);
        }
    }

    public Object evaluateNumeric(DeferredObject[] arguments) throws HiveException {
        List<?> list = listOI.getList(arguments[0].get());
        if (list == null) {
            return null;
        }
        if (list.size() <= 0) {
            return null;
        }

        List<Double> newList = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            Object element = listOI.getListElement(list, i);
            if (element == null) {
                continue;
            }
            Double value = PrimitiveObjectInspectorUtils.getDouble(listOI.getListElement(list, i), elementOI);
            newList.add(value);
        }
        if (newList.size() == 0) {
            return null;
        }
        Collections.sort(newList);
        if (newList.size() % 2 != 0) {
            return new DoubleWritable(newList.get(newList.size() / 2));
        } else {
            return new DoubleWritable((newList.get(newList.size() / 2 - 1) + newList.get(newList.size() / 2)) / 2.);
        }
    }

    public Object evaluateDecimal(DeferredObject[] arguments) throws HiveException {
        List<?> list = listOI.getList(arguments[0].get());

        if (list == null) {
            return null;
        }
        if (list.size() <= 0) {
            return null;
        }

        List<HiveDecimal> newList = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            Object element = listOI.getListElement(list, i);
            if (element == null) {
                continue;
            }
            HiveDecimal value = PrimitiveObjectInspectorUtils.getHiveDecimal(listOI.getListElement(list, i), elementOI);
            newList.add(value);
        }
        if (newList.size() == 0) {
            return null;
        }
        Collections.sort(newList);
        if (newList.size() % 2 != 0) {
            return new HiveDecimalWritable(newList.get(newList.size() / 2));
        } else {
            HiveDecimal value1 = newList.get(newList.size() / 2 - 1);
            HiveDecimal value2 = newList.get(newList.size() / 2);
            return new HiveDecimalWritable(value1.add(value2).divide(HiveDecimal.create(2)));
        }
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
