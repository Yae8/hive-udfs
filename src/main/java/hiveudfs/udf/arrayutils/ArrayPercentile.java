package hiveudfs.udf.arrayutils;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BooleanWritable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ArrayPercentile extends ArrayMetricBase {

    protected transient PrimitiveObjectInspector percentileOI;
    protected transient Converter percentileConverter;
    protected transient PrimitiveCategory percentileCategory;
    protected transient BooleanObjectInspector isLinerInterpolationOI;
    protected transient boolean isLinerInterpolation = true;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2 || arguments.length > 3) {
            throw new UDFArgumentLengthException("The function " + getUdfName() + " takes two or three arguments.");
        }
        super.initialize(arguments);

        if (arguments[1].getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(1, "Argument " + 2 + " of function " + getUdfName() + " only takes a primitive" +
                    ", but " + arguments[1].getTypeName() + ".");
        }
        percentileOI = (PrimitiveObjectInspector) arguments[1];
        percentileCategory = percentileOI.getPrimitiveCategory();
        switch (percentileCategory) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                break;
            default:
                throw new UDFArgumentTypeException(1, "Argument " + 2 + " of function " + getUdfName() + " only takes a floating point or decimal" +
                        ", but " + arguments[1].getTypeName() + ".");
        }
        percentileConverter = ObjectInspectorConverters.getConverter(percentileOI, PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);

        if (arguments.length == 3) {
            if (arguments[2].getCategory() != Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(2, "Argument " + 3 + " of function " + getUdfName() + " only takes a primitive" +
                        ", but " + arguments[2].getTypeName() + ".");
            }
            if (((PrimitiveObjectInspector) arguments[2]).getPrimitiveCategory() != PrimitiveCategory.BOOLEAN) {
                throw new UDFArgumentTypeException(2, "Argument " + 3 + " of function " + getUdfName() + " only takes a boolean" +
                        ", but " + arguments[2].getTypeName() + ".");
            }
            isLinerInterpolationOI = (BooleanObjectInspector) arguments[2];
        }

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
        if (arguments[0] == null || arguments[1] == null) {
            return null;
        }

        List<?> list = listOI.getList(arguments[0].get());
        double percentile = ((DoubleWritable) percentileConverter.convert(arguments[1].get())).get();
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
        }

        BigDecimal bigDecimal = new BigDecimal(String.valueOf((countTrue + countFalse + 1) * percentile / 100));
        double falseRatio = (double)countFalse / (countFalse + countTrue);

        if (bigDecimal.intValue() + 1 <= falseRatio) {
            return new BooleanWritable(false);
        } else if (bigDecimal.intValue() > falseRatio) {
            return new BooleanWritable(true);
        } else if (bigDecimal.floatValue() < 0.5) {
            return new BooleanWritable(false);
        } else {
            return new BooleanWritable(true);
        }
    }

    public Object evaluateNumeric(DeferredObject[] arguments) throws HiveException {
        if (arguments[0] == null || arguments[1] == null) {
            return null;
        }

        List<?> list = listOI.getList(arguments[0].get());
        double percentile = ((DoubleWritable) percentileConverter.convert(arguments[1].get())).get();
        if (arguments.length == 3) {
            isLinerInterpolation = isLinerInterpolationOI.get(arguments[2].get());
        }
        if (list == null) {
            return null;
        }
        if (list.size() <= 0) {
            return null;
        }

        ArrayList<Double> newList = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            Object element = listOI.getListElement(list, i);
            if (element == null) {
                continue;
            }
            double value = PrimitiveObjectInspectorUtils.getDouble(listOI.getListElement(list, i), elementOI);
            newList.add(value);
        }
        if (newList.size() == 0) {
            return null;
        }

        Collections.sort(newList);
        double n = percentile * (newList.size() - 1);
        int q = (int) Math.floor(n);
        double r = n - q;
        if (q < 0) {
            return null;
        }
        if (q >= newList.size() - 1) {
            return new DoubleWritable(newList.get(q));
        }
        double result;
        if (isLinerInterpolation) {
            double value1 = newList.get(q);
            double value2 = newList.get(q + 1);
            result = value1 + (value2 - value1) * r;
        } else {
            result = newList.get(q);
        }

        return new DoubleWritable(result);
    }

    public Object evaluateDecimal(DeferredObject[] arguments) throws HiveException {
        if (arguments[0] == null || arguments[1] == null) {
            return null;
        }

        List<?> list = listOI.getList(arguments[0].get());
        double percentile = ((DoubleWritable) percentileConverter.convert(arguments[1].get())).get();
        if (arguments.length == 3) {
            isLinerInterpolation = isLinerInterpolationOI.get(arguments[2].get());
        }
        if (list == null) {
            return null;
        }
        if (list.size() <= 0) {
            return null;
        }

        ArrayList<HiveDecimal> newList = new ArrayList<>();
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
        double n = percentile * (newList.size() - 1);
        int q = (int) Math.floor(n);
        double r = n - q;
        if (q < 0) {
            return null;
        }
        if (q >= newList.size() - 1) {
            return new HiveDecimalWritable(newList.get(q));
        }
        HiveDecimal result;
        if (isLinerInterpolation) {
            HiveDecimal value1 = newList.get(q);
            HiveDecimal value2 = newList.get(q + 1);
            result = value1.add(value2.subtract(value1).multiply(HiveDecimal.create(new BigDecimal(r))));
        } else {
            result = newList.get(q);
        }

        return new HiveDecimalWritable(result);
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
