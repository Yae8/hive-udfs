package hiveudfs.udaf.statistics;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;

public class Median extends AbstractGenericUDAFResolver {

    static final Logger LOG = LoggerFactory.getLogger(Median.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }
        switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
            case BOOLEAN:
                return new MedianEvaluatorBoolean();
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case STRING:
                return new MedianEvaluatorDouble();
            case DECIMAL:
                return new MedianEvaluatorDecimal();
            case DATE:
                return new MedianEvaluatorDate();
            case TIMESTAMP:
                return new MedianEvaluatorTimestamp();
            default:
                throw new UDFArgumentTypeException(0,
                        "Only numeric or date type arguments are accepted" +
                                ", but " + parameters[0].getCategory() + " is passed.");
        }
    }

    @UDFType(distinctLike = false)
    public static abstract class MedianEvaluatorBase <WritableType extends Writable> extends GenericUDAFEvaluator {

        // For PARTIAL1 and COMPLETE
        protected transient PrimitiveObjectInspector inputOI;

        // For PARTIAL2 and FINAL
        protected transient StructObjectInspector structOI;
        protected transient StructField containerField;
        protected transient ListObjectInspector containerFieldOI;

        // PARTIAL1 and PARTIAL2
        protected Object[] partialResult;

        // FINAL and COMPLETE
        protected WritableType result;

        boolean warned = false;

        @AggregationType(estimable = false)
        static class MedianAgg<Type> extends AbstractAggregationBuffer {
            ArrayList<Type> container;
        }
    }

    public static class MedianEvaluatorBoolean extends MedianEvaluatorBase<BooleanWritable> {

        protected transient StructField countFalseField;
        protected transient StructField countTrueField;
        protected transient IntObjectInspector countFalseFieldOI;
        protected transient IntObjectInspector countTrueFieldOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);

            result = new BooleanWritable();

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                structOI = (StructObjectInspector) parameters[0];

                countFalseField = structOI.getStructFieldRef("countFalse");
                countTrueField = structOI.getStructFieldRef("countTrue");

                countFalseFieldOI = (IntObjectInspector) countFalseField.getFieldObjectInspector();
                countTrueFieldOI = (IntObjectInspector) countTrueField.getFieldObjectInspector();
            }

            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                ArrayList<ObjectInspector> fieldOI = new ArrayList<>();
                fieldOI.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
                fieldOI.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);

                ArrayList<String> fieldName = new ArrayList<>();
                fieldName.add("countFalse");
                fieldName.add("countTrue");

                partialResult = new Object[2];
                partialResult[0] = new IntWritable();
                partialResult[1] = new IntWritable();

                return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldOI);
            } else {
                return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
            }
        }

        @AggregationType(estimable = false)
        static class MedianBooleanAgg extends AbstractAggregationBuffer {
            int countFalse;
            int countTrue;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MedianBooleanAgg buffer = new MedianBooleanAgg();
            reset(buffer);
            return buffer;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            MedianBooleanAgg myAgg = (MedianBooleanAgg) agg;
            myAgg.countFalse = 0;
            myAgg.countTrue = 0;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            if (parameters[0] == null) {
                return;
            }
            MedianBooleanAgg myAgg = (MedianBooleanAgg) agg;
            boolean value = PrimitiveObjectInspectorUtils.getBoolean(parameters[0], inputOI);
            if (value) {
                myAgg.countTrue++;
            } else {
                myAgg.countFalse++;
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MedianBooleanAgg myAgg = (MedianBooleanAgg) agg;

            partialResult[0] = new IntWritable();
            partialResult[1] = new IntWritable();

            ((IntWritable) partialResult[0]).set(myAgg.countFalse);
            ((IntWritable) partialResult[1]).set(myAgg.countTrue);
            return partialResult;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }
            MedianBooleanAgg myAgg = (MedianBooleanAgg) agg;
            Object partialCountFalse = structOI.getStructFieldData(partial, countFalseField);
            Object partialCountTrue = structOI.getStructFieldData(partial, countTrueField);

            int resultCountFalse = countFalseFieldOI.get(partialCountFalse);
            int resultCountTrue = countFalseFieldOI.get(partialCountTrue);
            myAgg.countFalse += resultCountFalse;
            myAgg.countTrue += resultCountTrue;
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MedianBooleanAgg myAgg = (MedianBooleanAgg) agg;

            int countFalse = myAgg.countFalse;
            int countTrue = myAgg.countTrue;

            if (countFalse == 0 && countTrue == 0) {
                return null;
            } else if (countFalse > countTrue) {
                result.set(false);
                return new BooleanWritable(false);
            } else if (countFalse < countTrue) {
                result.set(true);
                return new BooleanWritable(true);
            } else {
                result.set(false);
            }
            return result;
        }
    }

    public static class MedianEvaluatorDouble extends MedianEvaluatorBase<DoubleWritable> {

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);

            result = new DoubleWritable();

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                structOI = (StructObjectInspector) parameters[0];
                containerField = structOI.getStructFieldRef("container");
                containerFieldOI = (StandardListObjectInspector) containerField.getFieldObjectInspector();
            }

            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                ArrayList<ObjectInspector> fieldOI = new ArrayList<>();
                fieldOI.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));

                ArrayList<String> fieldName = new ArrayList<>();
                fieldName.add("container");

                partialResult = new Object[1];
                partialResult[0] = new ArrayList<DoubleWritable>();

                return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldOI);
            } else {
                return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            }
        }

        @AggregationType(estimable = false)
        static class MedianDoubleAgg extends MedianAgg<DoubleWritable> {
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MedianDoubleAgg buffer = new MedianDoubleAgg();
            reset(buffer);
            return buffer;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            MedianDoubleAgg myAgg = (MedianDoubleAgg) agg;
            myAgg.container = new ArrayList<>();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            if (parameters[0] == null) {
                return;
            }
            MedianDoubleAgg myAgg = (MedianDoubleAgg) agg;
            double value = .0;
            try {
                value = PrimitiveObjectInspectorUtils.getDouble(parameters[0], inputOI);
            } catch (NullPointerException e) {
                LOG.warn("got a null value, skip it");
            } catch (NumberFormatException e) {
                if (!warned) {
                    warned = true;
                    LOG.warn(getClass().getSimpleName() + " " + StringUtils.stringifyException(e));
                    LOG.warn("ignore similar exceptions.");
                }
            }
            myAgg.container.add(new DoubleWritable(value));
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MedianDoubleAgg myAgg = (MedianDoubleAgg) agg;

            partialResult[0] = new ArrayList<DoubleWritable>(myAgg.container.size());
            ((ArrayList<DoubleWritable>)partialResult[0]).addAll(myAgg.container);
            return partialResult;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }
            MedianDoubleAgg myAgg = (MedianDoubleAgg) agg;
            Object partialContainer = structOI.getStructFieldData(partial, containerField);
            ArrayList<DoubleWritable> resultContainer = (ArrayList<DoubleWritable>) containerFieldOI.getList(partialContainer);
            myAgg.container.addAll(resultContainer);
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MedianDoubleAgg myAgg = (MedianDoubleAgg) agg;
            Collections.sort(myAgg.container);
            int size = myAgg.container.size();
            if (size == 0) {
                return null;
            }
            if (size % 2 == 1) {
                double value = myAgg.container.get(size / 2).get();
                result.set(value);
            } else {
                double value1 = myAgg.container.get(size / 2 - 1).get();
                double value2 = myAgg.container.get(size / 2).get();
                result.set((value1 + value2) / 2L);
            }
            return result;
        }
    }

    public static class MedianEvaluatorDecimal extends MedianEvaluatorBase<HiveDecimalWritable> {

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);

            result = new HiveDecimalWritable();

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                structOI = (StructObjectInspector) parameters[0];
                containerField = structOI.getStructFieldRef("container");
                containerFieldOI = (StandardListObjectInspector) containerField.getFieldObjectInspector();
            }

            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                ArrayList<ObjectInspector> fieldOI = new ArrayList<>();
                fieldOI.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector));

                ArrayList<String> fieldName = new ArrayList<>();
                fieldName.add("container");

                partialResult = new Object[1];
                partialResult[0] = new ArrayList<DoubleWritable>();

                return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldOI);
            } else {
                return PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector;
            }
        }

        @AggregationType(estimable = false)
        static class MedianDecimalAgg extends MedianAgg<HiveDecimalWritable> {
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MedianDecimalAgg buffer = new MedianDecimalAgg();
            reset(buffer);
            return buffer;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            MedianDecimalAgg myAgg = (MedianDecimalAgg) agg;
            myAgg.container = new ArrayList<>();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            if (parameters[0] == null) {
                return;
            }
            MedianDecimalAgg myAgg = (MedianDecimalAgg) agg;
            HiveDecimal value = HiveDecimal.create(0);
            try {
                value = PrimitiveObjectInspectorUtils.getHiveDecimal(parameters[0], inputOI);
            } catch (NullPointerException e) {
                LOG.warn("got a null value, skip it");
            } catch (NumberFormatException e) {
                if (!warned) {
                    warned = true;
                    LOG.warn(getClass().getSimpleName() + " " + StringUtils.stringifyException(e));
                    LOG.warn("ignore similar exceptions.");
                }
            }
            myAgg.container.add(new HiveDecimalWritable(value));
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MedianDecimalAgg myAgg = (MedianDecimalAgg) agg;

            partialResult[0] = new ArrayList<HiveDecimalWritable>(myAgg.container.size());
            ((ArrayList<HiveDecimalWritable>)partialResult[0]).addAll(myAgg.container);
            return partialResult;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }
            MedianDecimalAgg myAgg = (MedianDecimalAgg) agg;
            Object partialContainer = structOI.getStructFieldData(partial, containerField);
            ArrayList<HiveDecimalWritable> resultContainer = (ArrayList<HiveDecimalWritable>) containerFieldOI.getList(partialContainer);
            myAgg.container.addAll(resultContainer);
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MedianDecimalAgg myAgg = (MedianDecimalAgg) agg;
            Collections.sort(myAgg.container);
            int size = myAgg.container.size();
            if (size == 0) {
                return null;
            }
            if (size % 2 == 1) {
                HiveDecimal hiveDecimal = myAgg.container.get(size / 2).getHiveDecimal();
                result.set(hiveDecimal);
            } else {
                HiveDecimal hiveDecimal1 = (myAgg.container.get(size / 2 - 1).getHiveDecimal());
                HiveDecimal hiveDecimal2 = (myAgg.container.get(size / 2).getHiveDecimal());
                result.set(hiveDecimal1.add(hiveDecimal2).divide(HiveDecimal.create(2)));
            }
            return result;
        }
    }

    public static class MedianEvaluatorDate extends MedianEvaluatorBase<DateWritable> {

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);

            result = new DateWritable();

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                structOI = (StructObjectInspector) parameters[0];
                containerField = structOI.getStructFieldRef("container");
                containerFieldOI = (StandardListObjectInspector) containerField.getFieldObjectInspector();
            }

            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                ArrayList<ObjectInspector> fieldOI = new ArrayList<>();
                fieldOI.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDateObjectInspector));

                ArrayList<String> fieldName = new ArrayList<>();
                fieldName.add("container");

                partialResult = new Object[1];
                partialResult[0] = new ArrayList<DateWritable>();

                return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldOI);
            } else {
                return PrimitiveObjectInspectorFactory.writableDateObjectInspector;
            }
        }

        @AggregationType(estimable = false)
        static class MedianDateAgg extends MedianAgg<DateWritable> {
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MedianDateAgg buffer = new MedianDateAgg();
            reset(buffer);
            return buffer;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            MedianDateAgg myAgg = (MedianDateAgg) agg;
            myAgg.container = new ArrayList<>();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            if (parameters[0] == null) {
                return;
            }
            MedianDateAgg myAgg = (MedianDateAgg) agg;
            DateWritable value = null;
            try {
                value = new DateWritable((DateWritable)inputOI.getPrimitiveWritableObject(parameters[0]));
            } catch (NullPointerException e) {
                LOG.warn("got a null value, skip it");
            } catch (NumberFormatException e) {
                if (!warned) {
                    warned = true;
                    LOG.warn(getClass().getSimpleName() + " " + StringUtils.stringifyException(e));
                    LOG.warn("ignore similar exceptions.");
                }
            }
            if (value != null) {
                myAgg.container.add(value);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MedianDateAgg myAgg = (MedianDateAgg) agg;

            partialResult[0] = new ArrayList<DateWritable>(myAgg.container.size());
            ((ArrayList<DateWritable>)partialResult[0]).addAll(myAgg.container);
            return partialResult;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }
            MedianDateAgg myAgg = (MedianDateAgg) agg;
            Object partialContainer = structOI.getStructFieldData(partial, containerField);
            ArrayList<DateWritable> resultContainer = (ArrayList<DateWritable>) containerFieldOI.getList(partialContainer);
            myAgg.container.addAll(resultContainer);
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MedianDateAgg myAgg = (MedianDateAgg) agg;
            Collections.sort(myAgg.container);
            int size = myAgg.container.size();
            if (size == 0) {
                return null;
            }
            if (size % 2 == 1) {
                Date date = myAgg.container.get(size / 2).get();
                result.set(date);
            } else {
                Date date1 = myAgg.container.get(size / 2 - 1).get();
                Date date2 = myAgg.container.get(size / 2).get();
                result.set(new Date((date1.getTime() + date2.getTime()) / 2));
            }
            return result;
        }
    }

    public static class MedianEvaluatorTimestamp extends MedianEvaluatorBase<TimestampWritable> {

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);

            result = new TimestampWritable();

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                structOI = (StructObjectInspector) parameters[0];
                containerField = structOI.getStructFieldRef("container");
                containerFieldOI = (StandardListObjectInspector) containerField.getFieldObjectInspector();
            }

            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                ArrayList<ObjectInspector> fieldOI = new ArrayList<>();
                fieldOI.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableTimestampObjectInspector));

                ArrayList<String> fieldName = new ArrayList<>();
                fieldName.add("container");

                partialResult = new Object[1];
                partialResult[0] = new ArrayList<TimestampWritable>();

                return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldOI);
            } else {
                return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
            }
        }

        @AggregationType(estimable = false)
        static class MedianTimestampAgg extends MedianAgg<TimestampWritable> {
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MedianTimestampAgg buffer = new MedianTimestampAgg();
            reset(buffer);
            return buffer;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            MedianTimestampAgg myAgg = (MedianTimestampAgg) agg;
            myAgg.container = new ArrayList<>();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            if (parameters[0] == null) {
                return;
            }
            MedianTimestampAgg myAgg = (MedianTimestampAgg) agg;
            TimestampWritable value = null;
            try {
                value = new TimestampWritable((TimestampWritable)inputOI.getPrimitiveWritableObject(parameters[0]));
            } catch (NullPointerException e) {
                LOG.warn("got a null value, skip it");
            } catch (NumberFormatException e) {
                if (!warned) {
                    warned = true;
                    LOG.warn(getClass().getSimpleName() + " " + StringUtils.stringifyException(e));
                    LOG.warn("ignore similar exceptions.");
                }
            }
            if (value != null) {
                myAgg.container.add(value);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MedianTimestampAgg myAgg = (MedianTimestampAgg) agg;

            partialResult[0] = new ArrayList<TimestampWritable>(myAgg.container.size());
            ((ArrayList<TimestampWritable>) partialResult[0]).addAll(myAgg.container);
            return partialResult;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }
            MedianTimestampAgg myAgg = (MedianTimestampAgg) agg;
            Object partialContainer = structOI.getStructFieldData(partial, containerField);
            ArrayList<TimestampWritable> resultContainer = (ArrayList<TimestampWritable>) containerFieldOI.getList(partialContainer);
            myAgg.container.addAll(resultContainer);
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MedianTimestampAgg myAgg = (MedianTimestampAgg) agg;
            Collections.sort(myAgg.container);
            int size = myAgg.container.size();
            if (size == 0) {
                return null;
            }
            if (size % 2 == 1) {
                Timestamp timestamp = myAgg.container.get(size / 2).getTimestamp();
                result.set(timestamp);
            } else {
                Timestamp timestamp1 = myAgg.container.get(size / 2 - 1).getTimestamp();
                Timestamp timestamp2 = myAgg.container.get(size / 2).getTimestamp();
                result.set(new Timestamp((timestamp1.getTime() + timestamp2.getTime()) / 2L));
            }
            return result;
        }
    }
}
