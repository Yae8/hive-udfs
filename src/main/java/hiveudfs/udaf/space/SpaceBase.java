package hiveudfs.udaf.space;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

abstract class SpaceBase extends AbstractGenericUDAFResolver {

    abstract protected GenericUDAFEvaluator getIntEvaluator();
    abstract protected GenericUDAFEvaluator getDoubleEvaluator();
    abstract protected GenericUDAFEvaluator getDecimalEvaluator();

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 2) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                "Exactly two argument is expected.");
        }
        for (int i = 0; i < parameters.length; i++) {
            if (parameters[i].getCategory() != Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i,
                    "Only primitive type arguments are accepted" +
                    ", but " + parameters[i].getCategory() + " is passed.");
            }
        }
        switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                break;
            default:
                throw new UDFArgumentTypeException(0,
                    "Only a numeric type argument is accepted" +
                    ", but " + parameters[0].getCategory() + " was passed.");
        }
        switch (((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                break;
            default:
                throw new UDFArgumentTypeException(1,
                    "Only an integer type argument is accepted" +
                    ", but " + parameters[1].getCategory() + " was passed.");
        }
        switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return this.getIntEvaluator();
            case FLOAT:
            case DOUBLE:
                return this.getDoubleEvaluator();
            case DECIMAL:
                return this.getDecimalEvaluator();
            default:
                throw new UDFArgumentTypeException(0,
                    "Only a numeric type argument is accepted" +
                    ", but " + parameters[0].getCategory() + " was passed.");
        }
    }

    abstract static class SpaceEvaluatorBase extends GenericUDAFEvaluator {

        // For PARTIAL1 and COMPLETE
        protected transient PrimitiveObjectInspector inputOI;
        protected transient PrimitiveObjectInspector numOI;
        protected Integer num;

        // For PARTIAL2 and FINAL
        protected transient StructObjectInspector structOI;
        protected transient StructField counterField;
        protected transient StructField numField;
        protected transient MapObjectInspector counterFieldOI;
        protected transient IntObjectInspector numFieldOI;

        // PARTIAL1 and PARTIAL2
        protected Object[] partialResult;

        boolean warned = false;

        @AggregationType(estimable = false)
        static class SpaceAgg extends AbstractAggregationBuffer {
            HashMap<Object, Integer> counter;
            Integer num;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            SpaceAgg buffer = new SpaceAgg();
            reset(buffer);
            return buffer;
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 2);
            super.init(m, parameters);

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
                numOI = (PrimitiveObjectInspector) parameters[1];
                num = PrimitiveObjectInspectorUtils.getInt(
                    ((ConstantObjectInspector) numOI).getWritableConstantValue(),
                    numOI);
            } else {
                structOI = (StructObjectInspector) parameters[0];
                counterField = structOI.getStructFieldRef("counter");
                numField = structOI.getStructFieldRef("num");
                counterFieldOI = (StandardMapObjectInspector) counterField.getFieldObjectInspector();
                numFieldOI = (IntObjectInspector) numField.getFieldObjectInspector();
            }

            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                ArrayList<ObjectInspector> fieldOI = new ArrayList<>();
                fieldOI.add(ObjectInspectorFactory.getStandardMapObjectInspector(
                    inputOI, PrimitiveObjectInspectorFactory.writableIntObjectInspector));
                fieldOI.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);

                ArrayList<String> fieldName = new ArrayList<>();
                fieldName.add("counter");
                fieldName.add("num");

                partialResult = new Object[2];
                partialResult[0] = new HashMap<Object, Integer>();
                partialResult[1] = num;

                return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldOI);
            } else if (m == Mode.COMPLETE) {
                List<String> structFieldNames = new ArrayList<>();
                List<ObjectInspector> structFieldOIs = new ArrayList<>();

                structFieldNames.add("samples");
                structFieldNames.add("count");
                structFieldOIs.add(
                    ObjectInspectorFactory.getStandardListObjectInspector(inputOI));
                structFieldOIs.add(
                    ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableIntObjectInspector));

                return ObjectInspectorFactory.getStandardStructObjectInspector(
                    structFieldNames, structFieldOIs);
            } else {
                List<String> structFieldNames = new ArrayList<>();
                List<ObjectInspector> structFieldOIs = new ArrayList<>();

                structFieldNames.add("samples");
                structFieldNames.add("count");
                structFieldOIs.add(
                    ObjectInspectorFactory.getStandardListObjectInspector(
                        counterFieldOI.getMapKeyObjectInspector()));
                structFieldOIs.add(
                    ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableIntObjectInspector));

                return ObjectInspectorFactory.getStandardStructObjectInspector(
                    structFieldNames, structFieldOIs);
            }
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            SpaceAgg myAgg = (SpaceAgg) agg;
            myAgg.counter = new HashMap<Object, Integer>();
            myAgg.num = num;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 2);
            if (parameters[0] == null || parameters[1] == null) {
                return;
            }
            SpaceAgg myAgg = (SpaceAgg) agg;

            Object key = ObjectInspectorUtils.copyToStandardObject(parameters[0], inputOI);
            int count = myAgg.counter.getOrDefault(key, 0);
            myAgg.counter.put(key, count + 1);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            SpaceAgg myAgg = (SpaceAgg) agg;

            HashMap<Object, IntWritable> partialCounter = new HashMap<>(myAgg.counter.size());
            myAgg.counter.forEach((k, v) -> partialCounter.put(k, new IntWritable(v)));
            IntWritable partialNum = new IntWritable(myAgg.num);

            partialResult[0] = partialCounter;
            partialResult[1] = partialNum;

            return partialResult;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }
            SpaceAgg myAgg = (SpaceAgg) agg;
            Object partialCounter = structOI.getStructFieldData(partial, counterField);
            Object partialNum = structOI.getStructFieldData(partial, numField);
            HashMap<Object, IntWritable> resultCounter = (HashMap<Object, IntWritable>) counterFieldOI.getMap(partialCounter);
            int resultNum = numFieldOI.get(partialNum);

            for (Map.Entry<Object, IntWritable> entry : resultCounter.entrySet()) {
                Object key = entry.getKey();
                int count = myAgg.counter.getOrDefault(key, 0);
                myAgg.counter.put(key, count + entry.getValue().get());
            }
            myAgg.num = resultNum;
        }
    }
}
