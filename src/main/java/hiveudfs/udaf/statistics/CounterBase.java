package hiveudfs.udaf.statistics;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

abstract class CounterBase extends AbstractGenericUDAFResolver {

    abstract protected GenericUDAFEvaluator getMyEvaluator();

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                "Exactly one argument is expected.");
        }
        if (parameters[0].getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                "Only a primitive type argument is accepted" +
                ", but " + parameters[0].getCategory() + " was passed.");
        }
        switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case STRING:
            case DECIMAL:
            case DATE:
            case TIMESTAMP:
                return this.getMyEvaluator();
            default:
                throw new UDFArgumentTypeException(0,
                    "Only a numeric or date type argument is accepted" +
                    ", but " + parameters[0].getCategory() + " was passed.");
        }
    }

    abstract static class CounterEvaluatorBase extends GenericUDAFEvaluator {

        // For PARTIAL1 and COMPLETE
        protected transient PrimitiveObjectInspector inputOI;

        // For PARTIAL2 and FINAL
        protected transient StructObjectInspector structOI;
        protected transient StructField counterField;
        protected transient MapObjectInspector counterFieldOI;

        // PARTIAL1 and PARTIAL2
        protected Object[] partialResult;

        boolean warned = false;

        abstract protected ObjectInspector getCompleteReturnType();
        abstract protected ObjectInspector getFinalReturnType();

        @AggregationType(estimable = false)
        static class CounterAgg extends AbstractAggregationBuffer {
            HashMap<Object, Integer> counter;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            CounterAgg buffer = new CounterAgg();
            reset(buffer);
            return buffer;
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                structOI = (StructObjectInspector) parameters[0];
                counterField = structOI.getStructFieldRef("counter");
                counterFieldOI = (StandardMapObjectInspector) counterField.getFieldObjectInspector();
            }

            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                ArrayList<ObjectInspector> fieldOI = new ArrayList<>();
                fieldOI.add(ObjectInspectorFactory.getStandardMapObjectInspector(
                    inputOI, PrimitiveObjectInspectorFactory.writableIntObjectInspector));

                ArrayList<String> fieldName = new ArrayList<>();
                fieldName.add("counter");

                partialResult = new Object[1];
                partialResult[0] = new HashMap<Object, Integer>();

                return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldOI);
            } else if (m == Mode.COMPLETE) {
                return this.getCompleteReturnType();
            } else {
                return this.getFinalReturnType();
            }
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            CounterAgg myAgg = (CounterAgg) agg;
            myAgg.counter = new HashMap<>();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            if (parameters[0] == null) {
                return;
            }
            CounterAgg myAgg = (CounterAgg) agg;

            Object key = ObjectInspectorUtils.copyToStandardObject(parameters[0], inputOI);
            int count = myAgg.counter.getOrDefault(key, 0);
            myAgg.counter.put(key, count + 1);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            CounterAgg myAgg = (CounterAgg) agg;
            HashMap<Object, IntWritable> partialResult = new HashMap<>(myAgg.counter.size());
            myAgg.counter.forEach((k, v) -> partialResult.put(k, new IntWritable(v)));
            return partialResult;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }
            CounterAgg myAgg = (CounterAgg) agg;
            Object partialCounter = structOI.getStructFieldData(partial, counterField);
            HashMap<Object, IntWritable> resultMapper =
                (HashMap<Object, IntWritable>) counterFieldOI.getMap(partialCounter);
            for (Map.Entry<Object, IntWritable> entry : resultMapper.entrySet()) {
                Object key = entry.getKey();
                int count = myAgg.counter.getOrDefault(key, 0);
                myAgg.counter.put(key, count + entry.getValue().get());
            }
        }
    }
}
