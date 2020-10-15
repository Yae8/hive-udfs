package hiveudfs.udaf.statistics;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Frequency extends AbstractGenericUDAFResolver {

    static final Logger LOG = LoggerFactory.getLogger(Frequency.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
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
                return new FrequencyEvaluator();
            default:
                throw new UDFArgumentTypeException(0, "Only numeric or date type arguments are accepted, but " + parameters[0].getCategory() + " is passed.");
        }
    }

    public static class FrequencyEvaluator extends GenericUDAFEvaluator {

        // For PARTIAL1 and COMPLETE
        protected transient PrimitiveObjectInspector inputOI;

        // For PARTIAL2 and FINAL
        protected transient StructObjectInspector structOI;
        protected transient StructField mapperField;
        protected transient MapObjectInspector mapperFieldOI;

        // PARTIAL1 and PARTIAL2
        protected Object[] partialResult;

        boolean warned = false;

        @AggregationType(estimable = false)
        static class FrequencyAgg extends AbstractAggregationBuffer {
            HashMap<Object, Integer> mapper;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            FrequencyAgg buffer = new FrequencyAgg();
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
                mapperField = structOI.getStructFieldRef("mapper");
                mapperFieldOI = (StandardMapObjectInspector) mapperField.getFieldObjectInspector();
            }

            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                ArrayList<ObjectInspector> fieldOI = new ArrayList<>();
                fieldOI.add(ObjectInspectorFactory.getStandardMapObjectInspector(inputOI, PrimitiveObjectInspectorFactory.writableIntObjectInspector));

                ArrayList<String> fieldName = new ArrayList<>();
                fieldName.add("mapper");

                partialResult = new Object[1];
                partialResult[0] = new HashMap<Object, Integer>();

                return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldOI);
            } else if (m == Mode.COMPLETE) {
                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        inputOI,
                        PrimitiveObjectInspectorFactory.writableIntObjectInspector);
            } else {
                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        mapperFieldOI.getMapKeyObjectInspector(),
                        PrimitiveObjectInspectorFactory.writableIntObjectInspector);
            }
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            FrequencyAgg myAgg = (FrequencyAgg) agg;
            myAgg.mapper = new HashMap<>();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            if (parameters[0] == null) {
                return;
            }
            FrequencyAgg myAgg = (FrequencyAgg) agg;

            Object key = ObjectInspectorUtils.copyToStandardObject(parameters[0], inputOI);
            int count = myAgg.mapper.getOrDefault(key, 0);
            myAgg.mapper.put(key, count + 1);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            FrequencyAgg myAgg = (FrequencyAgg) agg;
            HashMap<Object, IntWritable> partialResult = new HashMap<>(myAgg.mapper.size());
            myAgg.mapper.forEach((k, v) -> partialResult.put(k, new IntWritable(v)));
            return partialResult;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }
            FrequencyAgg myAgg = (FrequencyAgg) agg;
            Object partialMapper = structOI.getStructFieldData(partial, mapperField);
            HashMap<Object, IntWritable> resultMapper = (HashMap<Object, IntWritable>) mapperFieldOI.getMap(partialMapper);
            for (Map.Entry<Object, IntWritable> entry : resultMapper.entrySet()) {
                Object key = entry.getKey();
                int count = myAgg.mapper.getOrDefault(key, 0);
                myAgg.mapper.put(key, count + entry.getValue().get());
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            FrequencyAgg myAgg = (FrequencyAgg) agg;
            HashMap<Object, IntWritable> result = new HashMap<>(myAgg.mapper.size());
            myAgg.mapper.forEach((k, v) -> result.put(k, new IntWritable(v)));
            return result;
        }
    }
}
