package hiveudfs.udaf.statistics;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CountFreq extends CounterBase {

    static final Logger LOG = LoggerFactory.getLogger(CountFreq.class.getName());

    @Override
    protected GenericUDAFEvaluator getMyEvaluator() {
        return new CountFreqEvaluator();
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        return super.getEvaluator(parameters);
    }

    public static class CountFreqEvaluator extends CounterEvaluatorBase {

        @Override
        protected ObjectInspector getCompleteReturnType() {
            return ObjectInspectorFactory.getStandardMapObjectInspector(
                    inputOI,
                    PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        }

        @Override
        protected ObjectInspector getFinalReturnType() {
            return ObjectInspectorFactory.getStandardMapObjectInspector(
                    mapperFieldOI.getMapKeyObjectInspector(),
                    PrimitiveObjectInspectorFactory.writableIntObjectInspector);
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
