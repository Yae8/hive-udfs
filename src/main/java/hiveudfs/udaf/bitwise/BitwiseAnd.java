package hiveudfs.udaf.bitwise;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BitwiseAnd extends AbstractGenericUDAFResolver {
    static final Logger LOG = LoggerFactory.getLogger(BitwiseAnd.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
           throw new UDFArgumentTypeException(parameters.length - 1,
                   "Only primitive type arguments are accepted" +
                   ", but " + parameters[0].getCategory() + " is passed.");
        }
        switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
                return new BitwiseAndEvaluator32();
            case LONG:
                return new BitwiseAndEvaluator64();
            default:
                throw new UDFArgumentTypeException(0,
                        "Only numeric type arguments are accepted" +
                        ", but " + parameters[0].getCategory() + " is passed.");
        }
    }

    public static abstract class BitwiseAndEvaluator<ResultType extends Writable> extends GenericUDAFEvaluator {
        protected PrimitiveObjectInspector inputOI;
        protected PrimitiveObjectInspector outputOI;
        protected ResultType result;

        static abstract class BitwiseAgg<T> extends AbstractAggregationBuffer {
            boolean empty;
            T bitwise;
        }
    }

    @UDFType(distinctLike = false)
    public static class BitwiseAndEvaluator32 extends BitwiseAndEvaluator<IntWritable> {

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            result = new IntWritable(Integer.MAX_VALUE);
            inputOI = (PrimitiveObjectInspector) parameters[0];
            ObjectInspector oi = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(inputOI.getTypeInfo());
            outputOI = (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(oi, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);

            return oi;
        }

        @AggregationType(estimable = false)
        static class Bitwise32Agg extends BitwiseAgg<Integer> {
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            Bitwise32Agg buffer = new Bitwise32Agg();
            reset(buffer);
            return buffer;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            Bitwise32Agg myAgg = (Bitwise32Agg) agg;
            myAgg.empty = true;
            myAgg.bitwise = Integer.MAX_VALUE;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            Bitwise32Agg myAgg = (Bitwise32Agg) agg;
            myAgg.empty = false;
            myAgg.bitwise &= PrimitiveObjectInspectorUtils.getInt(parameters[0], inputOI);
            myAgg.bitwise &= Integer.MAX_VALUE;
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) { return; }
            Bitwise32Agg myAgg = (Bitwise32Agg) agg;
            myAgg.empty = false;
            myAgg.bitwise &= PrimitiveObjectInspectorUtils.getInt(partial, inputOI);
            myAgg.bitwise &= Integer.MAX_VALUE;
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            Bitwise32Agg myAgg = (Bitwise32Agg) agg;
            if (myAgg.empty) { return null; }
            result.set(myAgg.bitwise);
            return result;
        }
    }

    @UDFType(distinctLike = false)
    public static class BitwiseAndEvaluator64 extends BitwiseAndEvaluator<LongWritable> {

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            result = new LongWritable(Long.MAX_VALUE);
            inputOI = (PrimitiveObjectInspector) parameters[0];
            ObjectInspector oi = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(inputOI.getTypeInfo());
            outputOI = (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(oi, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);

            return oi;
        }

        @AggregationType(estimable = false)
        static class Bitwise64Agg extends BitwiseAgg<Long> {
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            Bitwise64Agg result = new Bitwise64Agg();
            reset(result);
            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            Bitwise64Agg myAgg = (Bitwise64Agg) agg;
            myAgg.empty = true;
            myAgg.bitwise = Long.MAX_VALUE;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            Bitwise64Agg myAgg = (Bitwise64Agg) agg;
            myAgg.empty = false;
            myAgg.bitwise &= PrimitiveObjectInspectorUtils.getInt(parameters[0], inputOI);
            myAgg.bitwise &= Long.MAX_VALUE;
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                Bitwise64Agg myAgg = (Bitwise64Agg) agg;
                myAgg.empty = false;
                myAgg.bitwise &= PrimitiveObjectInspectorUtils.getInt(partial, inputOI);
                myAgg.bitwise &= Long.MAX_VALUE;
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            Bitwise64Agg myAgg = (Bitwise64Agg) agg;
            if (myAgg.empty) { return null; }
            result.set(myAgg.bitwise);
            return result;
        }
    }
}
