package hiveudfs.udaf.bitwise;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class BitwiseAndTest {
    private BitwiseAnd udaf;

    @Before
    public void before() {
        udaf = new BitwiseAnd();
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testGetEvaluatorNoArguments() throws SemanticException {
        udaf.getEvaluator(new StructTypeInfo[0]);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testGetEvaluatorWithIntType() throws SemanticException {
        ObjectInspector[] params = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector};

        SimpleGenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, false, false);
        GenericUDAFEvaluator evaluator = udaf.getEvaluator(info);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testCompleteWithLongType() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaLongObjectInspector,
                PrimitiveObjectInspectorFactory.javaLongObjectInspector};

        SimpleGenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, false, false);
        GenericUDAFEvaluator evaluator = udaf.getEvaluator(info);
        AggregationBuffer agg;

        List<Object[]> rowSet = new ArrayList<>();
        long expected = Long.MAX_VALUE;
        Object actual;

        for (int i = 0; i < 100; i++) {
            ArrayList<Object> row = new ArrayList<>();
            row.add(i);
            rowSet.add(row.toArray());
            expected &= i;
        }

        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, info.getParameterObjectInspectors());
        agg = evaluator.getNewAggregationBuffer();
        for (Object[] parameters : rowSet) {
            evaluator.iterate(agg, parameters);
        }
        actual = evaluator.terminate(agg);
        assertNotNull(actual);
        assertEquals(expected, actual);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testFinalWithLongType() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaLongObjectInspector,
                PrimitiveObjectInspectorFactory.javaLongObjectInspector};

        SimpleGenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, false, false);
        GenericUDAFEvaluator evaluator = udaf.getEvaluator(info);
        AggregationBuffer agg;

        List<Object[]> rowSet = new ArrayList<>();
        long expected = Long.MAX_VALUE;
        Object actual;

        for (int i = 0; i < 100; i++) {
            ArrayList<Object> row = new ArrayList<>();
            row.add(i);
            rowSet.add(row.toArray());
            expected &= i;
        }

        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, info.getParameterObjectInspectors());
        agg = evaluator.getNewAggregationBuffer();


        for (Object[] parameters : rowSet) {
            evaluator.iterate(agg, parameters);
        }
        actual = evaluator.terminate(agg);
        assertNotNull(actual);
        assertEquals(expected, actual);
    }


    @Test(expected = UDFArgumentTypeException.class)
    public void testPartial1WithLongType() throws Exception {
        List<Object[]> rowSet = new ArrayList<>();
        long expected = Long.MAX_VALUE;

        for (int i=0; i<100; i++) {
            ArrayList<Object> row = new ArrayList<>();
            row.add(i);
            rowSet.add(row.toArray());
            expected &= i;
        }

        for (Object partial1Result : runPartial1WithLongType(rowSet)) {

        }

        List<Object> actual = runPartial1WithLongType(rowSet);
    }

    public List<Object> runPartial1WithLongType(List<Object[]> rowSet) throws Exception {
        ObjectInspector[] params = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaLongObjectInspector,
                PrimitiveObjectInspectorFactory.javaLongObjectInspector};

        SimpleGenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, false, false);
        int batchSize = 1;

        List<Object> result = new ArrayList<>();
        Iterator<Object[]> iter = rowSet.iterator();
        do {
            GenericUDAFEvaluator evaluator = udaf.getEvaluator(info);
            evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, info.getParameterObjectInspectors());
            AggregationBuffer agg = evaluator.getNewAggregationBuffer();
            for (int i=0; i<batchSize -1 && iter.hasNext(); i++) {
                evaluator.iterate(agg, iter.next());
            }
            batchSize <<= 1;
            result.add(evaluator.terminatePartial(agg));
        } while (iter.hasNext());
        return result;
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testPartial2WithLongType() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaLongObjectInspector,
                PrimitiveObjectInspectorFactory.javaLongObjectInspector};

        SimpleGenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, false, false);
        int batchSize = 1;

        List<Object[]> rowSet = new ArrayList<>();
        long expected = Long.MAX_VALUE;
        Object actual;

        for (int i=0; i<100; i++) {
            ArrayList<Object> row = new ArrayList<>();
            row.add(i);
            rowSet.add(row.toArray());
            expected &= i;
        }

        Iterator<Object[]> iter = rowSet.iterator();
        do {
            GenericUDAFEvaluator evaluator = udaf.getEvaluator(info);
            evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL2, info.getParameterObjectInspectors());
            AggregationBuffer agg = evaluator.getNewAggregationBuffer();
            for (int i=0; i<batchSize -1 && iter.hasNext(); i++) {
                evaluator.merge(agg, iter.next());
            }
            batchSize <<= 1;
            actual = evaluator.terminatePartial(agg);
            assertNotNull(actual);
            assertEquals(expected, actual);
        } while (iter.hasNext());
    }

}
