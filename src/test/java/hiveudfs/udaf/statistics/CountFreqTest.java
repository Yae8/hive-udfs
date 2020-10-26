package hiveudfs.udaf.statistics;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CountFreqTest {
    private CountFreq udaf;

    @Before
    public void before() { udaf = new CountFreq(); }

    @Test
    @SuppressWarnings("unchecked")
    public void testCompleteWithIntType() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaIntObjectInspector};

        SimpleGenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, false, false);
        GenericUDAFEvaluator evaluator = (CountFreq.CountFreqEvaluator) udaf.getEvaluator(info);
        GenericUDAFEvaluator.AggregationBuffer agg;

        List<Object[]> rowSet = new ArrayList<>();
        HashMap<Integer, Integer> expected = new HashMap<>();
        HashMap<Object, IntWritable> actual = new HashMap<>();
        Object result;

        for (int i = 0; i < 100; i++) {
            ArrayList<Object> row = new ArrayList<>();
            row.add(i);
            if (i < 25) {
                rowSet.add(row.toArray());
                expected.put(i, 1);
            } else if (i < 50) {
                rowSet.add(row.toArray());
                rowSet.add(row.toArray());
                expected.put(i, 2);
            } else if (i < 75) {
                rowSet.add(row.toArray());
                rowSet.add(row.toArray());
                rowSet.add(row.toArray());
                expected.put(i, 3);
            } else {
                rowSet.add(row.toArray());
                rowSet.add(row.toArray());
                rowSet.add(row.toArray());
                rowSet.add(row.toArray());
                expected.put(i, 4);
            }
        }

        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, info.getParameterObjectInspectors());
        agg = evaluator.getNewAggregationBuffer();
        for (Object[] parameters : rowSet) {
            evaluator.iterate(agg, parameters);
        }
        result = evaluator.terminate(agg);
        actual = (HashMap<Object, IntWritable>) result;

        assertNotNull(actual);
        assertEquals(expected.size(), actual.size());
        for (Map.Entry<Object, IntWritable> entry : actual.entrySet()) {
            assertTrue(expected.containsKey((Integer) entry.getKey()));
            assertEquals(expected.get((Integer) entry.getKey()), (Integer) entry.getValue().get());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCompleteWithStringType() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaStringObjectInspector};

        SimpleGenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, false, false);
        GenericUDAFEvaluator evaluator = udaf.getEvaluator(info);
        GenericUDAFEvaluator.AggregationBuffer agg;

        List<Object[]> rowSet = new ArrayList<>();
        HashMap<String, Integer> expected = new HashMap<>();
        HashMap<Object, IntWritable> actual = new HashMap<>();
        Object result;

        for (char c = 'a'; c < 'z'; c++) {
            ArrayList<Object> row = new ArrayList<>();
            row.add(c);
            if (c < 'd') {
                rowSet.add(row.toArray());
                expected.put(String.valueOf(c), 1);
            } else if (c < 'k') {
                rowSet.add(row.toArray());
                rowSet.add(row.toArray());
                expected.put(String.valueOf(c), 2);
            } else if (c < 'r') {
                rowSet.add(row.toArray());
                expected.put(String.valueOf(c), 1);
            } else {
                rowSet.add(row.toArray());
                rowSet.add(row.toArray());
                rowSet.add(row.toArray());
                expected.put(String.valueOf(c), 3);
            }
        }

        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, info.getParameterObjectInspectors());
        agg = evaluator.getNewAggregationBuffer();
        for (Object[] parameters : rowSet) {
            evaluator.iterate(agg, parameters);
        }
        result = evaluator.terminate(agg);
        actual = (HashMap<Object, IntWritable>) result;

        assertNotNull(actual);
        assertEquals(expected.size(), actual.size());
        for (Map.Entry<Object, IntWritable> entry : actual.entrySet()) {
            assertTrue(expected.containsKey(entry.getKey().toString()));
            assertEquals(expected.get(entry.getKey().toString()), (Integer) entry.getValue().get());
        }
    }
}
