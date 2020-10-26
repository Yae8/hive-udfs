package hiveudfs.udaf.statistics;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ModeTest {
    private Mode udaf;

    @Before
    public void before() { udaf = new Mode(); }

    @Test
    public void testCompleteWithIntType() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaIntObjectInspector};

        SimpleGenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, false, false);
        GenericUDAFEvaluator evaluator = udaf.getEvaluator(info);
        GenericUDAFEvaluator.AggregationBuffer agg;

        List<Object[]> rowSet = new ArrayList<>();
        int expected = 5;
        Object actual;

        for (int i = 0; i < 100; i++) {
            ArrayList<Object> row = new ArrayList<>();
            row.add(i);
            rowSet.add(row.toArray());
            if (i == expected) {
                rowSet.add(row.toArray());
            }
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
}
