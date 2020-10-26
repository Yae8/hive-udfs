package hiveudfs.udtf.series;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class IntRangeTest {
    private IntRange udtf;

    @Before
    public void before() {
        udtf = new IntRange();
    }

    @Test
    public void testWithOneArguments() throws HiveException {
        ObjectInspector[] argOIs = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaIntObjectInspector};
        ArrayList<Object> actual = new ArrayList<>();
        udtf.setCollector(new Collector() {
            @Override
            public void collect(Object input) throws HiveException {
                Object[] output = (Object[]) input;
                actual.add(output[0]);
            }
        });
        StructObjectInspector structOI = udtf.initialize(argOIs);
        assertEquals("struct<value:int>", structOI.getTypeName());

        actual.clear();
        udtf.process(new Object[] {1});
        assertEquals(1, actual.size());
        assertEquals(0, ((IntWritable) structOI.getStructFieldData(actual.get(0), structOI.getStructFieldRef("value"))).get());

        actual.clear();
        udtf.process(new Object[] {100});
        assertEquals(100, actual.size());
        for (int i = 0; i < actual.size(); i++) {
            assertEquals(i, ((IntWritable) structOI.getStructFieldData(actual.get(i), structOI.getStructFieldRef("value"))).get());
        }
    }

    @Test
    public void testWithTwoArguments() throws HiveException {
        ObjectInspector[] argOIs = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector};
        ArrayList<Object> actual = new ArrayList<>();
        udtf.setCollector(new Collector() {
            @Override
            public void collect(Object input) throws HiveException {
                Object[] output = (Object[]) input;
                actual.add(output[0]);
            }
        });
        StructObjectInspector structOI = udtf.initialize(argOIs);
        assertEquals("struct<value:int>", structOI.getTypeName());

        actual.clear();
        udtf.process(new Object[] {0, 1});
        assertEquals(1, actual.size());
        assertEquals(0, ((IntWritable) structOI.getStructFieldData(actual.get(0), structOI.getStructFieldRef("value"))).get());

        actual.clear();
        udtf.process(new Object[] {32, 100});
        assertEquals(68, actual.size());
        for (int i = 0, j = 32; i < actual.size(); i++, j++) {
            assertEquals(j, ((IntWritable) structOI.getStructFieldData(actual.get(i), structOI.getStructFieldRef("value"))).get());
        }
    }
}
