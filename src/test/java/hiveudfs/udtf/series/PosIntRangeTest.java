package hiveudfs.udtf.series;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class PosIntRangeTest {
    private PosIntRange udtf;

    @Before
    public void before() {
        udtf = new PosIntRange();
    }

    @Test
    public void testWithOneArguments() throws HiveException {
        ObjectInspector[] argOIs = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaIntObjectInspector};
        ArrayList<Object> indices = new ArrayList<>();
        ArrayList<Object> values = new ArrayList<>();
        udtf.setCollector(new Collector() {
            @Override
            public void collect(Object input) throws HiveException {
                Object[] output = (Object[]) input;
                indices.add(output[0]);
                values.add(output[1]);
            }
        });
        StructObjectInspector structOI = udtf.initialize(argOIs);
        assertEquals("struct<index:int,value:int>", structOI.getTypeName());

        indices.clear();
        values.clear();
        udtf.process(new Object[] {1});
        assertEquals(1, indices.size(), values.size());
        assertEquals(0, ((IntWritable) structOI.getStructFieldData(indices.get(0), structOI.getStructFieldRef("index"))).get());
        assertEquals(0, ((IntWritable) structOI.getStructFieldData(values.get(0), structOI.getStructFieldRef("value"))).get());

        indices.clear();
        values.clear();
        udtf.process(new Object[] {100});
        assertEquals(100, indices.size(), values.size());
        for (int i = 0; i < indices.size(); i++) {
            assertEquals(i, ((IntWritable) structOI.getStructFieldData(indices.get(i), structOI.getStructFieldRef("index"))).get());
            assertEquals(i, ((IntWritable) structOI.getStructFieldData(values.get(i), structOI.getStructFieldRef("value"))).get());
        }
    }

    @Test
    public void testWithTwoArguments() throws HiveException {
        ObjectInspector[] argOIs = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector};
        ArrayList<Object> indices = new ArrayList<>();
        ArrayList<Object> values = new ArrayList<>();
        udtf.setCollector(new Collector() {
            @Override
            public void collect(Object input) throws HiveException {
                Object[] output = (Object[]) input;
                indices.add(output[0]);
                values.add(output[1]);
            }
        });
        StructObjectInspector structOI = udtf.initialize(argOIs);
        assertEquals("struct<index:int,value:int>", structOI.getTypeName());

        indices.clear();
        values.clear();
        udtf.process(new Object[] {0, 1});
        assertEquals(1, indices.size(), values.size());
        assertEquals(0, ((IntWritable) structOI.getStructFieldData(indices.get(0), structOI.getStructFieldRef("index"))).get());
        assertEquals(0, ((IntWritable) structOI.getStructFieldData(values.get(0), structOI.getStructFieldRef("value"))).get());

        indices.clear();
        values.clear();
        udtf.process(new Object[] {32, 100});
        assertEquals(68, indices.size(), values.size());
        for (int i = 0, j = 32; i < values.size(); i++, j++) {
            assertEquals(i, ((IntWritable) structOI.getStructFieldData(indices.get(i), structOI.getStructFieldRef("index"))).get());
            assertEquals(j, ((IntWritable) structOI.getStructFieldData(values.get(i), structOI.getStructFieldRef("value"))).get());
        }
    }
}
