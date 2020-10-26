package hiveudfs.udtf.zip;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ZipLongestTest {
    private ZipLongest udtf;

    @Before
    public void before() { udtf = new ZipLongest(); }

    @Test
    public void testWithOneIntList() throws HiveException {
        ObjectInspector[] argOIs = new ObjectInspector[]{
            ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableIntObjectInspector)};
        ArrayList<Object[]> actual = new ArrayList<>();
        udtf.setCollector(new Collector() {
            @Override
            public void collect(Object input) throws HiveException {
                Object[] output = (Object[]) input;
                actual.add(output);
            }
        });

        StructObjectInspector structOI = udtf.initialize(argOIs);
        assertEquals("struct<col1:int>", structOI.getTypeName());

        actual.clear();
        udtf.process(new Object[] {
            new ArrayList<>(Arrays.asList(new IntWritable(1), new IntWritable(2), new IntWritable(3)))});
        assertEquals(3, actual.size());
        assertEquals(1, ((IntWritable)structOI.getStructFieldData(actual.get(0), structOI.getStructFieldRef("col1"))).get());
        assertEquals(2, ((IntWritable)structOI.getStructFieldData(actual.get(1), structOI.getStructFieldRef("col1"))).get());
        assertEquals(3, ((IntWritable)structOI.getStructFieldData(actual.get(2), structOI.getStructFieldRef("col1"))).get());
    }

    @Test
    public void testWithTwoIntLists() throws HiveException {
        ObjectInspector[] argOIs = new ObjectInspector[]{
            ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableIntObjectInspector),
            ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableIntObjectInspector)};
        ArrayList<Object[]> actual = new ArrayList<>();
        udtf.setCollector(new Collector() {
            @Override
            public void collect(Object input) throws HiveException {
                Object[] output = (Object[]) input;
                actual.add(output);
            }
        });

        StructObjectInspector structOI = udtf.initialize(argOIs);
        assertEquals("struct<col1:int,col2:int>", structOI.getTypeName());

        actual.clear();
        udtf.process(new Object[] {
            new ArrayList<>(Arrays.asList(new IntWritable(1), new IntWritable(2), new IntWritable(3))),
            new ArrayList<>(Arrays.asList(new IntWritable(4), new IntWritable(5), new IntWritable(6)))});
        assertEquals(3, actual.size());
        assertEquals(1, ((IntWritable)structOI.getStructFieldData(actual.get(0), structOI.getStructFieldRef("col1"))).get());
        assertEquals(2, ((IntWritable)structOI.getStructFieldData(actual.get(1), structOI.getStructFieldRef("col1"))).get());
        assertEquals(3, ((IntWritable)structOI.getStructFieldData(actual.get(2), structOI.getStructFieldRef("col1"))).get());
        assertEquals(4, ((IntWritable)structOI.getStructFieldData(actual.get(0), structOI.getStructFieldRef("col2"))).get());
        assertEquals(5, ((IntWritable)structOI.getStructFieldData(actual.get(1), structOI.getStructFieldRef("col2"))).get());
        assertEquals(6, ((IntWritable)structOI.getStructFieldData(actual.get(2), structOI.getStructFieldRef("col2"))).get());
    }

    @Test
    public void testWithTwoIntListsOfDifferentLength() throws HiveException {
        ObjectInspector[] argOIs = new ObjectInspector[]{
            ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableIntObjectInspector),
            ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableIntObjectInspector)};
        ArrayList<Object[]> actual = new ArrayList<>();
        udtf.setCollector(new Collector() {
            @Override
            public void collect(Object input) throws HiveException {
                Object[] output = (Object[]) input;
                actual.add(output);
            }
        });

        StructObjectInspector structOI = udtf.initialize(argOIs);
        assertEquals("struct<col1:int,col2:int>", structOI.getTypeName());

        actual.clear();
        udtf.process(new Object[] {
            new ArrayList<>(Arrays.asList(new IntWritable(1), new IntWritable(2), new IntWritable(3))),
            new ArrayList<>(Arrays.asList(new IntWritable(4), new IntWritable(5)))});
        assertEquals(3, actual.size());
        assertEquals(1, ((IntWritable)structOI.getStructFieldData(actual.get(0), structOI.getStructFieldRef("col1"))).get());
        assertEquals(2, ((IntWritable)structOI.getStructFieldData(actual.get(1), structOI.getStructFieldRef("col1"))).get());
        assertEquals(3, ((IntWritable)structOI.getStructFieldData(actual.get(2), structOI.getStructFieldRef("col1"))).get());
        assertEquals(4, ((IntWritable)structOI.getStructFieldData(actual.get(0), structOI.getStructFieldRef("col2"))).get());
        assertEquals(5, ((IntWritable)structOI.getStructFieldData(actual.get(1), structOI.getStructFieldRef("col2"))).get());
        assertNull(structOI.getStructFieldData(actual.get(2), structOI.getStructFieldRef("col2")));
    }
}
