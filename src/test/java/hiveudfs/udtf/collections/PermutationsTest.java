package hiveudfs.udtf.collections;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PermutationsTest {
    private Permutations udtf;

    @Before
    public void before() {
        udtf = new Permutations();
    }

    @Test
    public void testWithOneArguments() throws HiveException {
        ObjectInspector[] argOIs = new ObjectInspector[]{
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector)};
        ArrayList<Object> actual = new ArrayList<>();
        List<Integer> list;

        udtf.setCollector(new Collector() {
            @Override
            public void collect(Object input) throws HiveException {
                Object[] output = (Object[]) input;
                actual.add(output[0]);
            }
        });
        StructObjectInspector structOI = udtf.initialize(argOIs);
        StandardListObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(structOI);

        assertEquals("struct<array:array<int>>", structOI.getTypeName());

        actual.clear();
        udtf.process(new Object[] {new ArrayList<>(Arrays.asList(1, 2, 3))});
        assertEquals(6, actual.size());

        list = (List<Integer>) listOI.getList(actual.get(0));
        assertEquals(3, list.size());
        assertTrue(list.contains(1));
        assertTrue(list.contains(2));
        assertTrue(list.contains(3));

        list = (List<Integer>) listOI.getList(actual.get(1));
        assertEquals(3, list.size());
        assertTrue(list.contains(1));
        assertTrue(list.contains(3));
        assertTrue(list.contains(2));

        list = (List<Integer>) listOI.getList(actual.get(2));
        assertEquals(3, list.size());
        assertTrue(list.contains(2));
        assertTrue(list.contains(1));
        assertTrue(list.contains(3));

        list = (List<Integer>) listOI.getList(actual.get(3));
        assertEquals(3, list.size());
        assertTrue(list.contains(2));
        assertTrue(list.contains(3));
        assertTrue(list.contains(1));

        list = (List<Integer>) listOI.getList(actual.get(4));
        assertEquals(3, list.size());
        assertTrue(list.contains(3));
        assertTrue(list.contains(1));
        assertTrue(list.contains(2));

        list = (List<Integer>) listOI.getList(actual.get(5));
        assertEquals(3, list.size());
        assertTrue(list.contains(3));
        assertTrue(list.contains(2));
        assertTrue(list.contains(1));
    }

    @Test
    public void testWithTwoArguments() throws HiveException {
        ObjectInspector[] argOIs = new ObjectInspector[]{
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector),
                PrimitiveObjectInspectorFactory.javaIntObjectInspector};

        ArrayList<Object> actual = new ArrayList<>();
        List<Integer> list;

        udtf.setCollector(new Collector() {
            @Override
            public void collect(Object input) throws HiveException {
                Object[] output = (Object[]) input;
                actual.add(output[0]);
            }
        });
        StructObjectInspector structOI = udtf.initialize(argOIs);
        StandardListObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(structOI);

        assertEquals("struct<array:array<int>>", structOI.getTypeName());

        actual.clear();
        udtf.process(new Object[] {new ArrayList<>(Arrays.asList(1, 2, 3)), 2});
        assertEquals(6, actual.size());

        list = (List<Integer>) listOI.getList(actual.get(0));
        assertEquals(2, list.size());
        assertTrue(list.contains(1));
        assertTrue(list.contains(2));

        list = (List<Integer>) listOI.getList(actual.get(1));
        assertEquals(2, list.size());
        assertTrue(list.contains(1));
        assertTrue(list.contains(3));

        list = (List<Integer>) listOI.getList(actual.get(2));
        assertEquals(2, list.size());
        assertTrue(list.contains(2));
        assertTrue(list.contains(1));

        list = (List<Integer>) listOI.getList(actual.get(3));
        assertEquals(2, list.size());
        assertTrue(list.contains(2));
        assertTrue(list.contains(3));

        list = (List<Integer>) listOI.getList(actual.get(4));
        assertEquals(2, list.size());
        assertTrue(list.contains(3));
        assertTrue(list.contains(1));

        list = (List<Integer>) listOI.getList(actual.get(5));
        assertEquals(2, list.size());
        assertTrue(list.contains(3));
        assertTrue(list.contains(2));
    }
}
