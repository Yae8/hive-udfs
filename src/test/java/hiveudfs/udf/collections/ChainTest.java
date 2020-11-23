package hiveudfs.udf.collections;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class ChainTest {
    private Chain udf;

    @Before
    public void before() { udf = new Chain(); }

    @Test
    public void testEvaluateWithTwoList() throws Exception {
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(intOI);
        StandardListObjectInspector resultOI = (StandardListObjectInspector) udf.initialize(new ObjectInspector[]{listOI, listOI});

        Object result;
        List<?> list;

        result = udf.evaluate(new DeferredObject[]{
                new DeferredJavaObject(new ArrayList<>(Arrays.asList(1,2,3))),
                new DeferredJavaObject(new ArrayList<>(Arrays.asList(4,5,6)))});
        list = resultOI.getList(result);
        assertEquals(6, list.size());
        assertEquals(1, Collections.frequency(list,1));
        assertEquals(1, Collections.frequency(list,2));
        assertEquals(1, Collections.frequency(list,3));
        assertEquals(1, Collections.frequency(list,4));
        assertEquals(1, Collections.frequency(list,5));
        assertEquals(1, Collections.frequency(list,6));

        result = udf.evaluate(new DeferredObject[]{
                new DeferredJavaObject(new ArrayList<>(Arrays.asList(1,2,3))),
                new DeferredJavaObject(new ArrayList<>(Arrays.asList(4,3,2)))});
        list = resultOI.getList(result);
        assertEquals(6, list.size());
        assertEquals(1, Collections.frequency(list,1));
        assertEquals(2, Collections.frequency(list,2));
        assertEquals(2, Collections.frequency(list,3));
        assertEquals(1, Collections.frequency(list,4));
    }

    @Test
    public void testEvaluateWithThreeList() throws Exception {
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
        StandardListObjectInspector resultOI = (StandardListObjectInspector) udf.initialize(new ObjectInspector[]{listOI, listOI, listOI});

        Object result;
        List<?> list;

        result = udf.evaluate(new DeferredObject[]{
                new DeferredJavaObject(new ArrayList<>(Arrays.asList("a","b","c"))),
                new DeferredJavaObject(new ArrayList<>(Arrays.asList("d","e","f"))),
                new DeferredJavaObject(new ArrayList<>(Arrays.asList("g", "h")))});
        list = resultOI.getList(result);
        assertEquals(8, list.size());
        assertEquals(1, Collections.frequency(list,"a"));
        assertEquals(1, Collections.frequency(list,"b"));
        assertEquals(1, Collections.frequency(list,"c"));
        assertEquals(1, Collections.frequency(list,"d"));
        assertEquals(1, Collections.frequency(list,"e"));
        assertEquals(1, Collections.frequency(list,"f"));
        assertEquals(1, Collections.frequency(list,"g"));
        assertEquals(1, Collections.frequency(list,"h"));

        result = udf.evaluate(new DeferredObject[]{
                new DeferredJavaObject(new ArrayList<>(Arrays.asList("a","b","c"))),
                new DeferredJavaObject(new ArrayList<>(Arrays.asList("b","d","d","f"))),
                new DeferredJavaObject(new ArrayList<>(Arrays.asList("c", "e")))});
        list = resultOI.getList(result);
        assertEquals(9, list.size());
        assertEquals(1, Collections.frequency(list,"a"));
        assertEquals(2, Collections.frequency(list,"b"));
        assertEquals(2, Collections.frequency(list,"c"));
        assertEquals(2, Collections.frequency(list,"d"));
        assertEquals(1, Collections.frequency(list,"e"));
        assertEquals(1, Collections.frequency(list,"f"));
    }

    @Test
    public void testEvaluateWithTwoMap() throws Exception {
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector mapOI = ObjectInspectorFactory.getStandardMapObjectInspector(intOI, intOI);
        StandardMapObjectInspector resultOI = (StandardMapObjectInspector) udf.initialize(new ObjectInspector[]{mapOI, mapOI});

        Object result;
        Map<?, ?> map;

        result = udf.evaluate(new DeferredObject[]{
                new DeferredJavaObject(new HashMap<Integer, Integer>(){{put(1,1); put(2,4); put(3,6);}}),
                new DeferredJavaObject(new HashMap<Integer, Integer>(){{put(4,6); put(5,7); put(6,8);}})});
        map = resultOI.getMap(result);
        assertEquals(6, map.size());
        assertTrue(map.containsKey(1));
        assertTrue(map.containsKey(2));
        assertTrue(map.containsKey(3));
        assertTrue(map.containsKey(4));
        assertTrue(map.containsKey(5));
        assertTrue(map.containsKey(6));
        assertEquals(1, map.get(1));
        assertEquals(4, map.get(2));
        assertEquals(6, map.get(3));
        assertEquals(6, map.get(4));
        assertEquals(7, map.get(5));
        assertEquals(8, map.get(6));

        result = udf.evaluate(new DeferredObject[]{
                new DeferredJavaObject(new HashMap<Integer, Integer>(){{put(1,1); put(2,2); put(3,3);}}),
                new DeferredJavaObject(new HashMap<Integer, Integer>(){{put(2,4); put(3,6); put(4,8);}})});
        map = resultOI.getMap(result);
        assertEquals(4, map.size());
        assertTrue(map.containsKey(1));
        assertTrue(map.containsKey(2));
        assertTrue(map.containsKey(3));
        assertTrue(map.containsKey(4));
        assertEquals(1, map.get(1));
        assertEquals(4, map.get(2));
        assertEquals(6, map.get(3));
        assertEquals(8, map.get(4));
    }

    @Test
    public void testEvaluateWithThreeMap() throws Exception {
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector mapOI = ObjectInspectorFactory.getStandardMapObjectInspector(stringOI, stringOI);
        StandardMapObjectInspector resultOI = (StandardMapObjectInspector) udf.initialize(new ObjectInspector[]{mapOI, mapOI, mapOI});

        Object result;
        Map<?, ?> map;

        result = udf.evaluate(new DeferredObject[]{
                new DeferredJavaObject(new HashMap<String, String>(){{put("a","A"); put("b","B"); put("c","C");}}),
                new DeferredJavaObject(new HashMap<String, String>(){{put("d","D"); put("e","E"); put("f","F");}}),
                new DeferredJavaObject(new HashMap<String, String>(){{put("g","G"); put("h","H");}})});
        map = resultOI.getMap(result);
        assertEquals(8, map.size());
        assertTrue(map.containsKey("a"));
        assertTrue(map.containsKey("b"));
        assertTrue(map.containsKey("c"));
        assertTrue(map.containsKey("d"));
        assertTrue(map.containsKey("e"));
        assertTrue(map.containsKey("f"));
        assertTrue(map.containsKey("g"));
        assertTrue(map.containsKey("h"));
        assertEquals("A", map.get("a"));
        assertEquals("B", map.get("b"));
        assertEquals("C", map.get("c"));
        assertEquals("D", map.get("d"));
        assertEquals("E", map.get("e"));
        assertEquals("F", map.get("f"));
        assertEquals("G", map.get("g"));
        assertEquals("H", map.get("h"));

        result = udf.evaluate(new DeferredObject[]{
                new DeferredJavaObject(new HashMap<String, String>(){{put("a","A"); put("b","B"); put("c","C");}}),
                new DeferredJavaObject(new HashMap<String, String>(){{put("b","BB"); put("d","DD"); put("f","FF");}}),
                new DeferredJavaObject(new HashMap<String, String>(){{put("c","CCC"); put("e","EEE");}})});
        map = resultOI.getMap(result);
        assertEquals(6, map.size());
        assertTrue(map.containsKey("a"));
        assertTrue(map.containsKey("b"));
        assertTrue(map.containsKey("c"));
        assertTrue(map.containsKey("d"));
        assertTrue(map.containsKey("e"));
        assertTrue(map.containsKey("f"));
        assertEquals("A", map.get("a"));
        assertEquals("BB", map.get("b"));
        assertEquals("CCC", map.get("c"));
        assertEquals("DD", map.get("d"));
        assertEquals("EEE", map.get("e"));
        assertEquals("FF", map.get("f"));
    }
}
