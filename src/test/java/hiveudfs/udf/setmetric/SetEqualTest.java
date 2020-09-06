package hiveudfs.udf.setmetric;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

public class SetEqualTest {
    private SetEqual udf;

    @Before
    public void before() {
        udf = new SetEqual();
    }

    @Test
    public void testEvaluateWithIntType() throws Exception {
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(intOI);
        BooleanObjectInspector resultOI = (BooleanObjectInspector) udf.initialize(new ObjectInspector[]{listOI, listOI});

        Object result;

        result = udf.evaluate(new DeferredObject[]{
                new DeferredJavaObject(new ArrayList<>(Arrays.asList(1, 2, 3))),
                new DeferredJavaObject(new ArrayList<>(Arrays.asList(1, 2, 3)))});
        assertTrue(resultOI.get(result));
        result = udf.evaluate(new DeferredObject[]{
                new DeferredJavaObject(new ArrayList<>(Arrays.asList(1, 2, 3))),
                new DeferredJavaObject(new ArrayList<>(Arrays.asList(1, 3)))});
        assertFalse(resultOI.get(result));
        result = udf.evaluate(new DeferredObject[]{
                new DeferredJavaObject(new ArrayList<>(Arrays.asList(1, 3))),
                new DeferredJavaObject(new ArrayList<>(Arrays.asList(1, 2, 3)))});
        assertFalse(resultOI.get(result));
    }

    @Test
    public void testEvaluateWithStringType() throws Exception {
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
        BooleanObjectInspector resultOI = (BooleanObjectInspector) udf.initialize(new ObjectInspector[]{listOI, listOI});

        Object result;

        result = udf.evaluate(new DeferredObject[]{
                new DeferredJavaObject(new ArrayList<>(Arrays.asList("a", "b", "c"))),
                new DeferredJavaObject(new ArrayList<>(Arrays.asList("a", "b", "c")))});
        assertTrue(resultOI.get(result));
        result = udf.evaluate(new DeferredObject[]{
                new DeferredJavaObject(new ArrayList<>(Arrays.asList("a", "b", "c"))),
                new DeferredJavaObject(new ArrayList<>(Arrays.asList("a", "c")))});
        assertFalse(resultOI.get(result));
        result = udf.evaluate(new DeferredObject[]{
                new DeferredJavaObject(new ArrayList<>(Arrays.asList("a", "c"))),
                new DeferredJavaObject(new ArrayList<>(Arrays.asList("a", "b", "c")))});
        assertFalse(resultOI.get(result));
    }
}
