package hiveudfs.udf.setmetric;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SetSubsetTest {
    private SetSubset udf;

    @Before
    public void before() {
        udf = new SetSubset();
    }

    @Test
    public void testEvaluateWithIntType() throws Exception {
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(intOI);
        BooleanObjectInspector resultOI = (BooleanObjectInspector) udf.initialize(new ObjectInspector[]{listOI, listOI});

        Object result;

        result = udf.evaluate(new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(new ArrayList<>(Arrays.asList(1, 2, 3))),
                new GenericUDF.DeferredJavaObject(new ArrayList<>(Arrays.asList(1, 2, 3)))});
        assertTrue(resultOI.get(result));
        result = udf.evaluate(new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(new ArrayList<>(Arrays.asList(1, 2, 3))),
                new GenericUDF.DeferredJavaObject(new ArrayList<>(Arrays.asList(1, 3)))});
        assertFalse(resultOI.get(result));
        result = udf.evaluate(new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(new ArrayList<>(Arrays.asList(1, 3))),
                new GenericUDF.DeferredJavaObject(new ArrayList<>(Arrays.asList(1, 2, 3)))});
        assertTrue(resultOI.get(result));
    }

    @Test
    public void testEvaluateWithStringType() throws Exception {
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
        BooleanObjectInspector resultOI = (BooleanObjectInspector) udf.initialize(new ObjectInspector[]{listOI, listOI});

        Object result;

        result = udf.evaluate(new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(new ArrayList<>(Arrays.asList("a", "b", "c"))),
                new GenericUDF.DeferredJavaObject(new ArrayList<>(Arrays.asList("a", "b", "c")))});
        assertTrue(resultOI.get(result));
        result = udf.evaluate(new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(new ArrayList<>(Arrays.asList("a", "b", "c"))),
                new GenericUDF.DeferredJavaObject(new ArrayList<>(Arrays.asList("a", "c")))});
        assertFalse(resultOI.get(result));
        result = udf.evaluate(new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(new ArrayList<>(Arrays.asList("a", "c"))),
                new GenericUDF.DeferredJavaObject(new ArrayList<>(Arrays.asList("a", "b", "c")))});
        assertTrue(resultOI.get(result));
    }}
