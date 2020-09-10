package hiveudfs.udf.arrayutils;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

public class ArrayMinTest {
    private ArrayMin udf;

    @Before
    public void before() { udf = new ArrayMin(); }

    @Test
    public void testEvaluateWithArrayOfIntType() throws Exception {
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(intOI);
        IntObjectInspector resultOI = (IntObjectInspector) udf.initialize(new ObjectInspector[]{listOI});

        Object result;

        result = udf.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(new ArrayList<>(Arrays.asList(1,2,3,4,5)))});
        assertEquals(1, resultOI.get(result));
        result = udf.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(new ArrayList<>(Arrays.asList(5,4,3,2,1)))});
        assertEquals(1, resultOI.get(result));
        result = udf.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(new ArrayList<>(Arrays.asList(3,1,5,1,5)))});
        assertEquals(1, resultOI.get(result));
    }

    @Test
    public void testEvaluateWithArrayOfStringType() throws Exception {
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
        StringObjectInspector resultOI = (StringObjectInspector) udf.initialize(new ObjectInspector[]{listOI});

        Object result;

        result = udf.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(new ArrayList<>(Arrays.asList("a","b","c","d","e")))});
        assertEquals("a", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(new ArrayList<>(Arrays.asList("e","d","c","b","a")))});
        assertEquals("a", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(new ArrayList<>(Arrays.asList("c","a","e","a","e")))});
        assertEquals("a", resultOI.getPrimitiveJavaObject(result));
    }
}
