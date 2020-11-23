package hiveudfs.udf.collections;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class MergeMax extends GenericUDF {

    protected transient MapObjectInspector[] mapOIs;
    protected transient Converter[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 1) {
            throw new UDFArgumentException("The function " + getUdfName() + " takes arguments at least one.");
        }

        mapOIs = new MapObjectInspector[arguments.length];
        converters = new Converter[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            ObjectInspector inputOI = arguments[i];
            if (inputOI.getCategory() != Category.MAP) {
                throw new UDFArgumentException("The function " + getUdfName() + " takes maps or lists.");
            }
            mapOIs[i] = (MapObjectInspector) inputOI;
            if (!ObjectInspectorUtils.compareTypes(mapOIs[0], mapOIs[i])) {
                throw new UDFArgumentException("The function " + getUdfName() + " must be all maps of the same type");
            }
            converters[i] = ObjectInspectorConverters.getConverter(
                mapOIs[i].getMapValueObjectInspector(), PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        }
        return ObjectInspectorFactory.getStandardMapObjectInspector(
            mapOIs[0].getMapKeyObjectInspector(), PrimitiveObjectInspectorFactory.writableIntObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        HashMap<Object, Integer> result = new HashMap<>();

        for (int i = 0; i < arguments.length; i++) {
            Map<?, ?> map = mapOIs[i].getMap(arguments[i].get());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = entry.getKey();
                int value = ((IntWritable) converters[i].convert(entry.getValue())).get();
                if (result.containsKey(key)) {
                    result.put(key, Math.max(result.get(key), value));
                } else {
                    result.put(key, value);
                }
            }
        }
        return result.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new IntWritable(e.getValue())));
    }

    @Override
    protected String getFuncName() {
        return getClass().getSimpleName().toLowerCase();
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 1);
        return getStandardDisplayString(getFuncName(), children);
    }
}
