package hiveudfs.udf.set;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class SetDifference extends SetBase {

    public Object evaluateList(DeferredObject[] arguments) throws HiveException {
        ArrayList<Object> result = new ArrayList<>();
        ArrayList<Object> negList = new ArrayList<>();

        List<?> list1 = list1OI.getList(arguments[0].get());
        List<?> list2 = list2OI.getList(arguments[1].get());

        for (Object element : list2) {
            Object object = primitive2OI.getPrimitiveJavaObject(element);
            if (object == null) {
                continue;
            }
            if (!negList.contains(object)) {
                negList.add(object);
            }
        }
        for (Object element : list1) {
            Object object = primitive1OI.getPrimitiveJavaObject(element);
            if (object == null) {
                continue;
            }
            if (!result.contains(object) && !negList.contains(object)) {
                result.add(object);
            }
        }
        return result;
    }

    public Object evaluateMap(DeferredObject[] arguments) throws HiveException {
        HashMap<Object, Object> result = new HashMap<>();
        ArrayList<Object> negList = new ArrayList<>();

        Map<?, ?> map1 = map1OI.getMap(arguments[0].get());
        Map<?, ?> map2 = map2OI.getMap(arguments[1].get());

        for (Object key : map2.keySet()) {
            Object keyObject = primitive2OI.getPrimitiveJavaObject(key);
            if (keyObject == null) {
                continue;
            }
            Object valueObject = ObjectInspectorUtils.copyToStandardObject(map2.get(key), map2OI.getMapValueObjectInspector());
            if (valueObject == null) {
                continue;
            }
            if (!negList.contains(keyObject)) {
                negList.add(keyObject);
            }
        }
        for (Object key : map1.keySet()) {
            Object keyObject = primitive1OI.getPrimitiveJavaObject(key);
            if (keyObject == null) {
                continue;
            }
            Object valueObject = ObjectInspectorUtils.copyToStandardObject(map1.get(key), map1OI.getMapValueObjectInspector());
            if (valueObject == null) {
                continue;
            }
            if (!result.containsKey(keyObject) && !negList.contains(keyObject)) {
                result.put(keyObject, valueObject);
            }
        }
        return result;
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
