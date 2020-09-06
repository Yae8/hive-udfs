package hiveudfs.udaf.statistics;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Mode extends AbstractGenericUDAFResolver {

    static final Logger LOG = LoggerFactory.getLogger(Mode.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }
        switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case STRING:
            case DECIMAL:
            case DATE:
            case TIMESTAMP:
                return new ModeEvaluator();
            default:
                throw new UDFArgumentTypeException(0,
                        "Only numeric or date type arguments are accepted" +
                                ", but " + parameters[0].getCategory() + " is passed.");
        }
    }


    public static class ModeEvaluator extends Frequency.FrequencyEvaluator {

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                structOI = (StructObjectInspector) parameters[0];
                mapperField = structOI.getStructFieldRef("mapper");
                mapperFieldOI = (StandardMapObjectInspector) mapperField.getFieldObjectInspector();
            }

            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                ArrayList<ObjectInspector> fieldOI = new ArrayList<>();
                fieldOI.add(ObjectInspectorFactory.getStandardMapObjectInspector(inputOI, PrimitiveObjectInspectorFactory.writableIntObjectInspector));

                ArrayList<String> fieldName = new ArrayList<>();
                fieldName.add("mapper");

                partialResult = new Object[1];
                partialResult[0] = new HashMap<Object, Integer>();

                return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldOI);
            } else if (m == Mode.COMPLETE) {
                return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(inputOI.getPrimitiveCategory());
            } else {
                return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(((PrimitiveObjectInspector) mapperFieldOI.getMapKeyObjectInspector()).getPrimitiveCategory());
            }
        }


        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            FrequencyAgg myAgg = (FrequencyAgg) agg;
            Map.Entry<Object, Integer> maxEntry = Collections.max(myAgg.mapper.entrySet(), new Comparator<Map.Entry<Object, Integer>>() {
                @Override
                public int compare(Map.Entry<Object, Integer> entry1, Map.Entry<Object, Integer> entry2) {
                    return entry1.getValue().compareTo(entry2.getValue());
                }
            });
            return maxEntry.getKey();
        }
    }
}
