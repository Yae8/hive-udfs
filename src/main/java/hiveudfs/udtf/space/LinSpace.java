package hiveudfs.udtf.space;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.List;

public class LinSpace extends SpaceBase {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        return super.initialize(argOIs);
    }

    @Override
    List<Double> getSamples(double start, double stop, int num) {
        return Lists.newArrayList(
                new hiveudfs.utils.LinSpace(start, stop, num, false).iterator());
    }
}