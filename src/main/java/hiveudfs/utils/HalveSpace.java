package hiveudfs.utils;

import java.util.Iterator;

public class HalveSpace implements Iterable<Double>, Iterator<Double> {

    private double current;
    private int count;
    private final double stop;
    private double diff;
    private final int num;
    private final boolean endpoint;

    public HalveSpace(double start, double stop, int num) {
        this(start, stop, num, true);
    }

    public HalveSpace(double start, double stop, int num, boolean endpoint) {
        this.current = start;
        this.count = 0;
        this.stop = stop;
        this.diff = stop - start;
        this.num = num;
        this.endpoint = endpoint;
    }

    @Override
    public Iterator<Double> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return count < num;
    }

    @Override
    public Double next() {
        if (count == 0) {
            count++;
            return current;
        }
        if (endpoint & count == num - 1) {
            count++;
            return stop;
        }
        diff /= 2;
        current += diff;
        count++;
        return current;
    }
}
