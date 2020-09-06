package hiveudfs.utils;

import java.util.Iterator;

public class LinSpace implements Iterable<Double>, Iterator<Double> {

    private double current;
    private int count;
    private final double step;
    private final double stop;
    private final int num;
    private final boolean endpoint;

    public LinSpace(double start, double stop, int num) {
        this(start, stop, num, true);
    }

    public LinSpace(double start, double stop, int num, boolean endpoint) {
        if (num < 0) {
            throw new IllegalArgumentException("THe argument 'num' must be non-negative.");
        }
        this.current = start;
        this.count = 0;
        this.step = (stop - start) / (endpoint ? num - 1 : num);
        this.stop = stop;
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
        current += step;
        count++;
        return current;
    }
}