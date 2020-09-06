package hiveudfs.utils;

import java.util.Iterator;

public class LogSpace implements Iterable<Double>, Iterator<Double> {

    private final double base;
    private final LinSpace linspace;

    public LogSpace(double start, double stop, int num) {
        this(start, stop, num, 2, true);
    }

    public LogSpace(double start, double stop, int num, double base) {
        this(start, stop, num, base, true);
    }

    public LogSpace(double start, double stop, int num, boolean endpoint) {
        this(start, stop, num, 2, endpoint);
    }

    public LogSpace(double start, double stop, int num, double base, boolean endpoint) {
        if (num < 0) {
            throw new IllegalArgumentException("THe argument 'num' must be non-negative.");
        }
        this.linspace = new LinSpace(start, stop, num, endpoint);
        this.base = base;
    }

    @Override
    public Iterator<Double> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return linspace.hasNext();
    }

    @Override
    public Double next() {
        double y = linspace.next();
        return Math.pow(base, y);
    }
}