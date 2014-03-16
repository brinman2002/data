package io.github.brinman2002.dofn.learning;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.types.writable.WritableType;
import org.apache.crunch.types.writable.Writables;

public class Cost {

    public final static WritableType<Collection<Double>, ?> INPUT_TYPE = Writables.collections(Writables.doubles());

    /**
     * Compute the squared mean cost of the data set relative to the values of
     * theta.
     * 
     * @param dataSet
     *            PCollection of collections of doubles, where the first entry
     *            in the collection is the 'y' or expected value, and the
     *            remaining entries are the features X.
     * @param thetas
     *            List of doubles representing the thetas, starting with the
     *            intercept term theta0.
     * 
     * @param lambda
     *            Regularization term weighting. May be null, which will be
     *            treated as zero.
     * @return Value of the cost function.
     */
    public static double of(final PCollection<Collection<Double>> dataSet, final List<Double> thetas, final Double lambda) {
        final PCollection<Double> costs = dataSet.parallelDo(new LinearRegressionCostDoFn(thetas), Writables.doubles());

        // Sum outputs.
        final double cost = costs.by(new MapFn<Double, Integer>() {
            private static final long serialVersionUID = 4499559488463200632L;

            @Override
            public Integer map(final Double input) {
                return 1;
            }
        }, Writables.ints()).groupByKey().combineValues(Aggregators.SUM_DOUBLES()).values().materialize().iterator().next();

        double regularizationTerm = 0.0;

        if (lambda != null && lambda > 0.0) {
            for (final double theta : thetas) {
                regularizationTerm += theta * theta;
            }
            regularizationTerm *= lambda;
        }

        final long m = Aggregate.length(dataSet).getValue();
        return (cost + regularizationTerm) / (2.0 * m);
    }

    public static class LinearRegressionCostDoFn extends DoFn<Collection<Double>, Double> {
        /**
         * 
         */
        private static final long serialVersionUID = 5868051388626537350L;
        private final List<Double> thetas;

        public LinearRegressionCostDoFn(final List<Double> thetas) {
            this.thetas = thetas;
        }

        @Override
        public void process(final Collection<Double> input, final Emitter<Double> emitter) {
            // Cost is the value of h(x) minus the expected
            final double cost = mr_computeCost(input, thetas);
            // Squared
            emitter.emit(cost * cost);
        }
    }

    public static Map<Integer, Double> gradient(final PCollection<Collection<Double>> dataSet, final List<Double> thetas, final Double lambda) {

        final PTable<Integer, Double> gradients = dataSet.parallelDo(new LinearRegressionGradientDoFn(thetas),
                Writables.tableOf(Writables.ints(), Writables.doubles()));
        final Map<Integer, Double> map = gradients.groupByKey().combineValues(Aggregators.SUM_DOUBLES()).asMap().getValue();
        final double m = Aggregate.length(dataSet).getValue();
        final Map<Integer, Double> out = new HashMap<Integer, Double>(map.size());
        for (final Map.Entry<Integer, Double> entry : map.entrySet()) {
            final int index = entry.getKey();
            double gradient = entry.getValue() / m;
            if (lambda != null && lambda > 0) {
                gradient -= (lambda * thetas.get(index)) / m;
            }
            out.put(index, gradient);
        }
        return out;
    }

    public static class LinearRegressionGradientDoFn extends DoFn<Collection<Double>, Pair<Integer, Double>> {

        /**
         * 
         */
        private static final long serialVersionUID = 6060577946461174201L;
        private final List<Double> thetas;

        public LinearRegressionGradientDoFn(final List<Double> thetas) {
            this.thetas = thetas;
        }

        @Override
        public void process(final Collection<Double> input, final Emitter<Pair<Integer, Double>> emitter) {
            final double cost = mr_computeCost(input, thetas);
            final Iterator<Double> x = input.iterator();

            // Throw out the y value.
            x.next();

            // This represents the 1.0 bias term
            emitter.emit(Pair.of(0, cost));
            int i = 1;
            while (x.hasNext()) {
                emitter.emit(Pair.of(i, cost * x.next()));
                ++i;
            }
        }
    }

    static double mr_computeCost(final Collection<Double> inputCollection, final List<Double> thetaCollection) {
        final Iterator<Double> inputs = inputCollection.iterator();
        final Iterator<Double> thetas = thetaCollection.iterator();
        final double y = inputs.next();

        // Evaluate features times their respective thetas. initialize with
        // the intercept term, as this represents the 1.0 bias term that we are
        // avoiding requiring.
        double h = thetas.next();
        while (inputs.hasNext() && thetas.hasNext()) {
            h += inputs.next() * thetas.next();
        }
        if (inputs.hasNext() || thetas.hasNext()) {
            throw new IllegalArgumentException("Input size: " + inputCollection.size() + "  Theta size: " + thetaCollection.size());
        }
        // Cost is the value of h(x) minus the expected
        final double cost = h - y;
        return cost;
    }

    public static List<Double> gradientDescent(final List<Double> initialThetas, final PCollection<Collection<Double>> inputs, double alpha,
            final double lambda) {
        final List<Double> thetas = new ArrayList<Double>(initialThetas);
        double oldCost = Double.MAX_VALUE;
        int iter = 0;
        while (oldCost > 0.01) {
            final Map<Integer, Double> gradients = Cost.gradient(inputs, thetas, lambda);
            for (int j = 0; j < thetas.size(); ++j) {
                thetas.set(j, thetas.get(j) - (alpha * gradients.get(j)));
            }
            final double newCost = Cost.of(inputs, thetas, lambda);
            if (newCost > oldCost) {
                alpha *= 0.9;
            }
            oldCost = newCost;
            System.out.println(++iter + " " + newCost + "  " + gradients + "  " + thetas);
        }
        return thetas;
    }
}
