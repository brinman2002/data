package io.github.brinman2002.dofn.learning;

public class Sigmoid {

    public static double calculate(final double input) {
        return 1.0 / (1.0 + Math.exp(-input));
    }

}
