package io.github.brinman2002.filter;

import io.github.brinman2002.data.model.Attribute;
import io.github.brinman2002.data.model.Outcome;

import java.util.Collection;

import org.apache.crunch.FilterFn;
import org.apache.crunch.Pair;

/**
 * FilterFn for a set of attributes.
 * 
 * @author brandon
 * 
 */
public class AttributeFilterFn extends FilterFn<Pair<Attribute, Pair<Outcome, Double>>> {

    /**
     * 
     */
    private static final long serialVersionUID = 4058861046999819222L;

    public static AttributeFilterFn by(final Collection<Attribute> attributes) {
        return new AttributeFilterFn(attributes);
    }

    private final Collection<Attribute> attributes;

    public AttributeFilterFn(final Collection<Attribute> attributes) {
        this.attributes = attributes;
    }

    @Override
    public boolean accept(final Pair<Attribute, Pair<Outcome, Double>> input) {
        return attributes.contains(input.first());
    }
}
