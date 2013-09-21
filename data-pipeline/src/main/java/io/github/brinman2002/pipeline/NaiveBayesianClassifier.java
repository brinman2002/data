/**
 * Copyright 2013 Brandon Inman
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.brinman2002.pipeline;

import io.github.brinman2002.data.model.Attribute;
import io.github.brinman2002.data.model.Outcome;
import io.github.brinman2002.dofn.internal.AttributeOutcomeProbabilityCalculatingDoFn;
import io.github.brinman2002.dofn.internal.ProbabilityCalculatingDoFn;
import io.github.brinman2002.dofn.internal.RegroupOutcomeAttributeCountDoFn;
import io.github.brinman2002.filter.AttributeFilterFn;

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.Distinct;
import org.apache.crunch.lib.Join;
import org.apache.crunch.lib.PTables;
import org.apache.crunch.types.avro.Avros;

/**
 * Utility class providing methods to perform Naive Bayesian Classification. The
 * implementations of the class make the assumption that the number of possible
 * outcomes will be relatively small.
 * 
 * @author brandon
 * 
 */
public class NaiveBayesianClassifier {

    /**
     * Build training data from a PTable of Attribute instances and a PTable of
     * Outcome instances. The key of the table is used to join a given set of
     * attributes with the outcome that was observed. One convenient key is
     * milliseconds since Epoch, rounded to the nearest interval to group by.
     * However, no specific scheme for the key is enforced.
     * <p>
     * Callers may wish to limit one outcome per key, as multiple outcomes per
     * key will have identical attribute sets.
     * 
     * <p>
     * 
     * Calling this method will, at least partially, invoke the pipeline to
     * begin processing.
     * 
     * @param attributes
     *            PTable of attributes.
     * @param outcomes
     *            PTable of outcomes.
     * @return Training data.
     */
    public static Pair<PTable<Attribute, Pair<Outcome, Double>>, PTable<Outcome, Double>> train(final PTable<Long, Attribute> attributes,
            final PTable<Long, Outcome> outcomes) {

        final PTable<Outcome, Long> outcomeCounts = PTables.values(outcomes).count();

        // Join the outcomes and attributes on their key. Attributes are forced
        // to be distinct so that multiple occurrences of the same attribute
        // does not adversely weight the attribute.
        final PTable<Outcome, Attribute> attributesAssociatedToOutcome = PTables.asPTable(PTables.values(Join.innerJoin(outcomes,
                Distinct.distinct(attributes))));

        // Count the number of Outcome/Attribute combinations and regroup by
        // the outcome. We are counting the number of times an attribute is
        // associated with an outcome so that we can calculate the probability
        // (by dividing this by the total count of the outcome).
        final PTable<Pair<Outcome, Attribute>, Long> countedEventAttributes = attributesAssociatedToOutcome.count();
        final PTable<Outcome, Pair<Attribute, Long>> attributeCountsByEvent = countedEventAttributes.parallelDo(
                new RegroupOutcomeAttributeCountDoFn(),
                Avros.tableOf(Avros.containers(Outcome.class), Avros.pairs(Avros.containers(Attribute.class), Avros.longs())));

        // This join is used to group together the count of outcomes with the
        // count of attributes.
        final PTable<Outcome, Pair<Pair<Attribute, Long>, Long>> joinedAttributeAndEventCounts = Join
                .innerJoin(attributeCountsByEvent, outcomeCounts);

        final PTable<Attribute, Pair<Outcome, Double>> outcomeAttributeProbabilities = joinedAttributeAndEventCounts.parallelDo(
                new AttributeOutcomeProbabilityCalculatingDoFn(),
                Avros.tableOf(Avros.containers(Attribute.class), Avros.pairs(Avros.containers(Outcome.class), Avros.doubles())));

        // This will trigger the pipeline so far, at least the parts related to
        // that PTable.
        final long totalOutcomeCount = outcomes.length().getValue();
        final PTable<Outcome, Long> countedOutcomes = PTables.values(outcomes).count();

        final PTable<Outcome, Double> outcomeProbabilities = countedOutcomes.parallelDo(new ProbabilityCalculatingDoFn<Outcome>(totalOutcomeCount),
                Avros.tableOf(Avros.containers(Outcome.class), Avros.doubles()));

        final Pair<PTable<Attribute, Pair<Outcome, Double>>, PTable<Outcome, Double>> output = Pair.of(outcomeAttributeProbabilities,
                outcomeProbabilities);
        return output;
    }

    /**
     * Convenience method for {@link #predict(PTable, PTable)}.
     * 
     * @see {@link #predict(PTable, PTable)}
     * @param trainingResults
     * @return Predicted classification.
     */
    public static Map<Outcome, Double> predict(final Pair<PTable<Attribute, Pair<Outcome, Double>>, PTable<Outcome, Double>> trainingResults,
            final Collection<Attribute> observedAttributes) {
        return predict(trainingResults.first(), trainingResults.second(), observedAttributes);
    }

    /**
     * Perform prediction based on the training results and a set of attributes.
     * This method uses a FilterFn to scope down the data, but otherwise makes
     * the assumption that the final data is reasonably sized to be able to do
     * the prediction in-memory.
     * 
     * @param attributeOutcomeProbabilities
     *            Probability of an attribute occurring given an outcome.
     * @param outcomeProbabilities
     *            Probability of attribute occurring, independent of outcome.
     * @return Predicted classification.
     */
    public static Map<Outcome, Double> predict(final PTable<Attribute, Pair<Outcome, Double>> attributeOutcomeProbabilities,
            final PTable<Outcome, Double> outcomeProbabilities, final Collection<Attribute> observedAttributes) {
        // FIXME this is a pretty roughed out implementation to get us started;
        // will optimize if performance warrants it.
        final PTable<Attribute, Pair<Outcome, Double>> filteredAOP = attributeOutcomeProbabilities.filter(AttributeFilterFn.by(observedAttributes));
        final Map<Attribute, Pair<Outcome, Double>> attributeMap = filteredAOP.materializeToMap();
        final Map<Outcome, Double> outcomeMap = outcomeProbabilities.materializeToMap();

        final TreeMap<Outcome, Double> probabilityTable = new TreeMap<Outcome, Double>();
        for (final Pair<Outcome, Double> pair : attributeMap.values()) {
            final Double current = probabilityTable.get(pair.first());
            if (current == null) {
                probabilityTable.put(pair.first(), pair.second());
            } else {
                probabilityTable.put(pair.first(), pair.second() * current);
            }
        }

        for (final Map.Entry<Outcome, Double> entry : outcomeMap.entrySet()) {
            final Outcome key = entry.getKey();
            if (probabilityTable.containsKey(key)) {
                final double newValue = probabilityTable.get(key) * entry.getValue();
                probabilityTable.put(key, newValue);
            }

        }
        return probabilityTable;
    }

    public static List<String> toCsv(final Map<Outcome, Double> data, final boolean header) {
        final List<String> out = new LinkedList<String>();
        if (header) {
            out.add("OUTCOME_NAMESPACE,OUTCOME,OUTCOME_QUALIFIER,PROBABILITY");
        }

        // Sort in reverse order of the probability

        final TreeSet<Entry<Outcome, Double>> entrySet = new TreeSet<Entry<Outcome, Double>>(new Comparator<Entry<Outcome, Double>>() {
            @Override
            public int compare(Entry<Outcome, Double> o1, Entry<Outcome, Double> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        entrySet.addAll(data.entrySet());

        for (final Entry<Outcome, Double> entry : entrySet) {
            final Outcome outcome = entry.getKey();
            out.add(String.format("%s,%s,%s,%s", outcome.getNamespace(), outcome.getValue(), outcome.getQualifier(), entry.getValue()));
        }
        return out;
    }
}
