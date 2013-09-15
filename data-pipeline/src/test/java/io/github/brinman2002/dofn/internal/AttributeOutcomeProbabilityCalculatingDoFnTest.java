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
package io.github.brinman2002.dofn.internal;

import static io.github.brinman2002.Helper.attribute;
import static io.github.brinman2002.Helper.outcome;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import io.github.brinman2002.data.model.Attribute;
import io.github.brinman2002.data.model.Outcome;
import io.github.brinman2002.dofn.internal.AttributeOutcomeProbabilityCalculatingDoFn;

import java.util.Collection;

import org.apache.crunch.DoFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

public class AttributeOutcomeProbabilityCalculatingDoFnTest {

    @SuppressWarnings("unchecked")
    @Test
    public void process() {

        final DoFn<Pair<Outcome, Pair<Pair<Attribute, Long>, Long>>, Pair<Pair<Outcome, Attribute>, Double>> doFn = new AttributeOutcomeProbabilityCalculatingDoFn();
        final PCollection<Pair<Outcome, Pair<Pair<Attribute, Long>, Long>>> pCollection = MemPipeline.collectionOf(
                testData(outcome("1"), 10, attribute("1"), 5), testData(outcome("1"), 10, attribute("2"), 8),
                testData(outcome("1"), 10, attribute("3"), 1), testData(outcome("2"), 20, attribute("1"), 10));

        final PCollection<Pair<Pair<Outcome, Attribute>, Double>> pCollection2 = pCollection.parallelDo(doFn,
                Avros.pairs(Avros.pairs(Avros.containers(Outcome.class), Avros.containers(Attribute.class)), Avros.doubles()));

        final Collection<Pair<Pair<Outcome, Attribute>, Double>> collection = pCollection2.asCollection().getValue();

        assertEquals(4, collection.size());
        // As with elsewhere, asserting doubles is problematic but we'll do it
        // as long as it works.
        assertTrue(collection.contains(Pair.of(Pair.of(outcome("1"), attribute("1")), 0.5)));
        assertTrue(collection.contains(Pair.of(Pair.of(outcome("1"), attribute("2")), 0.8)));
        assertTrue(collection.contains(Pair.of(Pair.of(outcome("1"), attribute("3")), 0.1)));
        assertTrue(collection.contains(Pair.of(Pair.of(outcome("2"), attribute("1")), 0.5)));
    }

    /**
     * One row represents how many times an outcome occurred, and how many times
     * an attribute occurred for that outcome. It doesn't make sense for the
     * attribute count to be higher than the outcome count but nothing checks;
     * it would effectively give a probability higher than 1.
     * 
     * @param outcome
     * @param outcomeCount
     * @param attribute
     * @param attributeCount
     * @return
     */
    private Pair<Outcome, Pair<Pair<Attribute, Long>, Long>> testData(final Outcome outcome, final long outcomeCount, final Attribute attribute,
            final long attributeCount) {
        return Pair.of(outcome, Pair.of(Pair.of(attribute, attributeCount), outcomeCount));
    }
}
