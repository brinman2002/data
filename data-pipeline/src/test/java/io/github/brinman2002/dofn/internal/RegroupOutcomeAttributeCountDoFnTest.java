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

import java.util.Collection;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

public class RegroupOutcomeAttributeCountDoFnTest {

    @SuppressWarnings("unchecked")
    @Test
    public void process() {

        // We could also just build the PTable directly with counts, but doing
        // count() adds an additional sanity check.
        final PCollection<Pair<Outcome, Attribute>> inputPCollection = MemPipeline.typedCollectionOf(
                Avros.pairs(Avros.containers(Outcome.class), Avros.containers(Attribute.class)), testData("1", "1"), testData("2", "2"),
                testData("3", "1"), testData("1", "1"), testData("1", "1"), testData("1", "1"), testData("1", "1"));
        final PTable<Pair<Outcome, Attribute>, Long> count = inputPCollection.count();

        final PCollection<Pair<Outcome, Pair<Attribute, Long>>> pCollection2 = count.parallelDo(new RegroupOutcomeAttributeCountDoFn(),
                Avros.pairs(Avros.containers(Outcome.class), Avros.pairs(Avros.containers(Attribute.class), Avros.longs())));
        final Collection<Pair<Outcome, Pair<Attribute, Long>>> collection = pCollection2.asCollection().getValue();

        assertEquals(3, collection.size());
        assertTrue(collection.contains(Pair.of(outcome("1"), Pair.of(attribute("1"), 5L))));
        assertTrue(collection.contains(Pair.of(outcome("2"), Pair.of(attribute("2"), 1L))));
        assertTrue(collection.contains(Pair.of(outcome("3"), Pair.of(attribute("1"), 1L))));
    }

    private Pair<Outcome, Attribute> testData(final String i, final String j) {
        return Pair.of(outcome(i), attribute(j));
    }

}
