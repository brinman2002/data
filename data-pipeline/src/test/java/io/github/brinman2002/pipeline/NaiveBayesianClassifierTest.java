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

import static io.github.brinman2002.Helper.attribute;
import static io.github.brinman2002.Helper.outcome;
import io.github.brinman2002.data.model.Attribute;
import io.github.brinman2002.data.model.Outcome;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

public class NaiveBayesianClassifierTest {

    private static final PTableType<Long, Attribute> TABLE_OF_ATTRIBUTES = Avros.tableOf(Avros.longs(), Avros.containers(Attribute.class));
    private static final PTableType<Long, Outcome> TABLE_OF_OUTCOMES = Avros.tableOf(Avros.longs(), Avros.containers(Outcome.class));
    private static final Long DAY_ONE = 1000L * 60 * 60 * 24;
    private static final Long DAY_TWO = DAY_ONE * 2;
    private static final Long DAY_THREE = DAY_ONE * 3;
    private static final Long DAY_FOUR = DAY_ONE * 4;

    @Test
    public void train() {
        final PTable<Long, Attribute> attributes = MemPipeline.typedTableOf(TABLE_OF_ATTRIBUTES, attributes());
        final PTable<Long, Outcome> outcomes = MemPipeline.typedTableOf(TABLE_OF_OUTCOMES, outcomes());

        final Pair<PTable<Pair<Outcome, Attribute>, Double>, PTable<Attribute, Double>> trained = NaiveBayesianClassifier.train(attributes, outcomes);
        System.out.println(trained.first());
        System.out.println(trained.second());
    }

    private Collection<Pair<Long, Outcome>> outcomes() {
        final List<Pair<Long, Outcome>> outcomes = new ArrayList<Pair<Long, Outcome>>();

        outcomes.add(Pair.of(DAY_ONE, outcome("a")));
        outcomes.add(Pair.of(DAY_TWO, outcome("b")));
        outcomes.add(Pair.of(DAY_THREE, outcome("b")));
        outcomes.add(Pair.of(DAY_FOUR, outcome("a")));

        return outcomes;
    }

    private Collection<Pair<Long, Attribute>> attributes() {
        final List<Pair<Long, Attribute>> attributes = new ArrayList<Pair<Long, Attribute>>();
        // Only one outcome per block is currently permitted.
        attributes.add(Pair.of(DAY_ONE, attribute("a")));
        attributes.add(Pair.of(DAY_ONE, attribute("b")));
        attributes.add(Pair.of(DAY_ONE, attribute("c")));

        attributes.add(Pair.of(DAY_TWO, attribute("b")));

        attributes.add(Pair.of(DAY_THREE, attribute("c")));

        attributes.add(Pair.of(DAY_FOUR, attribute("d")));

        return attributes;
    }
}
