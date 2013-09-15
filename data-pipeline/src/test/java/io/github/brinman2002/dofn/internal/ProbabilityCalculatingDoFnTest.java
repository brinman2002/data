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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.github.brinman2002.dofn.internal.ProbabilityCalculatingDoFn;

import java.util.Collection;

import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;
import org.junit.Test;

public class ProbabilityCalculatingDoFnTest {

    @Test
    public void process() {

        final PTable<String, Long> pTable = MemPipeline.tableOf("One", 100L, "Two", 200L, "Three", 250L);
        final PTable<String, Double> pCollection = pTable.parallelDo(new ProbabilityCalculatingDoFn<String>(100L),
                Writables.tableOf(Writables.strings(), Writables.doubles()));

        final Collection<Pair<String, Double>> collection = pCollection.asCollection().getValue();

        assertEquals(3, collection.size());
        // The floating point math makes this a little sketchy but we'll deal
        // with that when the tests fail.
        assertTrue(collection.contains(Pair.of("One", 1.0)));
        assertTrue(collection.contains(Pair.of("Two", 2.0)));
        assertTrue(collection.contains(Pair.of("Three", 2.5)));
    }
}
