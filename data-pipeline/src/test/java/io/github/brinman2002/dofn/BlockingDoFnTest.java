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
package io.github.brinman2002.dofn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;
import org.junit.Test;

public class BlockingDoFnTest {
    private static final long ONE_DAY = 1000L * 60 * 60 * 24;

    @Test
    public void process() {
        final PTable<Long, String> table = MemPipeline.tableOf(ONE_DAY, "One", ONE_DAY * 2, "Two", ONE_DAY + 120, "Three", ONE_DAY * 3 + 500, "Four");

        final PCollection<Pair<Long, String>> pCollection = table.parallelDo(new BlockingDoFn<String>(),
                Writables.pairs(Writables.longs(), Writables.strings()));

        final Collection<Pair<Long, String>> collection = pCollection.asCollection().getValue();
        assertEquals(4, collection.size());
        assertTrue(collection.contains(Pair.of(ONE_DAY, "One")));
        assertTrue(collection.contains(Pair.of(ONE_DAY * 2, "Two")));
        assertTrue(collection.contains(Pair.of(ONE_DAY, "Three")));
        assertTrue(collection.contains(Pair.of(ONE_DAY * 3, "Four")));

    }
}
