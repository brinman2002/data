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

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;

/**
 * Calculate the probability of an event, given a constant number of totalEvents
 * and a PTable of objects and their counts (as output by {@link PCollection}
 * count())
 * 
 * @author brandon
 * 
 * @param <T>
 */
public class ProbabilityCalculatingDoFn<T> extends DoFn<Pair<T, Long>, Pair<T, Double>> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final double totalEvents;

    public ProbabilityCalculatingDoFn(final long totalEvents) {
        // Implicit conversion from long to double
        this.totalEvents = totalEvents;
    }

    @Override
    public void process(final Pair<T, Long> input, final Emitter<Pair<T, Double>> emitter) {
        emitter.emit(Pair.of(input.first(), input.second() / totalEvents));
    }
}
