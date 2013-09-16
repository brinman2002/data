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

import io.github.brinman2002.data.model.Attribute;
import io.github.brinman2002.data.model.Outcome;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

/**
 * This thing needs to be taken out and shot.
 * 
 * <p>
 * 
 * It builds a "map" of pairings of outcomes and an attribute (i.e. an
 * attribute), and the probability associated with it. This is one of the major
 * outputs of the naive bayesian classifier; the other being the overall
 * probabilities of an attribute occurring.
 * 
 * <p>
 * 
 * @author brandon
 * 
 */
public class AttributeOutcomeProbabilityCalculatingDoFn extends
        DoFn<Pair<Outcome, Pair<Pair<Attribute, Long>, Long>>, Pair<Attribute, Pair<Outcome, Double>>> {

    /**
     * 
     */
    private static final long serialVersionUID = -469857876381729475L;

    @Override
    public void process(final Pair<Outcome, Pair<Pair<Attribute, Long>, Long>> input, final Emitter<Pair<Attribute, Pair<Outcome, Double>>> emitter) {
        final Outcome outcome = input.first();
        final Attribute attribute = input.second().first().first();
        final long outcomeCount = input.second().second();
        final long attributeCount = input.second().first().second();

        emitter.emit(Pair.of(attribute, Pair.of(outcome, (attributeCount * 1.0) / outcomeCount)));
    }
}