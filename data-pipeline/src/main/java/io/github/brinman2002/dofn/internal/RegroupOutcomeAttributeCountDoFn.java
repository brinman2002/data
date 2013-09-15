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
 * Regroup the count of outcome/attribute pairs into a pair of outcome and
 * attribute/counts.
 * 
 * @author brandon
 * 
 */
public class RegroupOutcomeAttributeCountDoFn extends DoFn<Pair<Pair<Outcome, Attribute>, Long>, Pair<Outcome, Pair<Attribute, Long>>> {

    /**
     * 
     */
    private static final long serialVersionUID = 5322550060946429293L;

    @Override
    public void process(final Pair<Pair<Outcome, Attribute>, Long> input, final Emitter<Pair<Outcome, Pair<Attribute, Long>>> emitter) {
        final Pair<Outcome, Attribute> pair = input.first();
        final Outcome outcome = pair.first();
        final Attribute attribute = pair.second();
        final Long count = input.second();
        emitter.emit(Pair.of(outcome, Pair.of(attribute, count)));
    }
}