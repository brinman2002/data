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

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

/**
 * Block together objects based on the key. This DoFn is intended for consumers
 * to preprocess data before passing into a function that joins data based on
 * keys; typically this will be when the key represents milliseconds from Epoch
 * but the data should be grouped by a (much) larger interval.
 * 
 * <p>
 * The current implementation of this function supports a single day as the
 * block.
 * 
 * @author brandon
 * 
 * @param <T>
 */
public class BlockingDoFn<T> extends DoFn<Pair<Long, T>, Pair<Long, T>> {

    /**
     * 
     */
    private static final long serialVersionUID = 7364278592499656252L;

    // Millis in one day. As we're not sending people to Mars, we aren't
    // accounting for leap seconds or any other things that complicate reality.
    private static final long ONE_DAY = 1000 * 60 * 60 * 24;

    @Override
    public void process(final Pair<Long, T> input, final Emitter<Pair<Long, T>> emitter) {
        final long dateTimeBlocked = (input.first().longValue() / ONE_DAY) * ONE_DAY;
        emitter.emit(Pair.of(dateTimeBlocked, input.second()));
    }

}
