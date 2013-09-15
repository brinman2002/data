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
package io.github.brinman2002;

import io.github.brinman2002.data.model.Attribute;
import io.github.brinman2002.data.model.AttributeNamespace;
import io.github.brinman2002.data.model.Outcome;

/**
 * Helper functions for unit tests.
 * 
 * @author brandon
 * 
 */
public class Helper {

    /**
     * Create an arbitrary attribute based on the parameter.
     * 
     * @param i
     * @return attribute
     */
    public static Attribute attribute(final String i) {
        return Attribute.newBuilder().setNamespace(AttributeNamespace.KEYWORD).setValue("VALUE" + i).build();
    }

    /**
     * Create an arbitrary outcome instance based on the parameter.
     * 
     * @param i
     * @return outcome
     */
    public static Outcome outcome(final String i) {
        return Outcome.newBuilder().setValue("VALUE_" + i).build();
    }
}
