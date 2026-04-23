// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.connector.api.action;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ActionArgumentTest {

    @Test
    public void requiredFieldsAccessible() {
        ActionArgument a = new ActionArgument("count", ActionArgumentType.LONG, true);
        Assertions.assertEquals("count", a.name());
        Assertions.assertEquals(ActionArgumentType.LONG, a.type());
        Assertions.assertTrue(a.required());
        Assertions.assertTrue(a.defaultValue().isEmpty());
    }

    @Test
    public void defaultValueOptional() {
        ActionArgument a = new ActionArgument("mode", ActionArgumentType.STRING, false, "full");
        Assertions.assertTrue(a.defaultValue().isPresent());
        Assertions.assertEquals("full", a.defaultValue().get());
    }

    @Test
    public void blankNameRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ActionArgument(" ", ActionArgumentType.STRING, true));
    }

    @Test
    public void nullNameRejected() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new ActionArgument(null, ActionArgumentType.STRING, true));
    }

    @Test
    public void nullTypeRejected() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new ActionArgument("x", null, true));
    }

    @Test
    public void equalityAndHash() {
        ActionArgument a = new ActionArgument("x", ActionArgumentType.LONG, true, "1");
        ActionArgument b = new ActionArgument("x", ActionArgumentType.LONG, true, "1");
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
    }
}
