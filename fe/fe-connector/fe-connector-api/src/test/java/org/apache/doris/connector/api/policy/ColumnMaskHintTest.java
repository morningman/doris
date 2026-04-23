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

package org.apache.doris.connector.api.policy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class ColumnMaskHintTest {

    @Test
    public void nullifyFactory() {
        ColumnMaskHint h = ColumnMaskHint.nullify();
        Assertions.assertEquals(ColumnMaskHint.MaskKind.NULLIFY, h.maskKind());
        Assertions.assertTrue(h.maskExpr().isEmpty());
    }

    @Test
    public void redactFactory() {
        ColumnMaskHint h = ColumnMaskHint.redact();
        Assertions.assertEquals(ColumnMaskHint.MaskKind.REDACT, h.maskKind());
        Assertions.assertTrue(h.maskExpr().isEmpty());
    }

    @Test
    public void expressionFactoryRequiresExpr() {
        ColumnMaskHint h = ColumnMaskHint.expression("substr(c, 1, 4) || '****'");
        Assertions.assertEquals(ColumnMaskHint.MaskKind.EXPRESSION, h.maskKind());
        Assertions.assertEquals("substr(c, 1, 4) || '****'", h.maskExpr().orElse(""));
    }

    @Test
    public void expressionWithoutExprRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ColumnMaskHint(ColumnMaskHint.MaskKind.EXPRESSION, Optional.empty()));
    }

    @Test
    public void rejectsNullKind() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new ColumnMaskHint(null, Optional.empty()));
    }

    @Test
    public void rejectsNullExpr() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new ColumnMaskHint(ColumnMaskHint.MaskKind.NULLIFY, null));
    }
}
