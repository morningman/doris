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

package org.apache.doris.foundation.property;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface ConnectorProperty {
    String[] names() default {};

    /**
     * Binds every raw property whose key starts with this prefix into a {@code Map<String, String>}
     * field (key suffix -> value), for account-scoped dynamic keys such as
     * {@code adls.sas-token.<account-host>} that cannot be enumerated in {@link #names()}.
     * Mutually exclusive with {@link #names()}; {@link #validator()} and {@link #required()} are
     * not applied to prefix fields (validation belongs to the owning class' validate()).
     */
    String prefix() default "";

    String description() default "";

    boolean required() default true;
    boolean supported() default true;

    boolean sensitive() default false;

    boolean isRegionField() default false;

    Class<? extends ConnectorPropertyValidator> validator() default ConnectorPropertyValidator.None.class;
}
