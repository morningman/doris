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

package org.apache.doris.datasource.mtmv;

import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccSnapshot;

import java.util.Objects;

/**
 * Adapter that wraps an SPI-side {@link ConnectorMvccSnapshot} into a
 * fe-core {@link MvccSnapshot}, so plugin-driven tables can flow MVCC pins
 * through fe-core APIs that are typed against {@link MvccSnapshot}.
 *
 * <p>{@link PluginDrivenMtmvBridge} unwraps instances of this class when
 * forwarding {@code Optional<MvccSnapshot>} parameters to {@code MtmvOps}.
 * Any non-{@code PluginDrivenMvccSnapshot} value is rejected with
 * {@link UnsupportedOperationException}.</p>
 */
public final class PluginDrivenMvccSnapshot implements MvccSnapshot {

    private final ConnectorMvccSnapshot delegate;

    public PluginDrivenMvccSnapshot(ConnectorMvccSnapshot delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    public ConnectorMvccSnapshot delegate() {
        return delegate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PluginDrivenMvccSnapshot)) {
            return false;
        }
        return delegate.equals(((PluginDrivenMvccSnapshot) o).delegate);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public String toString() {
        return "PluginDrivenMvccSnapshot{" + delegate + '}';
    }
}
