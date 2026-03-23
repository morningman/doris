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

package org.apache.doris.datasource.spi;

import org.apache.doris.DorisFE;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Loads catalog plugins from the {@code connectors/} directory under Doris FE home.
 *
 * <p>Each subdirectory under {@code connectors/} represents one catalog plugin.
 * All JAR files in that subdirectory are loaded with an isolated {@link URLClassLoader},
 * providing ClassLoader-level isolation between different catalog plugins to prevent
 * dependency conflicts.</p>
 *
 * <p>This loader must be invoked <b>before</b> {@code Env.loadImage()} to ensure
 * all catalog types are registered before EditLog replay.</p>
 *
 * <h3>Directory Layout</h3>
 * <pre>
 * output/fe/
 * ├── lib/                    # fe-core JARs
 * └── lib/
 *     ├── doris-fe.jar           # fe-core
 *     └── connectors/            # Connector plugins
 *         ├── es/
 *         │   └── doris-connector-es.jar
 *         ├── iceberg/
 *         │   └── doris-connector-iceberg.jar
 *         └── ...
 * </pre>
 */
public class CatalogPluginLoader {
    private static final Logger LOG = LogManager.getLogger(CatalogPluginLoader.class);

    private static final String CONNECTORS_DIR = "lib/connectors";

    /** ClassLoaders for each loaded plugin, keyed by catalog type */
    private static final Map<String, ClassLoader> PLUGIN_CLASSLOADERS = new ConcurrentHashMap<>();

    /**
     * Scan the catalogs/ directory and load all catalog plugins.
     * Each subdirectory is treated as a separate plugin with its own ClassLoader.
     */
    public static void loadPlugins() {
        String dorisHome = DorisFE.DORIS_HOME_DIR;
        if (dorisHome == null || dorisHome.isEmpty()) {
            LOG.warn("DORIS_HOME is not set. Skipping catalog plugin loading.");
            return;
        }

        File pluginDir = new File(dorisHome, CONNECTORS_DIR);
        if (!pluginDir.exists() || !pluginDir.isDirectory()) {
            LOG.info("No catalog plugin directory found at {}. "
                    + "External catalog plugins will not be loaded.", pluginDir.getAbsolutePath());
            return;
        }

        LOG.info("Loading catalog plugins from: {}", pluginDir.getAbsolutePath());

        File[] pluginDirs = pluginDir.listFiles(File::isDirectory);
        if (pluginDirs == null || pluginDirs.length == 0) {
            LOG.info("No catalog plugins found in {}", pluginDir.getAbsolutePath());
            return;
        }

        for (File dir : pluginDirs) {
            try {
                loadPlugin(dir);
            } catch (Exception e) {
                LOG.error("Failed to load catalog plugin from {}: {}", dir.getName(), e.getMessage(), e);
            }
        }

        LOG.info("Catalog plugin loading complete. Registered types: {}",
                CatalogProviderRegistry.getAllProviders().keySet());
    }

    private static void loadPlugin(File pluginDir) throws Exception {
        URL[] jarUrls = findJars(pluginDir);
        if (jarUrls.length == 0) {
            LOG.warn("No JAR files found in plugin directory: {}", pluginDir.getName());
            return;
        }

        LOG.info("Loading catalog plugin from '{}' with {} JAR(s)", pluginDir.getName(), jarUrls.length);

        // Create isolated ClassLoader with fe-core as parent
        URLClassLoader pluginClassLoader = new URLClassLoader(
                jarUrls, CatalogPluginLoader.class.getClassLoader());

        ServiceLoader<CatalogProvider> providers = ServiceLoader.load(
                CatalogProvider.class, pluginClassLoader);

        int count = 0;
        for (CatalogProvider provider : providers) {
            CatalogProviderRegistry.register(provider);
            PLUGIN_CLASSLOADERS.put(provider.getType(), pluginClassLoader);
            count++;
        }

        if (count == 0) {
            LOG.warn("Plugin directory '{}' contains JAR files but no CatalogProvider implementation. "
                    + "Ensure META-INF/services/{} is correctly configured.",
                    pluginDir.getName(), CatalogProvider.class.getName());
            pluginClassLoader.close();
        }
    }

    private static URL[] findJars(File dir) throws Exception {
        FilenameFilter jarFilter = (d, name) -> name.endsWith(".jar");
        File[] jarFiles = dir.listFiles(jarFilter);
        if (jarFiles == null) {
            return new URL[0];
        }

        List<URL> urls = new ArrayList<>(jarFiles.length);
        for (File jar : jarFiles) {
            urls.add(jar.toURI().toURL());
        }
        return urls.toArray(new URL[0]);
    }

    /**
     * Get the ClassLoader for a specific catalog type.
     *
     * @param type catalog type
     * @return the plugin ClassLoader, or null if not loaded via plugin
     */
    public static ClassLoader getPluginClassLoader(String type) {
        return PLUGIN_CLASSLOADERS.get(type);
    }
}
