package io.github.redis.module;

import io.github.redis.command.CommandHandler;
import io.github.redis.command.CommandTable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Process-wide module registry.
 *
 * <p>Phase-8 minimal: modules are registered in code (no dynamic loading from
 * .so/.jar files yet). Each module may expose:
 * <ul>
 *   <li>commands — added to the {@link CommandTable} on load</li>
 *   <li>info sections — surfaced via MODULE LIST</li>
 * </ul>
 *
 * <p>The MODULE LOAD/UNLOAD commands from redis-cli just record an intent;
 * real loading of dynamically-compiled code is Phase 9 territory.
 */
public final class ModuleManager {

    /** A loaded module. */
    public static final class LoadedModule {
        public final String name;
        public final int apiVersion;
        public final List<String> commands = new ArrayList<>();
        public LoadedModule(String name, int apiVersion) {
            this.name = name; this.apiVersion = apiVersion;
        }
    }

    private final Map<String, LoadedModule> modules = new ConcurrentHashMap<>();

    public void register(LoadedModule m) { modules.put(m.name, m); }
    public LoadedModule unregister(String name) { return modules.remove(name); }
    public LoadedModule get(String name) { return modules.get(name); }
    public List<LoadedModule> all() { return new ArrayList<>(modules.values()); }
    public boolean isEmpty() { return modules.isEmpty(); }
}
