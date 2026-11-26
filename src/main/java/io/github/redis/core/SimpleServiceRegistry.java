package io.github.redis.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 简单服务注册表实现
 * 使用ConcurrentHashMap存储服务实例，支持并发访问
 */
public class SimpleServiceRegistry implements ServiceRegistry {

    private final Map<Class<?>, Object> services = new ConcurrentHashMap<>();

    @Override
    public <T> void register(Class<T> type, T instance) {
        if (type == null) {
            throw new IllegalArgumentException("Service type cannot be null");
        }
        if (instance == null) {
            throw new IllegalArgumentException("Service instance cannot be null");
        }
        if (!type.isInstance(instance)) {
            throw new IllegalArgumentException("Instance is not an instance of " + type.getName());
        }
        services.put(type, instance);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void register(T instance) {
        if (instance == null) {
            throw new IllegalArgumentException("Service instance cannot be null");
        }
        Class<T> type = (Class<T>) instance.getClass();
        register(type, instance);
    }

    @Override
    public <T> T get(Class<T> type) {
        if (type == null) {
            return null;
        }
        Object instance = services.get(type);
        if (instance == null) {
            return null;
        }
        return type.cast(instance);
    }

    @Override
    public <T> T require(Class<T> type) {
        T instance = get(type);
        if (instance == null) {
            throw new IllegalStateException("Service not found: " + type.getName());
        }
        return instance;
    }

    @Override
    public boolean has(Class<?> type) {
        return type != null && services.containsKey(type);
    }

    @Override
    public void remove(Class<?> type) {
        if (type != null) {
            services.remove(type);
        }
    }

    @Override
    public void clear() {
        services.clear();
    }
}





