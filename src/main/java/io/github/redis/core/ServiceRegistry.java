package io.github.redis.core;

/**
 * 服务注册表接口
 * 用于统一管理服务依赖，支持依赖注入
 */
public interface ServiceRegistry {

    /**
     * 注册服务实例
     *
     * @param type     服务类型
     * @param instance 服务实例
     * @param <T>      服务类型泛型
     */
    <T> void register(Class<T> type, T instance);

    /**
     * 注册服务实例（使用实现类作为key）
     *
     * @param instance 服务实例
     * @param <T>      服务类型泛型
     */
    <T> void register(T instance);

    /**
     * 获取服务实例
     *
     * @param type 服务类型
     * @param <T>  服务类型泛型
     * @return 服务实例，如果不存在返回null
     */
    <T> T get(Class<T> type);

    /**
     * 获取服务实例，如果不存在则抛出异常
     *
     * @param type 服务类型
     * @param <T>  服务类型泛型
     * @return 服务实例
     * @throws IllegalStateException 如果服务不存在
     */
    <T> T require(Class<T> type);

    /**
     * 检查服务是否已注册
     *
     * @param type 服务类型
     * @return 如果已注册返回true
     */
    boolean has(Class<?> type);

    /**
     * 移除服务
     *
     * @param type 服务类型
     */
    void remove(Class<?> type);

    /**
     * 清空所有服务
     */
    void clear();
}








