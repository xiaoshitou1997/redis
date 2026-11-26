package io.github.redis.storage;

/**
 * 存储工厂接口
 * 用于创建存储实例，支持不同的存储实现
 */
public interface StorageFactory {

    /**
     * 创建存储实例
     *
     * @return 存储实例
     */
    Storage createStorage();
}








