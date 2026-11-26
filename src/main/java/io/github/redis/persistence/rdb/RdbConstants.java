package io.github.redis.persistence.rdb;

/**
 * RDB文件格式常量
 */
public class RdbConstants {

    /**
     * RDB文件魔数
     */
    public static final String RDB_MAGIC = "REDIS";

    /**
     * RDB版本号（简化版使用0001）
     */
    public static final String RDB_VERSION = "0001";

    /**
     * 数据库选择操作码
     */
    public static final int OPCODE_SELECTDB = 0xFE;

    /**
     * 数据库结束操作码
     */
    public static final int OPCODE_EOF = 0xFF;

    /**
     * 字符串类型
     */
    public static final int TYPE_STRING = 0;

    /**
     * 列表类型
     */
    public static final int TYPE_LIST = 1;

    /**
     * 集合类型
     */
    public static final int TYPE_SET = 2;

    /**
     * 哈希类型
     */
    public static final int TYPE_HASH = 3;

    /**
     * 有序集合类型
     */
    public static final int TYPE_ZSET = 4;

    /**
     * Stream 类型 (Phase 4)
     */
    public static final int TYPE_STREAM = 5;

    /**
     * 过期时间（秒）
     */
    public static final int EXPIRETIME_SEC = 0xFD;

    /**
     * 过期时间（毫秒）
     */
    public static final int EXPIRETIME_MS = 0xFC;

    /**
     * 字符串长度编码类型：6位长度
     */
    public static final int ENCODING_6BIT = 0;

    /**
     * 字符串长度编码类型：14位长度
     */
    public static final int ENCODING_14BIT = 1;

    /**
     * 字符串长度编码类型：32位长度
     */
    public static final int ENCODING_32BIT = 2;

    private RdbConstants() {
        // 工具类，不允许实例化
    }
}
