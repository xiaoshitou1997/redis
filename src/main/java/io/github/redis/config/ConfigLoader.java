package io.github.redis.config;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 配置文件加载器
 * 支持加载redis.conf格式的配置文件
 */
public class ConfigLoader {

    private static final Pattern COMMENT_PATTERN = Pattern.compile("^\\s*#");
    private static final Pattern CONFIG_PATTERN = Pattern.compile("^\\s*([\\w-]+)\\s+(.+)$");
    private static final Pattern QUOTED_STRING_PATTERN = Pattern.compile("^\"(.+)\"$");

    /**
     * 从文件加载配置
     *
     * @param configPath 配置文件路径
     * @return Redis配置对象
     * @throws IOException 如果文件读取失败
     */
    public static RedisConfig loadFromFile(String configPath) throws IOException {
        Path path = Paths.get(configPath);
        if (!Files.exists(path)) {
            throw new FileNotFoundException("Config file not found: " + configPath);
        }
        return loadFromFile(path);
    }

    /**
     * 从文件加载配置
     *
     * @param configPath 配置文件路径
     * @return Redis配置对象
     * @throws IOException 如果文件读取失败
     */
    public static RedisConfig loadFromFile(Path configPath) throws IOException {
        RedisConfig config = new RedisConfig();
        List<String> lines = Files.readAllLines(configPath);

        for (String line : lines) {
            parseLine(line, config);
        }

        return config;
    }

    /**
     * 从输入流加载配置
     *
     * @param inputStream 输入流
     * @return Redis配置对象
     * @throws IOException 如果读取失败
     */
    public static RedisConfig loadFromStream(InputStream inputStream) throws IOException {
        RedisConfig config = new RedisConfig();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                parseLine(line, config);
            }
        }
        return config;
    }

    /**
     * 创建默认配置
     *
     * @return 默认配置对象
     */
    public static RedisConfig createDefault() {
        RedisConfig config = new RedisConfig();
        // 使用默认值（已在RedisConfig中设置）
        return config;
    }

    /**
     * 解析配置行
     */
    private static void parseLine(String line, RedisConfig config) {
        // 跳过空行和注释
        if (line == null || line.trim().isEmpty() || COMMENT_PATTERN.matcher(line).matches()) {
            return;
        }

        Matcher matcher = CONFIG_PATTERN.matcher(line);
        if (!matcher.matches()) {
            return;
        }

        String key = matcher.group(1).toLowerCase();
        String value = matcher.group(2).trim();
        value = unquote(value);

        try {
            parseConfigKey(key, value, config);
        } catch (Exception e) {
            // 忽略无法解析的配置项
            System.err.println("Warning: Failed to parse config line: " + line + " - " + e.getMessage());
        }
    }

    /**
     * 解析配置键值对
     */
    private static void parseConfigKey(String key, String value, RedisConfig config) {
        switch (key) {
            // 网络配置
            case "port":
                config.setPort(Integer.parseInt(value));
                break;
            case "bind":
                config.setBind(value);
                break;
            case "protected-mode":
                config.setProtectedMode(parseBoolean(value));
                break;
            case "timeout":
                config.setTimeout(Integer.parseInt(value));
                break;
            case "tcp-keepalive":
                config.setTcpKeepalive(Integer.parseInt(value));
                break;
            case "tcp-nodelay":
                config.setTcpNodelay(parseBoolean(value));
                break;

            // 通用配置
            case "databases":
                config.setDatabases(Integer.parseInt(value));
                break;
            case "loglevel":
                config.setLogLevel(value);
                break;
            case "logfile":
                config.setLogFile(value);
                break;

            // 内存配置
            case "maxmemory":
                config.setMaxMemory(value);
                break;
            case "maxmemory-policy":
                config.setMaxMemoryPolicy(value);
                break;

            // 持久化配置
            case "save":
                parseSaveRule(value, config);
                break;
            case "dbfilename":
                config.setDbfilename(value);
                break;
            case "dir":
                config.setDir(value);
                break;
            case "appendonly":
                config.setAppendOnly(parseBoolean(value));
                break;
            case "appendfilename":
                config.setAppendFilename(value);
                break;
            case "appendfsync":
                config.setAppendFsync(value);
                break;

            // 安全配置
            case "requirepass":
                config.setRequirePass(value);
                break;

            // TLS 配置
            case "tls-enabled":
            case "tlsenabled":
                config.setTlsEnabled(parseBoolean(value));
                break;
            case "tls-port":
                config.setTlsPort(Integer.parseInt(value));
                break;
            case "tls-cert-file":
                config.setTlsCertFile(value);
                break;
            case "tls-key-file":
                config.setTlsKeyFile(value);
                break;
            case "tls-ca-cert-file":
                config.setTlsCaCertFile(value);
                break;
            case "tls-auth-clients":
                config.setTlsAuthClients(parseBoolean(value));
                break;

            // 客户端配置
            case "maxclients":
                config.setMaxClients(Integer.parseInt(value));
                break;

            // 集群配置
            case "cluster-enabled":
                config.setClusterEnabled(parseBoolean(value));
                break;
            case "cluster-config-file":
                config.setClusterConfigFile(value);
                break;
            case "replication-port":
                config.setReplicationPort(Integer.parseInt(value));
                break;
            case "cluster-node-timeout":
                config.setClusterNodeTimeout(Integer.parseInt(value));
                break;

            // 复制配置
            case "replicaof":
                config.setReplicaOf(value);
                break;
            case "masterauth":
                config.setMasterAuth(value);
                break;
            case "replica-read-only":
                config.setReplicaReadOnly(parseBoolean(value));
                break;

            default:
                // 忽略未知配置项
                break;
        }
    }

    /**
     * 解析save规则
     * 格式: save 900 1
     */
    private static void parseSaveRule(String value, RedisConfig config) {
        String[] parts = value.trim().split("\\s+");
        if (parts.length >= 2) {
            int seconds = Integer.parseInt(parts[0]);
            int changes = Integer.parseInt(parts[1]);
            config.addSaveRule(seconds, changes);
        }
    }

    /**
     * 解析布尔值
     */
    private static boolean parseBoolean(String value) {
        return "yes".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value) || "1".equals(value);
    }

    /**
     * 去除引号
     */
    private static String unquote(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        Matcher matcher = QUOTED_STRING_PATTERN.matcher(value);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return value;
    }
}

