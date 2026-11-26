package io.github.redis.config;

import java.util.ArrayList;
import java.util.List;

/**
 * Redis配置类
 * 封装所有Redis服务器配置项
 */
public class RedisConfig {

    // ========== 网络配置 ==========
    private int port = 6379;
    private String bind = "127.0.0.1";
    private boolean protectedMode = true;
    private int timeout = 0;
    private int tcpKeepalive = 300;
    private boolean tcpNodelay = true;

    // ========== 通用配置 ==========
    private int databases = 16;
    private String logLevel = "notice";
    private String logFile = "";

    // ========== 内存配置 ==========
    private String maxMemory = "2gb";
    private String maxMemoryPolicy = "allkeys-lru";

    // ========== 持久化配置 ==========
    private List<SaveRule> saveRules = new ArrayList<>();
    private String dbfilename = "dump.rdb";
    private String dir = "./";
    private boolean appendOnly = false;
    private String appendFilename = "appendonly.aof";
    private String appendFsync = "everysec";

    // ========== 安全配置 ==========
    private String requirePass = null;

    // ========== TLS 配置 ==========
    private boolean tlsEnabled = false;
    private int tlsPort = 0; // 0 = disabled; otherwise separate TLS port
    private String tlsCertFile = null;
    private String tlsKeyFile = null;
    private String tlsCaCertFile = null;
    private boolean tlsAuthClients = false;

    // ========== 客户端配置 ==========
    private int maxClients = 10000;

    // ========== 集群配置 ==========
    private boolean clusterEnabled = false;
    private String clusterConfigFile = "nodes.conf";
    private int clusterNodeTimeout = 15000;

    // ========== 复制配置 ==========
    private String replicaOf = null;
    private String masterAuth = null;
    private boolean replicaReadOnly = true;
    private int replicationPort = 0; // 0 = disabled; otherwise port the master listens on for replicas

    /**
     * RDB保存规则
     */
    public static class SaveRule {
        private final int seconds;
        private final int changes;

        public SaveRule(int seconds, int changes) {
            this.seconds = seconds;
            this.changes = changes;
        }

        public int getSeconds() {
            return seconds;
        }

        public int getChanges() {
            return changes;
        }

        @Override
        public String toString() {
            return "save " + seconds + " " + changes;
        }
    }

    // ========== Getter和Setter方法 ==========

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getBind() {
        return bind;
    }

    public void setBind(String bind) {
        this.bind = bind;
    }

    public boolean isProtectedMode() {
        return protectedMode;
    }

    public void setProtectedMode(boolean protectedMode) {
        this.protectedMode = protectedMode;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getTcpKeepalive() {
        return tcpKeepalive;
    }

    public void setTcpKeepalive(int tcpKeepalive) {
        this.tcpKeepalive = tcpKeepalive;
    }

    public boolean isTcpNodelay() {
        return tcpNodelay;
    }

    public void setTcpNodelay(boolean tcpNodelay) {
        this.tcpNodelay = tcpNodelay;
    }

    public int getDatabases() {
        return databases;
    }

    public void setDatabases(int databases) {
        this.databases = databases;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public String getLogFile() {
        return logFile;
    }

    public void setLogFile(String logFile) {
        this.logFile = logFile;
    }

    public String getMaxMemory() {
        return maxMemory;
    }

    public void setMaxMemory(String maxMemory) {
        this.maxMemory = maxMemory;
    }

    public String getMaxMemoryPolicy() {
        return maxMemoryPolicy;
    }

    public void setMaxMemoryPolicy(String maxMemoryPolicy) {
        this.maxMemoryPolicy = maxMemoryPolicy;
    }

    public List<SaveRule> getSaveRules() {
        return new ArrayList<>(saveRules);
    }

    public void setSaveRules(List<SaveRule> saveRules) {
        this.saveRules = new ArrayList<>(saveRules);
    }

    public void addSaveRule(int seconds, int changes) {
        this.saveRules.add(new SaveRule(seconds, changes));
    }

    public String getDbfilename() {
        return dbfilename;
    }

    public void setDbfilename(String dbfilename) {
        this.dbfilename = dbfilename;
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public boolean isAppendOnly() {
        return appendOnly;
    }

    public void setAppendOnly(boolean appendOnly) {
        this.appendOnly = appendOnly;
    }

    public String getAppendFilename() {
        return appendFilename;
    }

    public void setAppendFilename(String appendFilename) {
        this.appendFilename = appendFilename;
    }

    public String getAppendFsync() {
        return appendFsync;
    }

    public void setAppendFsync(String appendFsync) {
        this.appendFsync = appendFsync;
    }

    public String getRequirePass() {
        return requirePass;
    }

    public void setRequirePass(String requirePass) {
        this.requirePass = requirePass;
    }

    public boolean isTlsEnabled() { return tlsEnabled; }
    public void setTlsEnabled(boolean v) { this.tlsEnabled = v; }
    public int getTlsPort() { return tlsPort; }
    public void setTlsPort(int p) { this.tlsPort = p; }
    public String getTlsCertFile() { return tlsCertFile; }
    public void setTlsCertFile(String s) { this.tlsCertFile = s; }
    public String getTlsKeyFile() { return tlsKeyFile; }
    public void setTlsKeyFile(String s) { this.tlsKeyFile = s; }
    public String getTlsCaCertFile() { return tlsCaCertFile; }
    public void setTlsCaCertFile(String s) { this.tlsCaCertFile = s; }
    public boolean isTlsAuthClients() { return tlsAuthClients; }
    public void setTlsAuthClients(boolean v) { this.tlsAuthClients = v; }

    public int getMaxClients() {
        return maxClients;
    }

    public void setMaxClients(int maxClients) {
        this.maxClients = maxClients;
    }

    public boolean isClusterEnabled() {
        return clusterEnabled;
    }

    public void setClusterEnabled(boolean clusterEnabled) {
        this.clusterEnabled = clusterEnabled;
    }

    public String getClusterConfigFile() {
        return clusterConfigFile;
    }

    public void setClusterConfigFile(String clusterConfigFile) {
        this.clusterConfigFile = clusterConfigFile;
    }

    public int getClusterNodeTimeout() {
        return clusterNodeTimeout;
    }

    public void setClusterNodeTimeout(int clusterNodeTimeout) {
        this.clusterNodeTimeout = clusterNodeTimeout;
    }

    public String getReplicaOf() {
        return replicaOf;
    }

    public void setReplicaOf(String replicaOf) {
        this.replicaOf = replicaOf;
    }

    public String getMasterAuth() {
        return masterAuth;
    }

    public void setMasterAuth(String masterAuth) {
        this.masterAuth = masterAuth;
    }

    public boolean isReplicaReadOnly() {
        return replicaReadOnly;
    }

    public void setReplicaReadOnly(boolean replicaReadOnly) {
        this.replicaReadOnly = replicaReadOnly;
    }

    public int getReplicationPort() { return replicationPort; }
    public void setReplicationPort(int p) { this.replicationPort = p; }

    /**
     * Parse a Redis memory size string (e.g. "2gb", "512mb", "1024") into bytes.
     * Returns 0 if the value is null, empty, or unparseable.
     */
    public static long parseMemoryToBytes(String s) {
        if (s == null) return 0;
        s = s.trim().toLowerCase();
        if (s.isEmpty()) return 0;
        long multiplier = 1;
        // Strip a known unit suffix if present.
        if (s.endsWith("gb"))      { multiplier = 1024L * 1024L * 1024L; s = s.substring(0, s.length() - 2); }
        else if (s.endsWith("mb")) { multiplier = 1024L * 1024L;          s = s.substring(0, s.length() - 2); }
        else if (s.endsWith("kb")) { multiplier = 1024L;                   s = s.substring(0, s.length() - 2); }
        else if (s.endsWith("g"))  { multiplier = 1024L * 1024L * 1024L; s = s.substring(0, s.length() - 1); }
        else if (s.endsWith("m"))  { multiplier = 1024L * 1024L;          s = s.substring(0, s.length() - 1); }
        else if (s.endsWith("k"))  { multiplier = 1024L;                   s = s.substring(0, s.length() - 1); }
        else if (s.endsWith("b"))  { multiplier = 1L;                       s = s.substring(0, s.length() - 1); }
        try {
            double n = Double.parseDouble(s.trim());
            return (long) (n * multiplier);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /** Convenience: parse {@link #maxMemory} into bytes. */
    public long getMaxMemoryBytes() {
        return parseMemoryToBytes(maxMemory);
    }
}




