package io.github.redis.acl;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class AclManager {

    // 用户列表
    private final Map<String, RedisUser> users = new ConcurrentHashMap<>();
    private volatile String defaultUser = "default";
    private volatile boolean aclEnabled = true;

    // 权限类别
    public static final String ALL_COMMANDS = "+@all";
    public static final String NONE = "-@all";
    public static final String ADMIN = "+@admin";
    public static final String READONLY = "+@read";
    public static final String WRITE = "+@write";

    public AclManager() {
        createDefaultUser();
    }

    public boolean isEnabled() {
        return aclEnabled;
    }

    public void setEnabled(boolean enabled) {
        this.aclEnabled = enabled;
    }

    public String getDefaultUser() {
        return defaultUser;
    }

    public void setDefaultUser(String username) {
        this.defaultUser = username;
    }

    public void createDefaultUser() {
        RedisUser defaultUser = new RedisUser("default");
        defaultUser.setEnabled(true);
        defaultUser.setNoPassword(true);
        defaultUser.addAllowedCommand("*"); // All commands
        defaultUser.addAllowedKey("*"); // All keys
        users.put("default", defaultUser);
    }

    public void createUser(String username, boolean enabled, String password, String... options) {
        RedisUser user = new RedisUser(username);
        user.setEnabled(enabled);

        if (password != null && !password.isEmpty()) {
            user.setPassword(hashPassword(password));
        }

        // Parse options
        int i = 0;
        while (i < options.length) {
            String opt = options[i++];

            if (opt.equals("nopass")) {
                user.setNoPassword(true);
            } else if (opt.startsWith(">")) {
                user.setPassword(hashPassword(opt.substring(1)));
            } else if (opt.startsWith("~")) {
                user.addAllowedKey(opt.substring(1));
            } else if (opt.startsWith("+@")) {
                addCommandGroup(user, opt.substring(2));
            } else if (opt.startsWith("-@")) {
                removeCommandGroup(user, opt.substring(2));
            } else if (opt.startsWith("+")) {
                user.addAllowedCommand(opt.substring(1));
            } else if (opt.startsWith("-")) {
                user.addDeniedCommand(opt.substring(1));
            } else if (opt.equals("on")) {
                user.setEnabled(true);
            } else if (opt.equals("off")) {
                user.setEnabled(false);
            }
        }

        users.put(username.toLowerCase(), user);
    }

    public boolean deleteUser(String username) {
        if (username.equalsIgnoreCase("default")) {
            return false; // Cannot delete default user
        }
        return users.remove(username.toLowerCase()) != null;
    }

    public boolean deleteUser(String... usernames) {
        long count = 0;
        for (String username : usernames) {
            if (deleteUser(username)) {
                count++;
            }
        }
        return count > 0;
    }

    public RedisUser getUser(String username) {
        if (username == null) {
            return users.get(defaultUser);
        }
        return users.get(username.toLowerCase());
    }

    public List<String> getUsernames() {
        return new ArrayList<>(users.keySet());
    }

    public boolean authenticate(String username, String password) {
        if (!aclEnabled) {
            return true; // ACL disabled, always allow
        }

        RedisUser user = getUser(username);
        if (user == null) {
            return false;
        }

        if (!user.isEnabled()) {
            return false;
        }

        if (user.isNoPassword()) {
            return true;
        }

        if (password == null || password.isEmpty()) {
            return false;
        }

        return verifyPassword(user, password);
    }

    public boolean checkCommandPermission(RedisUser user, String command) {
        if (!aclEnabled) {
            return true;
        }

        if (user == null) {
            user = users.get(defaultUser);
        }

        return user.canExecute(command);
    }

    public boolean checkKeyPermission(RedisUser user, String key) {
        if (!aclEnabled) {
            return true;
        }

        if (user == null) {
            user = users.get(defaultUser);
        }

        return user.canAccessKey(key);
    }

    public boolean checkCommandAndKeyPermissions(RedisUser user, String command, String... keys) {
        if (!checkCommandPermission(user, command)) {
            return false;
        }

        for (String key : keys) {
            if (!checkKeyPermission(user, key)) {
                return false;
            }
        }

        return true;
    }

    public Map<String, RedisUser.UserInfo> getUserList() {
        Map<String, RedisUser.UserInfo> result = new HashMap<>();

        for (Map.Entry<String, RedisUser> entry : users.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getUserInfo());
        }

        return result;
    }

    public String generatePassword() {
        // Cryptographically random 256-bit password (ACL GENPASS semantics) —
        // a timestamp-derived value would be predictable.
        return PasswordHasher.generateRandom();
    }

    private String hashPassword(String password) {
        // SHA256 hex hash (Redis 6+ canonical form).
        return PasswordHasher.hashCleartext(password);
    }

    private boolean verifyPassword(RedisUser user, String password) {
        // storedPassword may be the SHA256 hex form (current) or the legacy
        // "<hashCode>:<cleartext>" form left over from older saved state.
        // Legacy entries upgrade in place to SHA256 on the first successful
        // auth, so no plaintext survives in stored state afterwards.
        String stored = user.getPassword();
        if (PasswordHasher.verify(password, stored)) return true;
        if (stored != null) {
            String legacy = password.hashCode() + ":" + password;
            if (PasswordHasher.constantTimeEquals(legacy, stored)) {
                user.setPassword(hashPassword(password));
                return true;
            }
        }
        return false;
    }

    private void addCommandGroup(RedisUser user, String group) {
        Map<String, List<String>> commandGroups = getCommandGroups();
        List<String> commands = commandGroups.get(group);

        if (commands != null) {
            for (String cmd : commands) {
                user.addAllowedCommand(cmd);
            }
        }
    }

    private void removeCommandGroup(RedisUser user, String group) {
        Map<String, List<String>> commandGroups = getCommandGroups();
        List<String> commands = commandGroups.get(group);

        if (commands != null) {
            for (String cmd : commands) {
                user.addDeniedCommand(cmd);
            }
        }
    }

    private Map<String, List<String>> getCommandGroups() {
        Map<String, List<String>> groups = new HashMap<>();

        // All commands (+@all)
        groups.put("all", Arrays.asList("*"));

        // Admin commands
        groups.put("admin", Arrays.asList(
            "FLUSHALL", "FLUSHDB", "SHUTDOWN", "CONFIG", "DEBUG", "MONITOR"
        ));

        // Read commands
        groups.put("read", Arrays.asList(
            "GET", "MGET", "STRLEN", "LRANGE", "LINDEX", "LLEN", "SCARD",
            "SMEMBERS", "SISMEMBER", "SRANDMEMBER", "HGET", "HMGET", "HGETALL",
            "HKEYS", "HLEN", "HEXISTS", "HVALS", "ZCARD", "ZSCORE", "ZRANGE",
            "ZRANK", "TTL", "PTTL", "EXISTS", "KEYS", "TYPE", "RANDOMKEY",
            "PING", "INFO", "CLIENT", "AUTH"
        ));

        // Write commands
        groups.put("write", Arrays.asList(
            "SET", "SETEX", "PSETEX", "SETNX", "DEL", "EXPIRE", "PEXPIRE", "EXPIREAT",
            "PEXPIREAT", "PERSIST", "APPEND", "STRLEN", "LPUSH", "RPUSH",
            "LPUSHX", "RPUSHX", "LPOP", "RPOP", "LREM", "LSET", "LTRIM",
            "SADD", "SREM", "SPOP", "SMOVE", "HSET", "HMSET", "HSETNX",
            "HDEL", "HINCRBY", "HINCRBYFLOAT", "ZADD", "ZREM", "ZINCRBY"
        ));

        return groups;
    }

    public static class RedisUser {
        private final String name;
        private volatile boolean enabled;
        private volatile String password;
        private volatile boolean noPassword;

        // Permissions
        private final Set<String> allowedCommands = ConcurrentHashMap.newKeySet();
        private final Set<String> deniedCommands = ConcurrentHashMap.newKeySet();
        private final Set<String> allowedKeys = ConcurrentHashMap.newKeySet();
        private final Set<String> deniedKeys = ConcurrentHashMap.newKeySet();
        
        // Pub/Sub permissions
        private final Set<String> allowedChannels = ConcurrentHashMap.newKeySet();
        private final Set<String> deniedChannels = ConcurrentHashMap.newKeySet();

        public RedisUser(String name) {
            this.name = name.toLowerCase();
            this.enabled = false;
            this.noPassword = false;
        }

        public String getName() {
            return name;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
            this.noPassword = false;
        }

        public boolean isNoPassword() {
            return noPassword;
        }

        public void setNoPassword(boolean noPassword) {
            this.noPassword = noPassword;
        }

        public void addAllowedCommand(String command) {
            allowedCommands.add(command.toLowerCase());
        }

        public void addDeniedCommand(String command) {
            deniedCommands.add(command.toLowerCase());
        }

        public boolean canExecute(String command) {
            String cmd = command.toLowerCase();

            // Check denied first
            if (deniedCommands.contains(cmd)) {
                return false;
            }

            // Pattern match denied commands
            for (String pattern : deniedCommands) {
                if (pattern.contains("*") && matchPattern(pattern, cmd)) {
                    return false;
                }
            }

            // Check allowed
            if (allowedCommands.contains("*")) {
                return true;
            }

            if (allowedCommands.contains(cmd)) {
                return true;
            }

            // Pattern match allowed commands
            for (String pattern : allowedCommands) {
                if (pattern.contains("*") && matchPattern(pattern, cmd)) {
                    return true;
                }
            }

            return allowedCommands.contains("*");
        }

        public void addAllowedKey(String keyPattern) {
            allowedKeys.add(keyPattern);
        }

        public boolean canAccessKey(String key) {
            // Check denied first
            for (String pattern : deniedKeys) {
                if (matchPattern(pattern, key)) {
                    return false;
                }
            }

            // Check allowed
            for (String pattern : allowedKeys) {
                if (matchPattern(pattern, key)) {
                    return true;
                }
            }

            return allowedKeys.contains("*");
        }

        public void addAllowedChannel(String channelPattern) {
            allowedChannels.add(channelPattern);
        }

        public boolean canPublishToChannel(String channel) {
            for (String pattern : deniedChannels) {
                if (matchPattern(pattern, channel)) {
                    return false;
                }
            }

            for (String pattern : allowedChannels) {
                if (matchPattern(pattern, channel)) {
                    return true;
                }
            }

            return allowedChannels.contains("*");
        }

        private boolean matchPattern(String pattern, String text) {
            if (pattern.equals("*")) {
                return true;
            }

            if (!pattern.contains("*") && !pattern.contains("?")) {
                return pattern.equals(text);
            }

            int p = 0, t = 0, starIdx = -1, match = 0;
            
            while (t < text.length()) {
                if (p < pattern.length() && 
                    (pattern.charAt(p) == text.charAt(t) || pattern.charAt(p) == '?')) {
                    p++;
                    t++;
                } else if (p < pattern.length() && pattern.charAt(p) == '*') {
                    starIdx = p;
                    match = t;
                    p++;
                } else if (starIdx != -1) {
                    p = starIdx + 1;
                    match++;
                    t = match;
                } else {
                    return false;
                }
            }
            
            while (p < pattern.length() && pattern.charAt(p) == '*') {
                p++;
            }
            
            return p == pattern.length();
        }

        public UserInfo getUserInfo() {
            return new UserInfo(name, enabled, noPassword, 
                new ArrayList<>(allowedCommands), 
                new ArrayList<>(deniedCommands));
        }

        public static class UserInfo {
            public final String name;
            public final boolean enabled;
            public final boolean noPassword;
            public final List<String> allowedCommands;
            public final List<String> deniedCommands;

            public UserInfo(String name, boolean enabled, boolean noPassword, 
                          List<String> allowedCommands, List<String> deniedCommands) {
                this.name = name;
                this.enabled = enabled;
                this.noPassword = noPassword;
                this.allowedCommands = allowedCommands;
                this.deniedCommands = deniedCommands;
            }
        }
    }
}
