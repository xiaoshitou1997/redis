package io.github.redis;

import io.github.redis.bootstrap.Redis;
import io.github.redis.config.ConfigLoader;
import io.github.redis.config.RedisConfig;

import java.io.File;

/**
 * Redis application main entry point
 * Starts Redis server
 */
public class RedisApplication {

    /**
     * Main method
     * Supports loading configuration from config file or command line arguments
     *
     * @param args command line arguments
     *             args[0]: config file path (optional, uses redis.conf by default)
     *             args[1]: host address (optional, overrides config file)
     *             args[2]: port (optional, overrides config file)
     */
    public static void main(String[] args) {
        RedisConfig config = loadConfig(args);

        String host = config.getBind();
        int port = config.getPort();

        // Command line arguments can override config file
        if (args.length >= 2) {
            host = args[1];
        }
        if (args.length >= 3) {
            try {
                port = Integer.parseInt(args[2]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number: " + args[2]);
                System.exit(1);
            }
        }

        System.out.println("Starting Redis server on " + host + ":" + port);
        System.out.println("Configuration loaded: databases=" + config.getDatabases() +
                ", maxmemory=" + config.getMaxMemory());

        Redis redis = new Redis(host, port, config);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(redis::shutdown, "redis-shutdown-hook"));

        redis.start();
    }

    /**
     * Load configuration
     */
    private static RedisConfig loadConfig(String[] args) {
        // First try loading from config file specified in command line arguments
        String configPath = null;
        if (args.length >= 1 && !args[0].startsWith("-")) {
            configPath = args[0];
        } else {
            // Try finding redis.conf in current directory and common locations
            String[] possiblePaths = {
                    "redis.conf",
                    "./redis.conf",
                    "../redis.conf",
                    "conf/redis.conf"
            };

            for (String path : possiblePaths) {
                if (new File(path).exists()) {
                    configPath = path;
                    break;
                }
            }
        }

        if (configPath != null) {
            try {
                System.out.println("Loading configuration from: " + configPath);
                return ConfigLoader.loadFromFile(configPath);
            } catch (Exception e) {
                System.err.println("Warning: Failed to load config file: " + configPath);
                System.err.println("Using default configuration. Error: " + e.getMessage());
            }
        }

        // Use default configuration
        System.out.println("Using default configuration");
        return ConfigLoader.createDefault();
    }
}