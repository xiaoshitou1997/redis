package io.github.redis.cluster;

import java.util.ArrayList;
import java.util.List;

public class ClusterMessage {

    public enum MessageType {
        MEET,
        PING,
        PONG,
        FAIL,
        PUBLISH,
        FAILOVER_AUTH_REQUEST,
        FAILOVER_AUTH_ACK,
        UPDATE
    }

    private final MessageType type;
    private final ClusterNode sender;
    private final long configEpoch;
    private final List<ClusterNode> gossipNodes;
    private final String data;

    public ClusterMessage(MessageType type, ClusterNode sender, long configEpoch, 
                         List<ClusterNode> gossipNodes, String data) {
        this.type = type;
        this.sender = sender;
        this.configEpoch = configEpoch;
        this.gossipNodes = gossipNodes != null ? gossipNodes : new ArrayList<>();
        this.data = data;
    }

    public MessageType getType() {
        return type;
    }

    public ClusterNode getSender() {
        return sender;
    }

    public long getConfigEpoch() {
        return configEpoch;
    }

    public List<ClusterNode> getGossipNodes() {
        return gossipNodes;
    }

    public String getData() {
        return data;
    }

    public List<String> toRedisCommand() {
        List<String> command = new ArrayList<>();
        command.add("CLUSTER");
        command.add(type.name());
        command.add(sender.getNodeId());
        command.add(String.valueOf(sender.getPort()));
        command.add(sender.getHost());
        command.add(String.valueOf(configEpoch));
        command.add(String.valueOf(sender.getRole().ordinal()));
        
        for (ClusterNode node : gossipNodes) {
            command.add(node.getNodeId());
            command.add(String.valueOf(node.getPort()));
            command.add(node.getHost());
            command.add(String.valueOf(node.getConfigEpoch()));
            command.add(String.valueOf(node.getRole().ordinal()));
        }
        
        if (data != null) {
            command.add(data);
        }
        
        return command;
    }

    public static ClusterMessage fromRedisCommand(List<String> command) {
        if (command.size() < 7) {
            throw new IllegalArgumentException("Invalid cluster message format");
        }

        MessageType type = MessageType.valueOf(command.get(1));
        String nodeId = command.get(2);
        int port = Integer.parseInt(command.get(3));
        String host = command.get(4);
        long configEpoch = Long.parseLong(command.get(5));
        int roleOrdinal = Integer.parseInt(command.get(6));
        ClusterNode.ClusterRole role = ClusterNode.ClusterRole.values()[roleOrdinal];

        ClusterNode sender = new ClusterNode(nodeId, host, port, role);
        sender.setConfigEpoch(configEpoch);

        List<ClusterNode> gossipNodes = new ArrayList<>();
        for (int i = 7; i < command.size() - 1; i += 5) {
            if (i + 4 >= command.size()) break;
            
            String gossipNodeId = command.get(i);
            int gossipPort = Integer.parseInt(command.get(i + 1));
            String gossipHost = command.get(i + 2);
            long gossipConfigEpoch = Long.parseLong(command.get(i + 3));
            int gossipRoleOrdinal = Integer.parseInt(command.get(i + 4));
            ClusterNode.ClusterRole gossipRole = ClusterNode.ClusterRole.values()[gossipRoleOrdinal];

            ClusterNode gossipNode = new ClusterNode(gossipNodeId, gossipHost, gossipPort, gossipRole);
            gossipNode.setConfigEpoch(gossipConfigEpoch);
            gossipNodes.add(gossipNode);
        }

        String data = null;
        if (command.size() > 7 + gossipNodes.size() * 5) {
            data = command.get(command.size() - 1);
        }

        return new ClusterMessage(type, sender, configEpoch, gossipNodes, data);
    }

    public static ClusterMessage createPing(ClusterNode sender, List<ClusterNode> gossipNodes) {
        return new ClusterMessage(MessageType.PING, sender, sender.getConfigEpoch(), gossipNodes, null);
    }

    public static ClusterMessage createPong(ClusterNode sender, List<ClusterNode> gossipNodes) {
        return new ClusterMessage(MessageType.PONG, sender, sender.getConfigEpoch(), gossipNodes, null);
    }

    public static ClusterMessage createMeet(ClusterNode sender) {
        return new ClusterMessage(MessageType.MEET, sender, sender.getConfigEpoch(), new ArrayList<>(), null);
    }

    public static ClusterMessage createFail(ClusterNode sender, String failedNodeId) {
        return new ClusterMessage(MessageType.FAIL, sender, sender.getConfigEpoch(), new ArrayList<>(), failedNodeId);
    }

    public static ClusterMessage createUpdate(ClusterNode sender) {
        return new ClusterMessage(MessageType.UPDATE, sender, sender.getConfigEpoch(), new ArrayList<>(), null);
    }
}