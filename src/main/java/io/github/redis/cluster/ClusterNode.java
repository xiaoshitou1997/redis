package io.github.redis.cluster;

import java.util.List;
import java.util.Set;

public class ClusterNode {

    private final String nodeId;
    private final String host;
    private final int port;
    private ClusterRole role;
    private ClusterNode masterNode;
    private final Set<Integer> slots;
    private long pingSent;
    private long pongReceived;
    private long configEpoch;
    private boolean connected;
    private ClusterLink link;

    public enum ClusterRole {
        MASTER,
        REPLICA
    }

    public ClusterNode(String nodeId, String host, int port, ClusterRole role) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.role = role;
        this.slots = new java.util.HashSet<>();
        this.configEpoch = 0;
        this.connected = false;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public ClusterRole getRole() {
        return role;
    }

    public void setRole(ClusterRole role) {
        this.role = role;
    }

    public ClusterNode getMasterNode() {
        return masterNode;
    }

    public void setMasterNode(ClusterNode masterNode) {
        this.masterNode = masterNode;
    }

    public void addSlot(int slot) {
        slots.add(slot);
    }

    public void removeSlot(int slot) {
        slots.remove(slot);
    }

    public Set<Integer> getSlots() {
        return new java.util.HashSet<>(slots);
    }

    public boolean hasSlot(int slot) {
        return slots.contains(slot);
    }

    public int getSlotCount() {
        return slots.size();
    }

    public long getPingSent() {
        return pingSent;
    }

    public void setPingSent(long pingSent) {
        this.pingSent = pingSent;
    }

    public long getPongReceived() {
        return pongReceived;
    }

    public void setPongReceived(long pongReceived) {
        this.pongReceived = pongReceived;
    }

    public long getConfigEpoch() {
        return configEpoch;
    }

    public void setConfigEpoch(long configEpoch) {
        this.configEpoch = configEpoch;
    }

    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    public ClusterLink getLink() {
        return link;
    }

    public void setLink(ClusterLink link) {
        this.link = link;
    }

    public String getAddress() {
        return host + ":" + port;
    }

    @Override
    public String toString() {
        return String.format("ClusterNode{id=%s, address=%s, role=%s, slots=%d}", 
                nodeId, getAddress(), role, slots.size());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterNode that = (ClusterNode) o;
        return nodeId.equals(that.nodeId);
    }

    @Override
    public int hashCode() {
        return nodeId.hashCode();
    }
}