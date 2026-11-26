package io.github.redis.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ClusterManager {

    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);
    
    private static volatile ClusterManager instance;
    
    private final Map<String, ClusterNode> nodes;
    private final ClusterSlot[] slots;
    private final ClusterNode myself;
    private final AtomicBoolean clusterEnabled;
    private final AtomicLong currentEpoch;
    private final ScheduledExecutorService scheduler;
    private volatile boolean initialized = false;

    private ClusterManager(String nodeId, String host, int port) {
        this.nodes = new ConcurrentHashMap<>();
        this.slots = new ClusterSlot[ClusterSlot.TOTAL_SLOTS];
        for (int i = 0; i < ClusterSlot.TOTAL_SLOTS; i++) {
            slots[i] = new ClusterSlot(i);
        }
        
        this.myself = new ClusterNode(nodeId, host, port, ClusterNode.ClusterRole.MASTER);
        this.nodes.put(nodeId, myself);
        
        this.clusterEnabled = new AtomicBoolean(false);
        this.currentEpoch = new AtomicLong(0);
        this.scheduler = Executors.newScheduledThreadPool(1, 
            new ThreadFactory() {
                private final AtomicInteger counter = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "cluster-manager-" + counter.getAndIncrement());
                }
            });
    }

    public static ClusterManager getInstance() {
        if (instance == null) {
            synchronized (ClusterManager.class) {
                if (instance == null) {
                    throw new IllegalStateException("ClusterManager not initialized. Call initialize() first.");
                }
            }
        }
        return instance;
    }

    public static synchronized ClusterManager initialize(String nodeId, String host, int port) {
        if (instance != null) {
            throw new IllegalStateException("ClusterManager already initialized");
        }
        instance = new ClusterManager(nodeId, host, port);
        instance.startClusterTasks();
        return instance;
    }

    private void startClusterTasks() {
        if (initialized) return;
        initialized = true;

        scheduler.scheduleAtFixedRate(this::sendPeriodicPings, 1, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::checkNodeFailures, 5, 5, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::updateClusterState, 10, 10, TimeUnit.SECONDS);
        
        logger.info("Cluster manager started for node: {}", myself.getAddress());
    }

    public void enableCluster() {
        clusterEnabled.set(true);
        logger.info("Cluster enabled for node: {}", myself.getAddress());
    }

    public void disableCluster() {
        clusterEnabled.set(false);
        logger.info("Cluster disabled for node: {}", myself.getAddress());
    }

    public boolean isClusterEnabled() {
        return clusterEnabled.get();
    }

    public void addNode(ClusterNode node) {
        if (nodes.containsKey(node.getNodeId())) {
            return;
        }
        
        nodes.put(node.getNodeId(), node);
        logger.info("Added cluster node: {}", node.getAddress());
        
        if (clusterEnabled.get() && !node.getNodeId().equals(myself.getNodeId())) {
            establishLink(node);
        }
    }

    public ClusterNode getNode(String nodeId) {
        return nodes.get(nodeId);
    }

    public ClusterNode getMyself() {
        return myself;
    }

    public Collection<ClusterNode> getAllNodes() {
        return new ArrayList<>(nodes.values());
    }

    public void updateNode(ClusterNode updatedNode) {
        ClusterNode existingNode = nodes.get(updatedNode.getNodeId());
        if (existingNode != null) {
            if (updatedNode.getConfigEpoch() > existingNode.getConfigEpoch()) {
                existingNode.setConfigEpoch(updatedNode.getConfigEpoch());
                existingNode.setRole(updatedNode.getRole());
                existingNode.setMasterNode(updatedNode.getMasterNode());
                existingNode.setPongReceived(System.currentTimeMillis());
            }
        } else {
            addNode(updatedNode);
        }
    }

    public void markNodeAsFailed(String nodeId) {
        ClusterNode node = nodes.get(nodeId);
        if (node != null && !nodeId.equals(myself.getNodeId())) {
            logger.warn("Marking node as failed: {}", node.getAddress());
            node.setConnected(false);
        }
    }

    public void assignSlot(int slotId, ClusterNode ownerNode) {
        if (slotId < 0 || slotId >= ClusterSlot.TOTAL_SLOTS) {
            throw new IllegalArgumentException("Invalid slot ID: " + slotId);
        }
        try {
            ClusterSlot slot = slots[slotId];
            ClusterNode currentOwner = slot.getOwnerNode();
            if (currentOwner != null) currentOwner.removeSlot(slotId);
            slot.setOwnerNode(ownerNode);
            if (ownerNode != null) ownerNode.addSlot(slotId);
        } catch (Throwable t) {
            logger.error("assignSlot failed for slot {} owner={}", slotId, ownerNode, t);
        }
    }

    public ClusterSlot getSlot(int slotId) {
        if (slotId < 0 || slotId >= ClusterSlot.TOTAL_SLOTS) {
            throw new IllegalArgumentException("Invalid slot ID: " + slotId);
        }
        return slots[slotId];
    }

    public ClusterNode getNodeForSlot(int slotId) {
        ClusterSlot slot = getSlot(slotId);
        return slot.getOwnerNode();
    }

    public int keyHashSlot(String key) {
        return CRC16.keyHashSlot(key);
    }

    public ClusterNode getNodeForKey(String key) {
        int slotId = keyHashSlot(key);
        return getNodeForSlot(slotId);
    }

    public boolean isMySlot(int slotId) {
        ClusterNode owner = getNodeForSlot(slotId);
        return owner != null && owner.getNodeId().equals(myself.getNodeId());
    }

    public boolean isMyKey(String key) {
        int slotId = keyHashSlot(key);
        return isMySlot(slotId);
    }

    public void establishLink(ClusterNode remoteNode) {
        if (remoteNode.getNodeId().equals(myself.getNodeId())) {
            return;
        }
        
        ClusterLink link = new ClusterLink(myself, remoteNode);
        remoteNode.setLink(link);
        link.connect();
    }

    public void sendPing(ClusterNode targetNode) {
        if (!clusterEnabled.get() || targetNode.getNodeId().equals(myself.getNodeId())) {
            return;
        }
        
        List<ClusterNode> gossipNodes = selectRandomNodes(3);
        ClusterMessage ping = ClusterMessage.createPing(myself, gossipNodes);
        
        ClusterLink link = targetNode.getLink();
        if (link != null && link.isConnected()) {
            link.sendMessage(ping);
            myself.setPingSent(System.currentTimeMillis());
        }
    }

    public void sendPong(ClusterNode targetNode) {
        if (!clusterEnabled.get() || targetNode.getNodeId().equals(myself.getNodeId())) {
            return;
        }
        
        List<ClusterNode> gossipNodes = selectRandomNodes(3);
        ClusterMessage pong = ClusterMessage.createPong(myself, gossipNodes);
        
        ClusterLink link = targetNode.getLink();
        if (link != null && link.isConnected()) {
            link.sendMessage(pong);
        }
    }

    private void sendPeriodicPings() {
        if (!clusterEnabled.get()) {
            return;
        }
        
        for (ClusterNode node : nodes.values()) {
            if (!node.getNodeId().equals(myself.getNodeId()) && node.isConnected()) {
                long timeSinceLastPing = System.currentTimeMillis() - node.getPingSent();
                if (timeSinceLastPing > 1000) {
                    sendPing(node);
                }
            }
        }
    }

    private void checkNodeFailures() {
        if (!clusterEnabled.get()) {
            return;
        }
        
        long now = System.currentTimeMillis();
        for (ClusterNode node : nodes.values()) {
            if (!node.getNodeId().equals(myself.getNodeId())) {
                long timeSinceLastPong = now - node.getPongReceived();
                if (timeSinceLastPong > 30000 && node.isConnected()) {
                    logger.warn("Node {} is not responding, marking as failed", node.getAddress());
                    markNodeAsFailed(node.getNodeId());
                }
            }
        }
    }

    private void updateClusterState() {
        if (!clusterEnabled.get()) {
            return;
        }
        
        long epoch = currentEpoch.incrementAndGet();
        myself.setConfigEpoch(epoch);
        
        for (ClusterNode node : nodes.values()) {
            if (!node.getNodeId().equals(myself.getNodeId()) && node.isConnected()) {
                ClusterLink link = node.getLink();
                if (link != null && link.isConnected()) {
                    link.sendMessage(ClusterMessage.createUpdate(myself));
                }
            }
        }
    }

    private List<ClusterNode> selectRandomNodes(int count) {
        List<ClusterNode> allNodes = new ArrayList<>(nodes.values());
        allNodes.removeIf(n -> n.getNodeId().equals(myself.getNodeId()));
        
        if (allNodes.size() <= count) {
            return allNodes;
        }
        
        Collections.shuffle(allNodes);
        return allNodes.subList(0, count);
    }

    public ClusterState getClusterState() {
        int knownNodes = nodes.size();
        int assignedSlots = 0;
        int mySlots = myself.getSlotCount();
        
        for (ClusterSlot slot : slots) {
            if (slot.getOwnerNode() != null) {
                assignedSlots++;
            }
        }
        
        return new ClusterState(
            myself.getNodeId(),
            myself.getAddress(),
            knownNodes,
            assignedSlots,
            mySlots,
            clusterEnabled.get(),
            currentEpoch.get()
        );
    }

    public void shutdown() {
        logger.info("Shutting down cluster manager");
        
        for (ClusterNode node : nodes.values()) {
            if (node.getLink() != null) {
                node.getLink().disconnect();
            }
        }
        
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public static class ClusterState {
        private final String nodeId;
        private final String address;
        private final int knownNodes;
        private final int assignedSlots;
        private final int mySlots;
        private final boolean enabled;
        private final long epoch;

        public ClusterState(String nodeId, String address, int knownNodes, int assignedSlots, 
                          int mySlots, boolean enabled, long epoch) {
            this.nodeId = nodeId;
            this.address = address;
            this.knownNodes = knownNodes;
            this.assignedSlots = assignedSlots;
            this.mySlots = mySlots;
            this.enabled = enabled;
            this.epoch = epoch;
        }

        public String getNodeId() {
            return nodeId;
        }

        public String getAddress() {
            return address;
        }

        public int getKnownNodes() {
            return knownNodes;
        }

        public int getAssignedSlots() {
            return assignedSlots;
        }

        public int getMySlots() {
            return mySlots;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public long getEpoch() {
            return epoch;
        }

        @Override
        public String toString() {
            return String.format("ClusterState{nodeId=%s, address=%s, knownNodes=%d, assignedSlots=%d/%d, mySlots=%d, enabled=%s, epoch=%d}",
                    nodeId, address, knownNodes, assignedSlots, ClusterSlot.TOTAL_SLOTS, mySlots, enabled, epoch);
        }
    }
}