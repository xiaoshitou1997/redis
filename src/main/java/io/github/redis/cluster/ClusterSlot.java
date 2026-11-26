package io.github.redis.cluster;

public class ClusterSlot {

    public static final int TOTAL_SLOTS = 16384;

    private final int slotId;
    private ClusterNode ownerNode;
    private ClusterNode importingFrom;
    private ClusterNode migratingTo;
    private SlotState state;

    public enum SlotState {
        STABLE,
        IMPORTING,
        MIGRATING
    }

    public ClusterSlot(int slotId) {
        if (slotId < 0 || slotId >= TOTAL_SLOTS) {
            throw new IllegalArgumentException("Slot ID must be between 0 and " + (TOTAL_SLOTS - 1));
        }
        this.slotId = slotId;
        this.state = SlotState.STABLE;
    }

    public int getSlotId() {
        return slotId;
    }

    public ClusterNode getOwnerNode() {
        return ownerNode;
    }

    public void setOwnerNode(ClusterNode ownerNode) {
        this.ownerNode = ownerNode;
    }

    public ClusterNode getImportingFrom() {
        return importingFrom;
    }

    public void setImportingFrom(ClusterNode importingFrom) {
        this.importingFrom = importingFrom;
        if (importingFrom != null) {
            this.state = SlotState.IMPORTING;
        } else {
            updateState();
        }
    }

    public ClusterNode getMigratingTo() {
        return migratingTo;
    }

    public void setMigratingTo(ClusterNode migratingTo) {
        this.migratingTo = migratingTo;
        if (migratingTo != null) {
            this.state = SlotState.MIGRATING;
        } else {
            updateState();
        }
    }

    public SlotState getState() {
        return state;
    }

    private void updateState() {
        if (importingFrom != null) {
            this.state = SlotState.IMPORTING;
        } else if (migratingTo != null) {
            this.state = SlotState.MIGRATING;
        } else {
            this.state = SlotState.STABLE;
        }
    }

    public boolean isStable() {
        return state == SlotState.STABLE;
    }

    public boolean isImporting() {
        return state == SlotState.IMPORTING;
    }

    public boolean isMigrating() {
        return state == SlotState.MIGRATING;
    }

    @Override
    public String toString() {
        return String.format("ClusterSlot{id=%d, owner=%s, state=%s}", 
                slotId, 
                ownerNode != null ? ownerNode.getNodeId() : "none", 
                state);
    }
}