package io.github.redis.cluster;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ClusterMessageHandler extends SimpleChannelInboundHandler<List<String>> {

    private static final Logger logger = LoggerFactory.getLogger(ClusterMessageHandler.class);
    
    private final ClusterNode localNode;
    private final ClusterNode remoteNode;
    private final ClusterManager clusterManager;

    public ClusterMessageHandler(ClusterNode localNode, ClusterNode remoteNode) {
        this.localNode = localNode;
        this.remoteNode = remoteNode;
        this.clusterManager = ClusterManager.getInstance();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, List<String> command) {
        try {
            if (command.isEmpty() || !command.get(0).equalsIgnoreCase("CLUSTER")) {
                logger.warn("Received non-cluster message from {}", remoteNode.getAddress());
                return;
            }

            ClusterMessage message = ClusterMessage.fromRedisCommand(command);
            handleClusterMessage(message);
        } catch (Exception e) {
            logger.error("Error processing cluster message from {}: {}", remoteNode.getAddress(), e.getMessage(), e);
        }
    }

    private void handleClusterMessage(ClusterMessage message) {
        switch (message.getType()) {
            case MEET:
                handleMeet(message);
                break;
            case PING:
                handlePing(message);
                break;
            case PONG:
                handlePong(message);
                break;
            case FAIL:
                handleFail(message);
                break;
            case UPDATE:
                handleUpdate(message);
                break;
            default:
                logger.warn("Unhandled cluster message type: {}", message.getType());
        }
    }

    private void handleMeet(ClusterMessage message) {
        logger.info("Received MEET from {}", message.getSender().getAddress());
        
        ClusterNode newNode = message.getSender();
        clusterManager.addNode(newNode);
        
        clusterManager.sendPong(newNode);
    }

    private void handlePing(ClusterMessage message) {
        logger.debug("Received PING from {}", message.getSender().getAddress());
        
        ClusterNode sender = message.getSender();
        clusterManager.updateNode(sender);
        
        clusterManager.sendPong(sender);
        
        for (ClusterNode gossipNode : message.getGossipNodes()) {
            clusterManager.updateNode(gossipNode);
        }
    }

    private void handlePong(ClusterMessage message) {
        logger.debug("Received PONG from {}", message.getSender().getAddress());
        
        ClusterNode sender = message.getSender();
        clusterManager.updateNode(sender);
        
        sender.setPongReceived(System.currentTimeMillis());
        
        for (ClusterNode gossipNode : message.getGossipNodes()) {
            clusterManager.updateNode(gossipNode);
        }
    }

    private void handleFail(ClusterMessage message) {
        logger.warn("Received FAIL for node: {}", message.getData());
        
        String failedNodeId = message.getData();
        clusterManager.markNodeAsFailed(failedNodeId);
    }

    private void handleUpdate(ClusterMessage message) {
        logger.debug("Received UPDATE from {}", message.getSender().getAddress());
        
        ClusterNode sender = message.getSender();
        clusterManager.updateNode(sender);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Exception in cluster link to {}: {}", remoteNode.getAddress(), cause.getMessage(), cause);
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        logger.info("Cluster link to {} became inactive", remoteNode.getAddress());
    }
}