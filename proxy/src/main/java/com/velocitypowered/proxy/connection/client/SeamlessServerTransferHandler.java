/*
 * Copyright (C) 2018-2026 Velocity Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.velocitypowered.proxy.connection.client;

import com.velocitypowered.proxy.protocol.packet.JoinGamePacket;
import com.velocitypowered.proxy.protocol.packet.RespawnPacket;
import com.velocitypowered.proxy.protocol.packet.config.FinishedUpdatePacket;
import com.velocitypowered.proxy.protocol.packet.config.StartUpdatePacket;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Suppresses client-side reload packets during configured event-server to event-server transfers.
 */
public final class SeamlessServerTransferHandler extends ChannelOutboundHandlerAdapter {

  public static final String NAME = "seamless-server-transfer";

  private static final Logger LOGGER = LogManager.getLogger(SeamlessServerTransferHandler.class);

  private static final String CONFIG_PACKET_PACKAGE =
      "com.velocitypowered.proxy.protocol.packet.config";

  private final ConnectedPlayer player;

  private boolean suppressingJoinRespawn;

  private boolean suppressingConfiguration;

  public SeamlessServerTransferHandler(ConnectedPlayer player) {
    this.player = player;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (suppressingConfiguration && isConfigurationPacket(msg)) {
      if (msg instanceof FinishedUpdatePacket) {
        finishConfigurationSuppression();
      } else {
        LOGGER.debug("Suppressing client reconfiguration/config packet {} during seamless transfer for {}.",
            msg.getClass().getSimpleName(), player.getUsername());
      }
      drop(msg, promise);
      return;
    }

    if (suppressingJoinRespawn) {
      if (msg instanceof JoinGamePacket) {
        LOGGER.debug("Dropping JoinGamePacket during seamless transfer for {}.", player.getUsername());
        drop(msg, promise);
        return;
      }

      if (msg instanceof RespawnPacket) {
        LOGGER.debug("Dropping RespawnPacket during seamless transfer for {}.", player.getUsername());
        suppressingJoinRespawn = false;
        LOGGER.debug("Ending join/respawn suppression for {}.", player.getUsername());
        drop(msg, promise);
        return;
      }
    }

    super.write(ctx, msg, promise);
  }

  public void startSeamlessTransfer(String previousServerName, String targetServerName) {
    suppressingJoinRespawn = true;
    suppressingConfiguration = true;
    LOGGER.debug("Starting seamless transfer for player {} from {} to {}.",
        player.getUsername(), previousServerName, targetServerName);
  }

  public void finishConfigurationSuppression() {
    if (suppressingConfiguration) {
      suppressingConfiguration = false;
      LOGGER.debug("Finished client reconfiguration suppression during seamless transfer for {}.",
          player.getUsername());
    }
  }

  public void flushSeamlessTransfer() {
    if (suppressingJoinRespawn || suppressingConfiguration) {
      LOGGER.debug("Flushing all seamless transfer state for {}.", player.getUsername());
    }

    suppressingJoinRespawn = false;
    suppressingConfiguration = false;
  }

  private static boolean isConfigurationPacket(Object msg) {
    return msg instanceof StartUpdatePacket
        || msg instanceof FinishedUpdatePacket
        || msg.getClass().getPackageName().equals(CONFIG_PACKET_PACKAGE);
  }

  private static void drop(Object msg, ChannelPromise promise) {
    ReferenceCountUtil.release(msg);
    promise.trySuccess();
  }
}
