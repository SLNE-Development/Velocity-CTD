/*
 * Copyright (C) 2026 Velocity-CTD Contributors
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

package com.velocityctd.proxy.redis.depot.player;

import com.velocityctd.proxy.redis.VelocityRedis;
import com.velocityctd.proxy.redis.data.VelocityKick;
import com.velocityctd.proxy.redis.depot.AbstractDepotService;
import com.velocitypowered.api.proxy.player.PlayerSettings;
import com.velocitypowered.api.scheduler.ScheduledTask;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.connection.client.ConnectedPlayer;
import com.velocitypowered.proxy.plugin.virtual.VelocityVirtualPlugin;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import net.kyori.adventure.text.Component;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Unmodifiable;

/**
 * Represents an extension of the {@link AbstractDepotService} for the player depot, including
 * functionality to track certain information about a single player, or multiple players.
 */
public final class PlayerDepotService extends AbstractDepotService<UUID, PlayerEntry> {

  /**
   * Diagnostic logger for the "already connected to a remote proxy" investigation. All lines use
   * the shared {@code [REDIS-PLAYER-DEBUG]} prefix so they can be grepped as a single stream.
   */
  private static final Logger LOGGER = LogManager.getLogger(PlayerDepotService.class);

  /**
   * The Redis manager used to coordinate multi-proxy player synchronization.
   */
  private final VelocityRedis redis;

  /**
   * The proxy server instance associated with this depot service.
   */
  private final VelocityServer server;

  /**
   * Scheduled task responsible for periodically updating the total count of players
   * present across all proxies.
   */
  private final ScheduledTask updateTotalPlayerCountTask;

  /**
   * Scheduled task responsible for synchronizing player entries between Redis and
   * the current proxy, ensuring consistency with online players.
   */
  private final ScheduledTask syncPlayerEntriesTask;

  /**
   * The number of players currently recorded across all proxies.
   */
  private int totalPlayerCount = 0;

  /**
   * Constructs a new {@link PlayerDepotService}.
   *
   * @param redis the {@link VelocityRedis} instance
   */
  public PlayerDepotService(@NotNull VelocityRedis redis) {
    super(PlayerEntry.class, redis.getProvider());

    this.redis = redis;
    this.server = redis.getServer();

    this.updateTotalPlayerCountTask = redis.getServer().getScheduler()
            .buildTask(VelocityVirtualPlugin.INSTANCE, this::updateTotalPlayerCount)
            .repeat(Duration.ofMillis(250L))
            .schedule();

    this.syncPlayerEntriesTask = redis.getServer().getScheduler()
            .buildTask(VelocityVirtualPlugin.INSTANCE, this::syncPlayerEntries)
            .repeat(Duration.ofSeconds(1L))
            .schedule();
  }

  @Override
  public void teardown() {
    for (ConnectedPlayer player : this.server.getOnlinePlayers()) {
      this.depot.remove(player.getUniqueId());
    }

    if (this.updateTotalPlayerCountTask != null) {
      this.updateTotalPlayerCountTask.cancel();
    }

    if (this.syncPlayerEntriesTask != null) {
      this.syncPlayerEntriesTask.cancel();
    }
  }

  /**
   * Called when a {@link ConnectedPlayer} connects to the proxy.
   *
   * @param player the player that connected
   * @return {@code true} if the player was successfully added to the depot, {@code false} otherwise
   */
  public boolean onPlayerConnect(ConnectedPlayer player) {
    if (this.redis.isShutdown()) {
      LOGGER.info("[REDIS-PLAYER-DEBUG] stage=PlayerDepotService.onPlayerConnect decision=DENY_REDIS_SHUTDOWN "
          + "localProxyId={} {}", this.redis.getProxyId(), describe(player));
      return false;
    }

    boolean contains = this.depot.contains(player.getUniqueId());
    LOGGER.info("[REDIS-PLAYER-DEBUG] stage=PlayerDepotService.onPlayerConnect action=ENTRY "
        + "localProxyId={} redisContainsUuid={} {}",
        this.redis.getProxyId(), contains, describe(player));

    if (contains) {
      // Re-read once so the existing entry's owner/server can be logged for every branch below.
      PlayerEntry existingEntry = this.depot.get(player.getUniqueId());
      boolean kickExisting = this.server.getConfiguration().isKickExistingPlayers();
      boolean sameProxy = existingEntry != null
          && existingEntry.getProxyId().equalsIgnoreCase(this.redis.getProxyId());
      LOGGER.info("[REDIS-PLAYER-DEBUG] stage=PlayerDepotService.onPlayerConnect action=EXISTING_ENTRY "
          + "localProxyId={} sameProxy={} kickExistingPlayers={} existing[{}] {}",
          this.redis.getProxyId(), sameProxy, kickExisting, describe(existingEntry), describe(player));

      if (kickExisting) {
        Component component = Component.translatable("multiplayer.disconnect.duplicate_login");
        // Only send a VelocityKick if the existing player is on a DIFFERENT proxy.
        // If they are on this proxy, registerConnection() already kicked them locally.
        if (existingEntry != null && !sameProxy) {
          LOGGER.info("[REDIS-PLAYER-DEBUG] stage=PlayerDepotService.onPlayerConnect decision=REMOTE_KICK_EXISTING "
              + "localProxyId={} targetProxyId={} existing[{}] {}",
              this.redis.getProxyId(), existingEntry.getProxyId(), describe(existingEntry), describe(player));
          this.redis.publish(new VelocityKick(player.getUniqueId(), component, existingEntry.getProxyId()));
        }
      } else {
        Component component = Component.translatable("velocity.error.already-connected-proxy.remote");
        LOGGER.info("[REDIS-PLAYER-DEBUG] stage=PlayerDepotService.onPlayerConnect decision=DENY_ALREADY_CONNECTED "
            + "localProxyId={} existing[{}] {}",
            this.redis.getProxyId(), describe(existingEntry), describe(player));
        player.disconnect0(component, true);
        return false;
      }
    }

    PlayerEntry written = this.upsertPlayerEntry(player);
    LOGGER.info("[REDIS-PLAYER-DEBUG] stage=PlayerDepotService.onPlayerConnect decision=ALLOW_UPSERT "
        + "localProxyId={} written[{}] {}",
        this.redis.getProxyId(), describe(written), describe(player));
    return true;
  }

  /**
   * Called when a {@link ConnectedPlayer} disconnects from the proxy.
   *
   * @param player the player that disconnected
   */
  public void onPlayerDisconnect(ConnectedPlayer player) {
    if (this.redis.isShutdown()) {
      LOGGER.info("[REDIS-PLAYER-DEBUG] stage=PlayerDepotService.onPlayerDisconnect decision=SKIP_REDIS_SHUTDOWN {}",
          describe(player));
      return;
    }

    PlayerEntry existing = this.depot.get(player.getUniqueId());
    LOGGER.info("[REDIS-PLAYER-DEBUG] stage=PlayerDepotService.onPlayerDisconnect action=ENTRY "
        + "localProxyId={} existing[{}] {}",
        this.redis.getProxyId(), describe(existing), describe(player));

    if (existing == null) {
      LOGGER.info("[REDIS-PLAYER-DEBUG] stage=PlayerDepotService.onPlayerDisconnect decision=SKIP_NO_ENTRY "
          + "localProxyId={} {}", this.redis.getProxyId(), describe(player));
      return;
    }

    if (!existing.getProxyId().equalsIgnoreCase(this.redis.getProxyId())) {
      LOGGER.info("[REDIS-PLAYER-DEBUG] stage=PlayerDepotService.onPlayerDisconnect decision=SKIP_DIFFERENT_PROXY "
          + "localProxyId={} existing[{}] {}",
          this.redis.getProxyId(), describe(existing), describe(player));
      return;
    }

    ConnectedPlayer currentPlayer = this.server.getPlayer(player.getUniqueId()).orElse(null);
    if (currentPlayer != null && currentPlayer != player) {
      // The registry slot is now owned by a newer connection (fast reconnect / duplicate login).
      // Removing here would delete the entry the new connection just wrote, so we skip.
      LOGGER.info("[REDIS-PLAYER-DEBUG] stage=PlayerDepotService.onPlayerDisconnect "
          + "decision=SKIP_REPLACED_LOCAL_PLAYER localProxyId={} existing[{}] {}",
          this.redis.getProxyId(), describe(existing), describe(player));
      return;
    }

    LOGGER.info("[REDIS-PLAYER-DEBUG] stage=PlayerDepotService.onPlayerDisconnect action=BEFORE_REMOVE "
        + "decision=REMOVE_DISCONNECT localProxyId={} existing[{}] {}",
        this.redis.getProxyId(), describe(existing), describe(player));
    this.depot.remove(player.getUniqueId());
    LOGGER.info("[REDIS-PLAYER-DEBUG] stage=PlayerDepotService.onPlayerDisconnect action=AFTER_REMOVE "
        + "decision=REMOVE_DISCONNECT localProxyId={} removedUuid={} removedUser={} thread={}",
        this.redis.getProxyId(), player.getUniqueId(), player.getUsername(), Thread.currentThread().getName());
  }

  /**
   * Called when a {@link ConnectedPlayer} switches servers.
   *
   * @param player the player that switched servers
   * @param serverName the name of the server that the player switched to
   */
  public void onPlayerSwitchServer(ConnectedPlayer player, String serverName) {
    PlayerEntry playerEntry = this.getPlayerEntry(player.getUniqueId());
    if (playerEntry == null) {
      return;
    }

    playerEntry.setServerName(serverName);
    playerEntry.upsert();
  }

  /**
   * Called when a {@link ConnectedPlayer} changes its {@link PlayerSettings}.
   *
   * @param player the player that got its settings changed
   * @param settings the new settings
   */
  public void onPlayerSettingsChange(ConnectedPlayer player, PlayerSettings settings) {
    PlayerEntry playerEntry = this.getPlayerEntry(player.getUniqueId());
    if (playerEntry == null) {
      return;
    }

    playerEntry.setClientListingAllowed(settings.isClientListingAllowed());
    playerEntry.upsert();
  }

  /**
   * Get the total player count across all proxies, currently present in the depot.
   *
   * @return the total player count
   */
  public int getTotalPlayerCount() {
    return this.totalPlayerCount;
  }

  /**
   * Get a player entry by their unique ID.
   *
   * @param uniqueId the unique ID of the player
   * @return the player entry, or {@code null} if the player is not present in the depot
   */
  public @Nullable PlayerEntry getPlayerEntry(UUID uniqueId) {
    return this.depot.get(uniqueId);
  }

  /**
   * Get a player entry by their username.
   *
   * @param username the username of the player
   * @return the player entry, or {@code null} if the player is not present in the depot
   */
  public @Nullable PlayerEntry getPlayerEntry(String username) {
    for (PlayerEntry entry : this.depot.values()) {
      if (entry.getUsername().equalsIgnoreCase(username)) {
        return entry;
      }
    }

    return null;
  }

  /**
   * Checks whether a player is online.
   *
   * @param uniqueId the unique ID of the player
   * @return {@code true} if the player is online, {@code false} otherwise
   */
  public boolean isPlayerOnline(UUID uniqueId) {
    return this.depot.contains(uniqueId);
  }

  /**
   * Checks whether a player is online.
   *
   * @param username the username of the player
   * @return {@code true} if the player is online, {@code false} otherwise
   */
  public boolean isPlayerOnline(String username) {
    for (PlayerEntry entry : this.depot.values()) {
      if (entry.getUsername().equalsIgnoreCase(username)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Retrieves a list of player entries associated with a specific server.
   *
   * @param serverName the name of the server whose player entries are to be retrieved; must not be null
   * @return an unmodifiable list of {@link PlayerEntry} objects representing the players currently on the specified server; never null
   */
  public @NotNull @Unmodifiable List<PlayerEntry> getPlayerEntriesInServer(@NotNull String serverName) {
    return List.copyOf(this.queryAll(playerEntry -> serverName.equalsIgnoreCase(playerEntry.getServerName())));
  }

  /**
   * Retrieves a list of player entries associated with a specific proxy.
   *
   * @param proxyId the identifier of the proxy whose player entries are to be retrieved;
   *                must not be null or empty
   * @return an unmodifiable list of {@link PlayerEntry} objects representing players
   *         currently associated with the specified proxy; never null
   */
  public @NotNull @Unmodifiable List<PlayerEntry> getPlayerEntriesOnProxy(String proxyId) {
    return List.copyOf(this.queryAll(playerEntry -> playerEntry.getProxyId().equalsIgnoreCase(proxyId)));
  }

  /**
   * Upserts a player's entry in the depot. If an entry for the given player already exists,
   * it is updated with the latest details. If it doesn't exist, a new entry is created.
   *
   * @param player the {@link ConnectedPlayer} object representing the player for whom the entry is to be upserted; must not be null
   * @return the {@link PlayerEntry} that was written to the depot
   */
  public PlayerEntry upsertPlayerEntry(@NotNull ConnectedPlayer player) {
    PlayerEntry playerEntry = new PlayerEntry(player, this.redis.getProxyId());
    playerEntry.setDepot(this.depot);

    this.depot.upsert(playerEntry);
    return playerEntry;
  }

  /**
   * Updates the total player count by recalculating the number of entries in the depot.
   */
  private void updateTotalPlayerCount() {
    if (this.redis.isShutdown()) {
      return;
    }

    this.totalPlayerCount = this.depot.size();
  }

  /**
   * Synchronizes the player entries within the depot. This method ensures that the depot's
   * player entries are kept up to date and consistent with the current state of players on
   * the server.
   */
  private void syncPlayerEntries() {
    if (this.redis.isShutdown()) {
      return;
    }

    // Only mutations are logged below; the common "nothing to do" pass stays silent so the
    // 1Hz task does not spam the log. Note: getOnlinePlayers() returns players that are merely
    // registered (login lock acquired) but not necessarily fullyConnected yet -- watch the
    // local state in SYNC_UPSERT_MISSING_PLAYER for premature/stale Redis writes.
    for (ConnectedPlayer player : this.server.getOnlinePlayers()) {
      if (this.depot.contains(player.getUniqueId())) {
        continue;
      }

      PlayerEntry written = this.upsertPlayerEntry(player);
      LOGGER.info("[REDIS-PLAYER-DEBUG] stage=PlayerDepotService.syncPlayerEntries action=SYNC_UPSERT_MISSING_PLAYER "
          + "localProxyId={} written[{}] {}",
          this.redis.getProxyId(), describe(written), describe(player));
    }

    for (PlayerEntry playerEntry : this.depot.values()) {
      if (!playerEntry.getProxyId().equalsIgnoreCase(this.redis.getProxyId())) {
        continue;
      }

      if (this.server.getPlayer(playerEntry.getUniqueId()).isPresent()) {
        continue;
      }

      LOGGER.info("[REDIS-PLAYER-DEBUG] stage=PlayerDepotService.syncPlayerEntries action=SYNC_REMOVE_STALE_PLAYER "
          + "localProxyId={} removing[{}] thread={}",
          this.redis.getProxyId(), describe(playerEntry), Thread.currentThread().getName());
      playerEntry.remove();
    }
  }

  /**
   * Builds a readable one-line description of a live {@link ConnectedPlayer} for diagnostics,
   * covering identity, connection liveness and the local login/connection state that determines
   * whether this proxy should own a Redis entry for the player.
   *
   * @param player the player to describe
   * @return a space-separated {@code key=value} description
   */
  private static String describe(@Nullable ConnectedPlayer player) {
    if (player == null) {
      return "player=null";
    }

    return String.format(
        "user=%s uuid=%s active=%s closed=%s fullyConnected=%s currentServer=%s thread=%s",
        player.getUsername(), player.getUniqueId(), player.isActive(),
        player.getConnection().isClosed(), player.isFullyConnected(),
        serverNameOf(player), Thread.currentThread().getName());
  }

  /**
   * Builds a readable one-line description of a stored {@link PlayerEntry} for diagnostics,
   * including which proxy currently claims ownership of the entry.
   *
   * @param entry the entry to describe, may be {@code null}
   * @return a space-separated {@code key=value} description, or {@code "none"} when absent
   */
  private static String describe(@Nullable PlayerEntry entry) {
    if (entry == null) {
      return "none";
    }

    return String.format("entryUser=%s entryUuid=%s entryProxyId=%s entryServer=%s",
        entry.getUsername(), entry.getUniqueId(), entry.getProxyId(), entry.getServerName());
  }

  /**
   * Resolves the current backend server name of a player, if connected to one.
   *
   * @param player the player whose server name to resolve
   * @return the backend server name, or {@code null} if the player is not on a server
   */
  private static @Nullable String serverNameOf(@NotNull ConnectedPlayer player) {
    return player.getCurrentServer().map(server -> server.getServerInfo().getName()).orElse(null);
  }
}
