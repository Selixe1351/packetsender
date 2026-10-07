/*
 * Copyright (c) 2026 Atlantis Services.
 * This software is licensed under the MIT License. See LICENSE for details.
 */

package net.atlantisservices.packetsender

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import net.atlantisservices.packetsender.credentials.RedisCredentials
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * The core entry point for sending and receiving [Packet]s over a Redis pub/sub channel.
 *
 * Manages a [JedisPool] for publishing and a dedicated daemon thread for subscribing.
 * If the subscriber connection drops (e.g. Redis restarts), it reconnects automatically
 * every [RECONNECT_DELAY_MS] until [disconnect] is called. Packets published while the
 * subscriber is disconnected are not delivered to it.
 *
 * Call [connect] before sending any packets, and [disconnect] when done.
 *
 * Example:
 * ```kotlin
 * val bus = PacketBus(channel = "my-app")
 * bus.connect(RedisCredentials("localhost", 6379))
 *
 * bus.send(ChatPacket("Steve", "hello"))
 * ```
 *
 * @param channel The Redis pub/sub channel to publish and subscribe on. Defaults to `"packets"`.
 * @param gson The [Gson] instance used for serialization and deserialization.
 *             Supply a custom instance if your packets require type adapters.
 * @param executor The [Executor] used for async [send] calls. If null, the bus creates a
 *                 single daemon thread and shuts it down in [disconnect]. A supplied executor
 *                 is never shut down by the bus.
 * @param classLoader The class loader used to resolve incoming packet classes.
 */
class PacketBus(
    private val channel: String = "packets",
    private val gson: Gson = GsonBuilder().serializeNulls().create(),
    executor: Executor? = null,
    private val classLoader: ClassLoader = PacketBus::class.java.classLoader
) {

    private val ownedExecutor: ExecutorService? = if (executor == null)
        Executors.newSingleThreadExecutor { Thread(it, "PacketBus-Publisher").apply { isDaemon = true } }
    else null
    private val executor: Executor = executor ?: ownedExecutor!!

    private var pool: JedisPool? = null
    private var credentials: RedisCredentials? = null

    @Volatile private var running = false
    @Volatile private var pubSub: PacketPubSub? = null
    private var subscriber: Thread? = null

    /**
     * Connects to the Redis instance and starts listening for incoming packets on the channel.
     *
     * The subscriber runs on a daemon thread named `PacketBus-Subscriber`. It keeps retrying
     * while Redis is unreachable, so this method does not fail when Redis is down. This method
     * is not thread-safe — call it once before any [send] or [sendSync] calls.
     *
     * @param credentials The [RedisCredentials] to connect with.
     */
    fun connect(credentials: RedisCredentials) {
        this.credentials = credentials

        val pool = if (credentials.auth)
            JedisPool(JedisPoolConfig(), credentials.hostname, credentials.port, 20_000, credentials.password)
        else
            JedisPool(JedisPoolConfig(), credentials.hostname, credentials.port, 20_000)
        this.pool = pool

        running = true
        subscriber = Thread({ subscribeLoop(pool, credentials) }, "PacketBus-Subscriber")
            .apply { isDaemon = true }
            .also { it.start() }
    }

    private fun subscribeLoop(pool: JedisPool, credentials: RedisCredentials) {
        while (running) {
            try {
                pool.resource.use { jedis ->
                    if (credentials.auth) jedis.auth(credentials.password)
                    val pubSub = PacketPubSub(gson, classLoader)
                    this.pubSub = pubSub
                    // Re-check after publishing pubSub so a concurrent disconnect() is not missed.
                    if (running) jedis.subscribe(pubSub, channel)
                }
            } catch (e: Exception) {
                if (!running) break
                System.err.println("PacketBus subscriber on '$channel' lost connection: ${e.message}")
            }
            if (!running) break
            try {
                Thread.sleep(RECONNECT_DELAY_MS)
            } catch (e: InterruptedException) {
                break
            }
        }
    }

    /**
     * Publishes a [packet] to the channel asynchronously.
     *
     * Returns a [CompletableFuture] that completes once the packet has been published.
     * Exceptions during publishing will complete the future exceptionally.
     *
     * @param packet The packet to send.
     * @return A [CompletableFuture] that completes when the packet is published.
     */
    fun send(packet: Packet): CompletableFuture<Void> {
        return CompletableFuture.runAsync({
            withJedis { jedis ->
                jedis.publish(channel, "${packet.javaClass.name}||${gson.toJson(packet)}")
            }
        }, executor)
    }

    /**
     * Publishes a [packet] to the channel synchronously, blocking until complete.
     *
     * Prefer [send] for non-blocking usage.
     *
     * @param packet The packet to send.
     */
    fun sendSync(packet: Packet) {
        withJedis { jedis ->
            jedis.publish(channel, "${packet.javaClass.name}||${gson.toJson(packet)}")
        }
    }

    /**
     * Stops the subscriber thread, closes the underlying [JedisPool] and shuts down the
     * executor if the bus created it.
     *
     * After calling this, [send] and [sendSync] will throw.
     */
    fun disconnect() {
        running = false
        pubSub?.let { pubSub ->
            try {
                if (pubSub.isSubscribed) pubSub.unsubscribe()
            } catch (e: Exception) {
                // Connection already gone; the subscriber loop exits on its own.
            }
        }
        subscriber?.interrupt()
        pool?.close()
        ownedExecutor?.shutdown()
    }

    private fun withJedis(block: (Jedis) -> Unit) {
        pool!!.resource.use { jedis ->
            if (credentials!!.auth) jedis.auth(credentials!!.password)
            block(jedis)
        }
    }

    companion object {
        /** Delay between subscriber reconnect attempts. */
        const val RECONNECT_DELAY_MS = 5_000L
    }

}
