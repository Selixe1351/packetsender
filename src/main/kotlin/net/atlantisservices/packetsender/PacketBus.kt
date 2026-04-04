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
import java.util.concurrent.Executors

/**
 * The core entry point for sending and receiving [Packet]s over a Redis pub/sub channel.
 *
 * Manages a [JedisPool] for publishing and a dedicated daemon thread for subscribing.
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
 * @param executor The [Executor] used for async [send] calls. Defaults to a single-threaded executor.
 *                 In production, prefer supplying a managed thread pool from your application.
 */
class PacketBus(
    private val channel: String = "packets",
    private val gson: Gson = GsonBuilder().serializeNulls().create(),
    private val executor: Executor = Executors.newSingleThreadExecutor(),
    private val classLoader: ClassLoader = PacketBus::class.java.classLoader
) {

    private var pool: JedisPool? = null
    private var credentials: RedisCredentials? = null

    /**
     * Connects to the Redis instance and starts listening for incoming packets on the channel.
     *
     * The subscriber runs on a daemon thread named `PacketBus-Subscriber` and will
     * automatically stop when the JVM shuts down. This method is not thread-safe —
     * call it once before any [send] or [sendSync] calls.
     *
     * @param credentials The [RedisCredentials] to connect with.
     */
    fun connect(credentials: RedisCredentials) {
        this.credentials = credentials

        pool = if (credentials.auth)
            JedisPool(JedisPoolConfig(), credentials.hostname, credentials.port, 20_000, credentials.password)
        else
            JedisPool(JedisPoolConfig(), credentials.hostname, credentials.port, 20_000)

        Thread({
            pool!!.resource.use { jedis ->
                if (credentials.auth) jedis.auth(credentials.password)
                jedis.subscribe(PacketPubSub(gson, classLoader), channel)
            }
        }, "PacketBus-Subscriber").apply { isDaemon = true }.start()
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
     * Closes the underlying [JedisPool] and releases all connections.
     *
     * After calling this, [send] and [sendSync] will throw. The subscriber thread
     * will also terminate as its connection is dropped.
     */
    fun disconnect() {
        pool?.close()
    }

    private fun withJedis(block: (Jedis) -> Unit) {
        pool!!.resource.use { jedis ->
            if (credentials!!.auth) jedis.auth(credentials!!.password)
            block(jedis)
        }
    }

}