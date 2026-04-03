/*
 * Copyright (c) 2026 Atlantis Services.
 * This software is licensed under the MIT License. See LICENSE for details.
 */

package net.atlantisservices.packetsender

import com.google.gson.Gson
import redis.clients.jedis.JedisPubSub

/**
 * Internal Redis pub/sub handler that deserializes incoming messages into [Packet] instances
 * and dispatches them via [Packet.receive].
 *
 * Messages are expected to follow the format: `fully.qualified.ClassName||{json}`.
 * Malformed messages or unknown class names are silently skipped with a warning.
 *
 * @param gson The [Gson] instance used for deserialization. Should match the one used when publishing.
 */
internal class PacketPubSub(private val gson: Gson) : JedisPubSub() {

    /**
     * Invoked by Jedis when a message arrives on the subscribed channel.
     *
     * Parses the class name and JSON payload from the message, deserializes the packet,
     * and calls [Packet.receive]. Exceptions are caught and printed to stderr to prevent
     * the subscriber thread from dying on a bad message.
     *
     * @param channel The channel the message was received on.
     * @param message The raw message string.
     */
    override fun onMessage(channel: String, message: String) {
        try {
            val delimIndex = message.indexOf("||")
            if (delimIndex == -1) return

            val className = message.substring(0, delimIndex)
            val json = message.substring(delimIndex + 2)

            val packetClass = try {
                Class.forName(className).asSubclass(Packet::class.java)
            } catch (e: ClassNotFoundException) {
                System.err.println("Unknown packet class '$className' — ignoring")
                return
            }

            gson.fromJson(json, packetClass).receive()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }
}