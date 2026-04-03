/*
 * Copyright (c) 2026 Atlantis Services.
 * This software is licensed under the MIT License. See LICENSE for details.
 */

package net.atlantisservices.packetsender

/**
 * Represents a packet that can be sent and received over a [PacketBus].
 *
 * Implement this interface for each distinct message type in your application.
 * The implementing class must be serializable by Gson — all fields should be
 * Gson-compatible types, and the class must have a no-arg constructor (or Gson
 * will use unsafe allocation).
 *
 * Example:
 * ```kotlin
 * class ChatPacket(val sender: String, val message: String) : Packet {
 *     override fun receive() {
 *         println("[$sender]: $message")
 *     }
 * }
 * ```
 */
interface Packet {

    /**
     * Invoked on the subscriber side when this packet is received from the channel.
     *
     * This method is called on the [PacketBus] subscriber thread. Avoid blocking
     * or long-running operations here — offload to your own executor if needed.
     */
    fun receive()

}