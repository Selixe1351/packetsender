/*
 * Copyright (c) 2026 Atlantis Services.
 * This software is licensed under the MIT License. See LICENSE for details.
 */

package net.atlantisservices.packetsender.credentials

import java.net.URI

/**
 * Holds the connection credentials for a Redis instance.
 *
 * Can be constructed from a full Redis URI or a simple host/port pair.
 *
 * @property uri The full Redis URI (e.g. `redis://user:password@localhost:6379`).
 * @property hostname The resolved hostname of the Redis instance.
 * @property port The resolved port of the Redis instance. Defaults to `6379` if not specified in the URI.
 * @property auth Whether the Redis instance requires authentication.
 * @property password The password to authenticate with. Empty string if [auth] is false.
 */
class RedisCredentials(val uri: String) {

    val hostname: String
    val port: Int
    val auth: Boolean
    val password: String

    init {
        val redisUri = URI(uri)
        hostname = redisUri.host
        port = if (redisUri.port != -1) redisUri.port else 6379

        if (redisUri.userInfo != null && redisUri.userInfo.contains(":")) {
            val parts = redisUri.userInfo.split(":")
            auth = true
            password = parts[1]
        } else {
            auth = false
            password = ""
        }
    }

    /**
     * Constructs [RedisCredentials] from a plain host and port, without authentication.
     *
     * @param host The hostname of the Redis instance.
     * @param port The port of the Redis instance.
     */
    constructor(host: String, port: Int) : this("redis://$host:$port")
}