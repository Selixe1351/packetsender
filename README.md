# PacketSender

A lightweight Redis pub/sub packet library for JVM applications. Define packets, connect to Redis, and send them across your infrastructure with minimal setup.

## Setup

Add the dependency to your `build.gradle` or `build.gradle.kts`:
```groovy
repositories {
    maven {
        url = uri("https://repository.atlantisservices.net/repository/api/")
    }
}

dependencies {
    implementation 'net.atlantisservices:packetsender:1.0.0'
}
```

## Usage

**Define a packet:**
```kotlin
class ChatPacket(val sender: String, val message: String) : Packet {
    override fun receive() {
        println("[$sender]: $message")
    }
}
```

**Connect and send:**
```kotlin
val bus = PacketBus(channel = "my-app")
bus.connect(RedisCredentials("localhost", 6379))

// Async
bus.send(ChatPacket("Steve", "hello world"))

// Sync
bus.sendSync(ChatPacket("Steve", "hello world"))
```

**With authentication:**
```kotlin
bus.connect(RedisCredentials("redis://user:password@localhost:6379"))
```

**Disconnect when done:**
```kotlin
bus.disconnect()
```

## License

Copyright (c) 2026 Atlantis Services. Licensed under the MIT License. See [LICENSE](LICENSE) for details.