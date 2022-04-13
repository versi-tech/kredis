package io.github.versi.kredis.command

import io.github.versi.kredis.KRedisCommand
import io.github.versi.kredis.getString
import io.github.versi.kredis.hiredis.*
import io.github.versi.kredis.validateError
import kotlinx.cinterop.*

internal abstract class KRedisGetCommand<T> : KRedisCommand<T> {

    abstract val key: String

    override fun run(context: CPointer<redisContext>?): T {
        var redisReply: COpaquePointer? = null
        try {
            redisReply = redisCommand(context, "GET %s", key)
            val reply = redisReply?.reinterpret<redisReply>()
            reply?.validateError(name)
            return getReturnValue(reply)
        } finally {
            redisReply?.let {
                freeReplyObject(redisReply)
            }
        }
    }

    abstract fun getReturnValue(redisReply: CPointer<redisReply>?): T
}

internal class KGetStringCommand(override val key: String) : KRedisGetCommand<String>() {

    override fun getReturnValue(redisReply: CPointer<redisReply>?): String {
        return redisReply?.getString().orEmpty()
    }

    override val name = "get String"
}

internal class KGetBytesCommand(override val key: String) : KRedisGetCommand<ByteArray?>() {

    override val name = "get bytes"

    override fun getReturnValue(redisReply: CPointer<redisReply>?): ByteArray? {
        return redisReply.readBytes()
    }

    private fun CPointer<redisReply>?.readBytes(): ByteArray? {
        val reply = this?.pointed
        reply?.str?.let {
            val dataSize = reply.len
            return it.readBytes(dataSize.convert())
        }
        return null
    }
}