package io.github.versi.kredis.command

import io.github.versi.kredis.getString
import io.github.versi.kredis.hiredis.*
import kotlinx.cinterop.*

internal abstract class KRedisGetCommand<T>(key: String) : KRedisRetrieveCommand<T>(command = "GET %s", arg = key)
internal class KGetStringCommand(key: String) : KRedisGetCommand<String>(key) {

    override fun getReturnValue(redisReply: CPointer<redisReply>?): String {
        return redisReply?.getString().orEmpty()
    }

    override val name = "get String"
}

internal class KGetBytesCommand(key: String) : KRedisGetCommand<ByteArray?>(key) {

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