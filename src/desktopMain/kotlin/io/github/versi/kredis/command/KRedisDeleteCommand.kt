package io.github.versi.kredis.command

import io.github.versi.kredis.KRedisCommand
import io.github.versi.kredis.getBoolean
import io.github.versi.kredis.hiredis.freeReplyObject
import io.github.versi.kredis.hiredis.redisCommand
import io.github.versi.kredis.hiredis.redisContext
import io.github.versi.kredis.hiredis.redisReply
import io.github.versi.kredis.validateError
import kotlinx.cinterop.COpaquePointer
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.reinterpret

internal class KRedisDeleteCommand(private val key: String) : KRedisCommand<Boolean> {

    override val name = "delete"

    override fun run(context: CPointer<redisContext>?): Boolean {
        var redisReply: COpaquePointer? = null
        try {
            redisReply = redisCommand(context, "DEL %s", key)
            val reply = redisReply?.reinterpret<redisReply>()
            reply?.validateError(name)
            return reply?.getBoolean() ?: false
        } finally {
            redisReply?.let {
                freeReplyObject(redisReply)
            }
        }
    }
}