package io.github.versi.kredis.command

import io.github.versi.kredis.KRedisCommand
import io.github.versi.kredis.hiredis.freeReplyObject
import io.github.versi.kredis.hiredis.redisCommand
import io.github.versi.kredis.hiredis.redisContext
import io.github.versi.kredis.hiredis.redisReply
import io.github.versi.kredis.validateError
import kotlinx.cinterop.COpaquePointer
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.reinterpret

internal class KRedisFlushCommand : KRedisCommand<Unit> {

    override val name = "flush"

    override fun run(context: CPointer<redisContext>?) {
        var redisReply: COpaquePointer? = null
        try {
            redisReply = redisCommand(context, "FLUSHDB")
            val reply = redisReply?.reinterpret<redisReply>()
            reply?.validateError(name)
        } finally {
            redisReply?.let {
                freeReplyObject(redisReply)
            }
        }
    }
}