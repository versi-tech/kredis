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

internal abstract class KRedisRetrieveCommand<T>(private val command: String, private val arg: String? = null) :
    KRedisCommand<T> {

    override fun run(context: CPointer<redisContext>?): T {
        var redisReply: COpaquePointer? = null
        try {
            redisReply = arg?.let {
                redisCommand(context, command, it)
            } ?: redisCommand(context, command)
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