package io.github.versi.kredis.command

import io.github.versi.kredis.KRedisCommand
import io.github.versi.kredis.hiredis.freeReplyObject
import io.github.versi.kredis.hiredis.redisCommand
import io.github.versi.kredis.hiredis.redisContext
import io.github.versi.kredis.hiredis.redisReply
import io.github.versi.kredis.validateError
import kotlinx.cinterop.*
import platform.posix.size_t

internal abstract class KRedisSetCommand : KRedisCommand<Unit> {

    protected abstract val key: String
    protected abstract val timeToLive: Long?

    override fun run(context: CPointer<redisContext>?) {
        var redisSetReply: COpaquePointer? = null
        var redisExpireReply: COpaquePointer? = null
        try {
            redisSetReply = setCommand(context)
            val reply = redisSetReply?.reinterpret<redisReply>()
            reply?.validateError(name)
            timeToLive?.let {
                redisExpireReply = setValueExpiration(redisSetReply, context, it)
                val expireReply = redisExpireReply?.reinterpret<redisReply>()
                expireReply?.validateError("$name and expire")
            }
        } finally {
            redisSetReply?.let {
                freeReplyObject(redisSetReply)
            }
            redisExpireReply?.let {
                freeReplyObject(redisExpireReply)
            }
        }
    }

    private fun setValueExpiration(
        redisSetReply: COpaquePointer?,
        context: CPointer<redisContext>?,
        timeToLive: Long
    ): COpaquePointer? {
        redisSetReply?.let {
            return redisCommand(
                context,
                "EXPIRE %s %ld",
                key,
                timeToLive
            )
        } ?: return null
    }

    abstract fun setCommand(context: CPointer<redisContext>?): COpaquePointer?
}

internal class KSetStringCommand(override val key: String, private val value: String, override val timeToLive: Long?) :
    KRedisSetCommand() {

    override val name = "set String"

    override fun setCommand(context: CPointer<redisContext>?): COpaquePointer? {
        return redisCommand(
            context,
            "SET %s %s",
            key,
            value
        )
    }
}

internal class KSetBytesCommand(
    override val key: String,
    private val value: ByteArray,
    override val timeToLive: Long?
) :
    KRedisSetCommand() {

    override val name = "set bytes"

    override fun setCommand(context: CPointer<redisContext>?): COpaquePointer? {
        value.usePinned {
            val inputData = it.addressOf(0)
            return redisCommand(
                context,
                "SET %s %b",
                key,
                inputData,
                it.get().size.convert<size_t>()
            )
        }
    }
}