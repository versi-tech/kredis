package io.github.versi.kredis.command

import io.github.versi.kredis.KRedisCommand
import io.github.versi.kredis.hiredis.*
import io.github.versi.kredis.validateError
import kotlinx.cinterop.*

internal class KRedisAuthCommand(private val password: String) : KRedisCommand<Unit> {

    override val name = "authentication"

    override fun run(context: CPointer<redisContext>?) {
        var redisReply: COpaquePointer? = null
        try {
            redisReply = redisCommand(context, "AUTH %s", password)
            val reply = redisReply?.reinterpret<redisReply>()
            reply?.validateError(name)
        } finally {
            redisReply?.let {
                freeReplyObject(redisReply)
            }
        }
    }
}