package io.github.versi.kredis.command

import io.github.versi.kredis.getStringElements
import io.github.versi.kredis.hiredis.redisReply
import kotlinx.cinterop.CPointer

internal class KRedisKeysCommand(filterPattern: String = "*") :
    KRedisRetrieveCommand<List<String>>(command = "KEYS $filterPattern") {

    override val name: String = "keys"

    override fun getReturnValue(redisReply: CPointer<redisReply>?): List<String> {
        return redisReply?.getStringElements() ?: emptyList()
    }
}