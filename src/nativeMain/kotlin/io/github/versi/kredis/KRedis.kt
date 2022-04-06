package io.github.versi.kredis

import io.github.versi.kredis.command.KGetBytesCommand
import io.github.versi.kredis.command.KGetStringCommand
import io.github.versi.kredis.command.KSetBytesCommand
import io.github.versi.kredis.command.KSetStringCommand
import io.github.versi.kredis.hiredis.*
import io.github.versi.kredis.command.KRedisAuthCommand
import io.github.versi.kredis.command.KRedisDeleteCommand
import io.github.versi.kredis.command.KRedisFlushCommand
import kotlinx.atomicfu.locks.SynchronizedObject
import kotlinx.atomicfu.locks.synchronized
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.pointed
import kotlinx.cinterop.toKStringFromUtf8

interface KRedis {

    suspend fun getString(key: String): String

    suspend fun getBytes(key: String): ByteArray?

    suspend fun setString(key: String, value: String, timeToLive: Long?)

    suspend fun setBytes(key: String, value: ByteArray, timeToLive: Long?)

    suspend fun delete(key: String): Boolean

    suspend fun flush()

    fun dispose()

    companion object {

        fun create(address: String, port: Int, password: String? = null): KRedis {
            return KRedisCache(address, port, password)
        }
    }
}

internal interface KRedisCommand<T> {

    val name: String

    fun run(context: CPointer<redisContext>?): T
}

/**
 * Based on https://github.com/redis/hiredis/blob/master/examples/example.c
 * https://github.com/redis/hiredis/blob/master/examples/example-glib.c
 */
internal class KRedisCache(private val address: String, private val port: Int, private val password: String? = null) :
    KRedis, SynchronizedObject() {

    private var context: CPointer<redisContext>? = null

    init {
        prepareContext(address, port, password)
    }

    override suspend fun getString(key: String): String {
        return runCommand(KGetStringCommand(key))
    }

    override suspend fun getBytes(key: String): ByteArray? {
        return runCommand(KGetBytesCommand(key))
    }

    override suspend fun setString(key: String, value: String, timeToLive: Long?) {
        runCommand(KSetStringCommand(key, value, timeToLive))
    }

    override suspend fun setBytes(key: String, value: ByteArray, timeToLive: Long?) {
        runCommand(KSetBytesCommand(key, value, timeToLive))
    }

    override suspend fun delete(key: String): Boolean {
        return runCommand(KRedisDeleteCommand(key))
    }

    override suspend fun flush() {
        return runCommand(KRedisFlushCommand())
    }

    override fun dispose() {
        synchronized(this) {
            clearContext()
        }
    }

    private fun <T> runCommand(redisCommand: KRedisCommand<T>): T {
        synchronized(this) {
            prepareContext(address, port, password)

            try {
                return redisCommand.run(context)
            } catch (exception: Exception) {
                clearContext()
                throw exception
            }
        }
    }

    private fun prepareContext(address: String, port: Int, password: String?) {
        synchronized(this) {
            context?.let {
                // context prepared and working
                if (it.pointed.err <= 0) return
            }

            try {
                context = requireNotNull(redisConnect(address, port)) { "Can't allocate redis context" }
                    .apply {
                        validateError("connecting to Redis")
                    }
                password?.let {
                    KRedisAuthCommand(password).run(context)
                }
            } catch (exception: Exception) {
                clearContext()
                throw exception
            }
        }
    }

    private fun clearContext() {
        context?.let {
            redisFree(context)
            context = null
        }
    }
}

internal fun CPointer<redisContext>.validateError(operationName: String) {
    if (this.pointed.err > 0) {
        throw KRedisException(operationName, pointed.errstr.toKStringFromUtf8())
    }
}

internal fun CPointer<redisReply>.validateError(operationName: String) {
    if (pointed.type == REDIS_REPLY_ERROR) {
        throw KRedisException(operationName, getString())
    }
}

internal fun CPointer<redisReply>.getString(): String {
    return pointed.str?.toKStringFromUtf8().orEmpty()
}

internal fun CPointer<redisReply>.getBoolean(): Boolean {
    return pointed.integer.toInt() == 1
}

class KRedisException(operationName: String, errorMessage: String) :
    Exception("Redis error occurred while performing $operationName operation: $errorMessage")