/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.event.listener

import org.apache.logging.log4j.LogManager
import org.opensearch.common.settings.Settings
import org.opensearch.threadpool.ThreadPool
import java.lang.IllegalStateException
import java.util.concurrent.atomic.AtomicBoolean

private val log = LogManager.getLogger(AlertingEventListenerModule::class.java)

class AlertingEventListenerModule(
    val alertingEventListeners: MutableMap<String, MutableSet<String>> = mutableMapOf(),
    var frozen: AtomicBoolean = AtomicBoolean(false),
    var eventListener: AlertingEventListener? = null,
    val settings: Settings,
    val threadPool: ThreadPool
) {

    public fun addEventListener(listener: String?, topic: String) {
        ensureNotFrozen()
        if (listener == null) {
            throw IllegalArgumentException("listener must not be null")
        }
        if (alertingEventListeners.contains(topic)) {
            if (alertingEventListeners[topic]!!.contains(listener)) {
                throw IllegalArgumentException("listener already added for topic")
            } else {
                alertingEventListeners[topic]!!.add(listener)
            }
        } else {
            alertingEventListeners[topic] = mutableSetOf(listener)
        }
    }

    public fun eventListener(): AlertingEventListener {
        if (!isFrozen()) {
            ensureNotFrozen()
            eventListener = freeze()
        }
        return eventListener!!
    }

    private fun freeze(): CompositeAlertingEventListener {
        if (this.frozen.compareAndSet(false, true)) {
            return CompositeAlertingEventListener(alertingEventListeners, settings, threadPool)
        } else {
            throw IllegalStateException("already frozen")
        }
    }

    private fun ensureNotFrozen() {
        if (this.frozen.get()) {
            throw IllegalArgumentException("Can't modify AlertingEventListenerModule once Alerting callbacks are frozen")
        }
    }

    private fun isFrozen(): Boolean {
        return frozen.get()
    }
}
