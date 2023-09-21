/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionRequest
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.event.listener.AlertingEventListenerModule
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.EventListenerRequest
import org.opensearch.commons.alerting.action.EventListenerResponse
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportAddEventListenerAction::class.java)

class TransportAddEventListenerAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val eventListenerModule: AlertingEventListenerModule
) : HandledTransportAction<ActionRequest, EventListenerResponse>(
    AlertingActions.EVENT_LISTENER_ACTION_NAME, transportService, actionFilters,
    ::EventListenerRequest
) {

    override fun doExecute(task: Task, request: ActionRequest, listener: ActionListener<EventListenerResponse>) {
        log.info("hit here- TransportAddEventListenerAction")
        val transformedRequest = request as? EventListenerRequest
            ?: recreateObject(request) { EventListenerRequest(it) }
        eventListenerModule.addEventListener(transformedRequest.listener, transformedRequest.topic)
        listener.onResponse(EventListenerResponse())
    }
}
