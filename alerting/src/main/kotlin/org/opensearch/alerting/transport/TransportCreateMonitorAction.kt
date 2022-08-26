/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionRequest
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.util.DocLevelMonitorQueries
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.CreateMonitorRequest
import org.opensearch.commons.alerting.action.CreateMonitorResponse
import org.opensearch.commons.utils.recreateObject
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportCreateMonitorAction::class.java)

class TransportCreateMonitorAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val scheduledJobIndices: ScheduledJobIndices,
    val docLevelMonitorQueries: DocLevelMonitorQueries,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<ActionRequest, CreateMonitorResponse>(
    AlertingActions.CREATE_ALERTING_CONFIG_NAME, transportService, actionFilters, ::CreateMonitorRequest
) {
    override fun doExecute(p0: Task?, p1: ActionRequest, p2: ActionListener<CreateMonitorResponse>?) {
        log.info("hit from alerting - successful call")
        val transformedRequest = p1 as? CreateMonitorRequest
            ?: recreateObject(p1) { CreateMonitorRequest(it) }

        val builder = XContentFactory.jsonBuilder()
        transformedRequest.monitor?.toXContent(builder, ToXContent.EMPTY_PARAMS)

        log.info(BytesReference.bytes(builder).utf8ToString())
        p2?.onResponse(CreateMonitorResponse("ab1245"))
    }
}
