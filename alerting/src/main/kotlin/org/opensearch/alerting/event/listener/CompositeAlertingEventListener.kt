/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.event.listener

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionType
import org.opensearch.alerting.opensearchapi.InjectorContextElement
import org.opensearch.alerting.opensearchapi.withClosableContext
import org.opensearch.client.node.NodeClient
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.AlertingPluginInterface
import org.opensearch.commons.alerting.action.PublishAdRequest
import org.opensearch.commons.alerting.action.PublishFindingsRequest
import org.opensearch.commons.alerting.action.SubscribeAdResponse
import org.opensearch.commons.alerting.action.SubscribeFindingsResponse
import org.opensearch.commons.alerting.model.Finding
import org.opensearch.commons.authuser.User
import org.opensearch.core.action.ActionListener
import org.opensearch.threadpool.ThreadPool

private val log = LogManager.getLogger(CompositeAlertingEventListener::class.java)

class CompositeAlertingEventListener(
    val topics: MutableMap<String, MutableSet<String>>,
    val settings: Settings,
    val threadPool: ThreadPool
) : AlertingEventListener {

    override suspend fun onAdCallbackCalled(client: NodeClient, monitorId: String, user: User?) {
        val listeners = topics["onAdCallbackCalled"]
        listeners?.forEach { listener ->
            val SUBSCRIBE_AD_ACTION_TYPE = ActionType(listener, ::SubscribeAdResponse)
            val request = PublishAdRequest(monitorId)

            withClosableContext(InjectorContextElement(monitorId, settings, threadPool.threadContext, user!!.roles, user)) {
                AlertingPluginInterface.publishAdRequest(
                    client,
                    SUBSCRIBE_AD_ACTION_TYPE,
                    request,
                    object : ActionListener<SubscribeAdResponse> {
                        override fun onResponse(response: SubscribeAdResponse) {}

                        override fun onFailure(e: Exception) {}
                    }
                )
            }
        }
    }

    override fun onFindingCreated(client: NodeClient, monitorId: String, finding: Finding) {
        val listeners = topics["onFindingCreated"]
        listeners?.forEach { listener ->
            val SUBSCRIBE_FINDINGS_ACTION_TYPE = ActionType(listener, ::SubscribeFindingsResponse)
            val request = PublishFindingsRequest(monitorId, finding)

            AlertingPluginInterface.publishFinding(
                client,
                SUBSCRIBE_FINDINGS_ACTION_TYPE,
                request,
                object : ActionListener<SubscribeFindingsResponse> {
                    override fun onResponse(response: SubscribeFindingsResponse) {}

                    override fun onFailure(e: Exception) {}
                }
            )
        }
    }
}
