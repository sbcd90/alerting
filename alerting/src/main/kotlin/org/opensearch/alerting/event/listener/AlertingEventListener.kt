/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.event.listener

import org.opensearch.client.node.NodeClient
import org.opensearch.commons.alerting.model.Finding
import org.opensearch.commons.authuser.User

interface AlertingEventListener {

    suspend fun onAdCallbackCalled(client: NodeClient, monitorId: String, user: User?)

    fun onFindingCreated(client: NodeClient, monitorId: String, finding: Finding)
}
