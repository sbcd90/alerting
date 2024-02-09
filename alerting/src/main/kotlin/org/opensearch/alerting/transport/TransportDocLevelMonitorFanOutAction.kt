package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.MonitorRunnerExecutionContext
import org.opensearch.alerting.action.DocLevelMonitorFanOutRequest
import org.opensearch.alerting.action.DocLevelMonitorFanOutResponse
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.core.action.ActionListener
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportDocLevelMonitorFanOutAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportDocLevelMonitorFanOutAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    val actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
    val monitorCtx: MonitorRunnerExecutionContext,

) : HandledTransportAction<DocLevelMonitorFanOutRequest, DocLevelMonitorFanOutResponse>(
    AlertingActions.INDEX_MONITOR_ACTION_NAME, transportService, actionFilters, ::DocLevelMonitorFanOutRequest
),
    SecureTransportAction {

    @Volatile
    override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)
    override fun doExecute(
        task: Task,
        request: DocLevelMonitorFanOutRequest,
        listener: ActionListener<DocLevelMonitorFanOutResponse>,
    ) {
        executeMonitor(request, monitorCtx)
    }

    private fun executeMonitor(request: DocLevelMonitorFanOutRequest, monitorCtx: MonitorRunnerExecutionContext) {
    }
}
