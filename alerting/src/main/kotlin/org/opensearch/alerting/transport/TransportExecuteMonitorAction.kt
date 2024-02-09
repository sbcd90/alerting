/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.admin.indices.stats.IndicesStatsAction
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.MonitorMetadataService
import org.opensearch.alerting.MonitorRunnerService
import org.opensearch.alerting.action.ExecuteMonitorAction
import org.opensearch.alerting.action.ExecuteMonitorRequest
import org.opensearch.alerting.action.ExecuteMonitorResponse
import org.opensearch.alerting.model.DocumentLevelTriggerRunResult
import org.opensearch.alerting.model.InputRunResults
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.TriggerRunResult
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.DocLevelMonitorQueries
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.authuser.User
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.time.Instant

private val log = LogManager.getLogger(TransportExecuteMonitorAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportExecuteMonitorAction @Inject constructor(
    transportService: TransportService,
    private val client: Client,
    private val clusterService: ClusterService,
    private val runner: MonitorRunnerService,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
    private val docLevelMonitorQueries: DocLevelMonitorQueries,
    private val settings: Settings
) : HandledTransportAction<ExecuteMonitorRequest, ExecuteMonitorResponse> (
    ExecuteMonitorAction.NAME,
    transportService,
    actionFilters,
    ::ExecuteMonitorRequest
) {
    @Volatile private var indexTimeout = AlertingSettings.INDEX_TIMEOUT.get(settings)

    override fun doExecute(task: Task, execMonitorRequest: ExecuteMonitorRequest, actionListener: ActionListener<ExecuteMonitorResponse>) {
        val userStr = client.threadPool().threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
        log.debug("User and roles string from thread context: $userStr")
        val user: User? = User.parse(userStr)

        client.threadPool().threadContext.stashContext().use {
            val executeMonitor = fun(monitor: Monitor) {
                // Launch the coroutine with the clients threadContext. This is needed to preserve authentication information
                // stored on the threadContext set by the security plugin when using the Alerting plugin with the Security plugin.
                // runner.launch(ElasticThreadContextElement(client.threadPool().threadContext)) {
                runner.launch {
                    val (periodStart, periodEnd) =
                        monitor.schedule.getPeriodEndingAt(Instant.ofEpochMilli(execMonitorRequest.requestEnd.millis))
                    try {
                        log.info(
                            "Executing monitor from API - id: ${monitor.id}, type: ${monitor.monitorType.name}, " +
                                "periodStart: $periodStart, periodEnd: $periodEnd, dryrun: ${execMonitorRequest.dryrun}"
                        )
                        val monitorRunResult = runner.runJob(monitor, periodStart, periodEnd, execMonitorRequest.dryrun)
                        withContext(Dispatchers.IO) {
                            actionListener.onResponse(ExecuteMonitorResponse(monitorRunResult))
                        }
                    } catch (e: Exception) {
                        log.error("Unexpected error running monitor", e)
                        withContext(Dispatchers.IO) {
                            actionListener.onFailure(AlertingException.wrap(e))
                        }
                    }
                }
            }
            val executeMonitors = fun(monitors: List<Monitor>) {
                // Launch the coroutine with the clients threadContext. This is needed to preserve authentication information
                // stored on the threadContext set by the security plugin when using the Alerting plugin with the Security plugin.
                // runner.launch(ElasticThreadContextElement(client.threadPool().threadContext)) {
                runner.launch {
                    val (periodStart, periodEnd) =
                        monitors[0].schedule.getPeriodEndingAt(Instant.ofEpochMilli(execMonitorRequest.requestEnd.millis))
                    val inputRunResults = mutableMapOf<String, MutableSet<String>>()
                    val triggerRunResults = mutableMapOf<String, DocumentLevelTriggerRunResult>()
                    val monitorRunResult = MonitorRunResult<TriggerRunResult>(monitors[0].name, periodStart, periodEnd)
                    monitors.forEach { monitor ->
                        log.info(
                            "Executing monitor from API - id: ${monitor.id}, type: ${monitor.monitorType.name}, " +
                                "periodStart: $periodStart, periodEnd: $periodEnd, dryrun: ${execMonitorRequest.dryrun}"
                        )
                        val childMonitorRunResult = runner.runJob(monitor, periodStart, periodEnd, execMonitorRequest.dryrun)
                        if (childMonitorRunResult.error != null) {
                            monitorRunResult.error = childMonitorRunResult.error
                        } else {
                            val childInputRunResults = childMonitorRunResult.inputResults.results[0]
                            childInputRunResults.forEach {
                                if (inputRunResults.containsKey(it.key)) {
                                    val existingResults = inputRunResults[it.key]
                                    existingResults!!.addAll(it.value as Set<String>)
                                    inputRunResults[it.key] = existingResults
                                } else {
                                    inputRunResults[it.key] = it.value as MutableSet<String>
                                }
                            }
                            childMonitorRunResult.triggerResults.forEach {
                                if (triggerRunResults.containsKey(it.key)) {
                                    val newDocs = mutableListOf<String>()
                                    val existingResults = triggerRunResults[it.key]

                                    newDocs.addAll(existingResults!!.triggeredDocs)
                                    newDocs.addAll((it.value as DocumentLevelTriggerRunResult).triggeredDocs)

                                    triggerRunResults[it.key] = existingResults.copy(triggeredDocs = newDocs)
                                } else {
                                    triggerRunResults[it.key] = it.value as DocumentLevelTriggerRunResult
                                }
                            }
                        }
                    }

                    try {
                        withContext(Dispatchers.IO) {
                            actionListener.onResponse(
                                ExecuteMonitorResponse(
                                    monitorRunResult.copy(
                                        inputResults = InputRunResults(listOf(inputRunResults)),
                                        triggerResults = triggerRunResults
                                    )
                                )
                            )
                        }
                    } catch (e: Exception) {
                        log.error("Unexpected error running monitor", e)
                        withContext(Dispatchers.IO) {
                            actionListener.onFailure(AlertingException.wrap(e))
                        }
                    }
                }
            }

            if (execMonitorRequest.monitorId != null) {
                val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX).id(execMonitorRequest.monitorId)
                client.get(
                    getRequest,
                    object : ActionListener<GetResponse> {
                        override fun onResponse(response: GetResponse) {
                            if (!response.isExists) {
                                actionListener.onFailure(
                                    AlertingException.wrap(
                                        OpenSearchStatusException("Can't find monitor with id: ${response.id}", RestStatus.NOT_FOUND)
                                    )
                                )
                                return
                            }
                            if (!response.isSourceEmpty) {
                                XContentHelper.createParser(
                                    xContentRegistry,
                                    LoggingDeprecationHandler.INSTANCE,
                                    response.sourceAsBytesRef,
                                    XContentType.JSON
                                ).use { xcp ->
                                    val monitor = ScheduledJob.parse(xcp, response.id, response.version) as Monitor
                                    if (monitor.monitorType == Monitor.MonitorType.DOC_LEVEL_MONITOR) {
                                        val request: SearchRequest = SearchRequest()
                                            .indices(ScheduledJob.SCHEDULED_JOBS_INDEX)
                                            .source(
                                                SearchSourceBuilder()
                                                    .query(QueryBuilders.matchQuery("monitor.owner", monitor.id))
                                                    .size(10000)
                                            )
                                        client.search(
                                            request,
                                            object : ActionListener<SearchResponse> {
                                                override fun onResponse(response: SearchResponse) {
                                                    val childMonitors = mutableListOf<Monitor>()
                                                    response.hits.forEach { hit ->
                                                        XContentHelper.createParser(
                                                            xContentRegistry,
                                                            LoggingDeprecationHandler.INSTANCE,
                                                            hit.sourceRef,
                                                            XContentType.JSON
                                                        ).use { xcp ->
                                                            val childMonitor = ScheduledJob.parse(xcp, hit.id, hit.version) as Monitor
                                                            childMonitors.add(childMonitor)
                                                        }
                                                    }
                                                    executeMonitors(childMonitors)
                                                }

                                                override fun onFailure(t: Exception) {
                                                    actionListener.onFailure(AlertingException.wrap(t))
                                                }
                                            }
                                        )
                                    } else {
                                        executeMonitor(monitor)
                                    }
                                }
                            }
                        }

                        override fun onFailure(t: Exception) {
                            actionListener.onFailure(AlertingException.wrap(t))
                        }
                    }
                )
            } else {
                val monitor = when (user?.name.isNullOrEmpty()) {
                    true -> execMonitorRequest.monitor as Monitor
                    false -> (execMonitorRequest.monitor as Monitor).copy(user = user)
                }

                if (monitor.monitorType == Monitor.MonitorType.DOC_LEVEL_MONITOR) {
                    try {
                        scope.launch {
                            if (!docLevelMonitorQueries.docLevelQueryIndexExists(monitor.dataSources)) {
                                docLevelMonitorQueries.initDocLevelQueryIndex(monitor.dataSources)
                                log.info("Central Percolation index ${ScheduledJob.DOC_LEVEL_QUERIES_INDEX} created")
                            }
                            val (metadata, _) = MonitorMetadataService.getOrCreateMetadata(monitor, skipIndex = true)
                            docLevelMonitorQueries.indexDocLevelQueries(
                                monitor,
                                monitor.id,
                                metadata,
                                WriteRequest.RefreshPolicy.IMMEDIATE,
                                indexTimeout
                            )
                            log.info("Queries inserted into Percolate index ${ScheduledJob.DOC_LEVEL_QUERIES_INDEX}")
                            val shardInfoMap = getShards((monitor.inputs[0] as DocLevelMonitorInput).indices)
                            val indexShardPairs = mutableListOf<String>()
                            shardInfoMap.forEach {
                                val index = it.key
                                val shards = it.value
                                shards.forEach { shard ->
                                    indexShardPairs.add("$index:$shard")
                                }
                            }
                            executeMonitor(monitor.copy(isChild = true, shards = indexShardPairs))
                        }
                    } catch (t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                } else {
                    executeMonitor(monitor)
                }
            }
        }
    }

    private suspend fun getShards(indices: List<String>): Map<String, List<String>> {
        return indices.associateWith { index ->
            val request = IndicesStatsRequest().indices(index).clear()
            val response: IndicesStatsResponse =
                client.suspendUntil { execute(IndicesStatsAction.INSTANCE, request, it) }
            if (response.status != RestStatus.OK) {
                val errorMessage = "Failed fetching index stats for index:$index"
                throw AlertingException(
                    errorMessage,
                    RestStatus.INTERNAL_SERVER_ERROR,
                    IllegalStateException(errorMessage)
                )
            }
            val shards = response.shards.filter { it.shardRouting.primary() && it.shardRouting.active() }
            shards.map { it.shardRouting.id.toString() }
        }
    }
}
