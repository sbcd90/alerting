package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.search.SearchAction
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.MonitorRunnerExecutionContext
import org.opensearch.alerting.action.DocLevelMonitorFanOutRequest
import org.opensearch.alerting.action.DocLevelMonitorFanOutResponse
import org.opensearch.alerting.model.DocumentLevelTriggerRunResult
import org.opensearch.alerting.model.IndexExecutionContext
import org.opensearch.alerting.model.InputRunResults
import org.opensearch.alerting.model.MonitorMetadata
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.Operator
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indices.IndexClosedException
import org.opensearch.percolator.PercolateQueryBuilderExt
import org.opensearch.search.SearchHit
import org.opensearch.search.SearchHits
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.SortOrder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.io.IOException
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import java.util.stream.Collectors

private val log = LogManager.getLogger(TransportDocLevelMonitorFanOutAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportDocLevelMonitorFanOutAction
@Inject constructor(
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
        scope.launch {
            executeMonitor(request, monitorCtx, listener)
        }
    }

    private suspend fun executeMonitor(
        request: DocLevelMonitorFanOutRequest,
        monitorCtx: MonitorRunnerExecutionContext,
        listener: ActionListener<DocLevelMonitorFanOutResponse>,
    ) {
        val monitor = request.monitor
        var monitorResult = MonitorRunResult<DocumentLevelTriggerRunResult>(monitor.name, Instant.now(), Instant.now())
        // todo periodStart periodEnd
        var nonPercolateSearchesTimeTaken = AtomicLong(0)
        var percolateQueriesTimeTaken = AtomicLong(0)
        var totalDocsQueried = AtomicLong(0)
        var docTransformTimeTaken = AtomicLong(0)
        val updatedIndexNames = request.indexExecutionContexts[0].updatedIndexNames
        val concreteIndexNames = request.indexExecutionContexts[0].concreteIndexNames
        val monitorMetadata = request.monitorMetadata
        val queryToDocIds = mutableMapOf<DocLevelQuery, MutableSet<String>>()
        val inputRunResults = mutableMapOf<String, MutableSet<String>>()
        val docsToQueries = mutableMapOf<String, MutableList<String>>()
        val transformedDocs = mutableListOf<Pair<String, TransformedDocDto>>()
        val docsSizeInBytes = AtomicLong(0)
        val shardIds = request.shardIds
        val indexShardsMap: MutableMap<String, MutableList<Int>> = mutableMapOf()
        val queryingStartTimeMillis = System.currentTimeMillis()
        for (shardId in shardIds) {
            if (indexShardsMap.containsKey(shardId.indexName)) {
                indexShardsMap[shardId.indexName]!!.add(shardId.id)
            } else {
                indexShardsMap[shardId.indexName] = mutableListOf(shardId.id)
            }
        }
        val docLevelMonitorInput = request.monitor.inputs[0] as DocLevelMonitorInput
        val queries: List<DocLevelQuery> = docLevelMonitorInput.queries
        val fieldsToBeQueried = mutableSetOf<String>()
        for (it in queries) {
            if (it.queryFieldNames.isEmpty()) {
                fieldsToBeQueried.clear()
                logger.debug(
                    "Monitor ${request.monitor.id} : " +
                        "Doc Level query ${it.id} : ${it.query} doesn't have queryFieldNames populated. " +
                        "Cannot optimize monitor to fetch only query-relevant fields. " +
                        "Querying entire doc source."
                )
                break
            }
            fieldsToBeQueried.addAll(it.queryFieldNames)
        }
        for (entry in indexShardsMap) {
            val indexExecutionContext =
                request.indexExecutionContexts.stream()
                    .filter { it.concreteIndexName.equals(entry.key) }.findAny()
                    .get()
            fetchShardDataAndMaybeExecutePercolateQueries(
                request.monitor,
                monitorCtx,
                indexExecutionContext,
                request.monitorMetadata,
                inputRunResults,
                docsToQueries,
                transformedDocs,
                docsSizeInBytes,
                indexExecutionContext.updatedIndexNames,
                indexExecutionContext.concreteIndexNames,
                ArrayList(fieldsToBeQueried),
                nonPercolateSearchesTimeTaken,
                percolateQueriesTimeTaken,
                totalDocsQueried,
                docTransformTimeTaken
            ) { shard, maxSeqNo -> // function passed to update last run context with new max sequence number
                indexExecutionContext.updatedLastRunContext[shard] = maxSeqNo
            }
        }
        val took = System.currentTimeMillis() - queryingStartTimeMillis
        logger.error("PERF_DEBUG_STAT: Entire query+percolate completed in $took millis in ${request.executionId}")
        monitorResult = monitorResult.copy(inputResults = InputRunResults(listOf(inputRunResults)))
        /* if all indices are covered still in-memory docs size limit is not breached we would need to submit
               the percolate query at the end */
        if (transformedDocs.isNotEmpty()) {
            performPercolateQueryAndResetCounters(
                monitorCtx,
                transformedDocs,
                docsSizeInBytes,
                monitor,
                monitorMetadata,
                updatedIndexNames,
                concreteIndexNames,
                inputRunResults,
                docsToQueries,
                percolateQueriesTimeTaken,
                totalDocsQueried
            )
        }
    }

    /** 1. Fetch data per shard for given index. (only 10000 docs are fetched.
     * needs to be converted to scroll if not performant enough)
     *  2. Transform documents to conform to format required for percolate query
     *  3a. Check if docs in memory are crossing threshold defined by setting.
     *  3b. If yes, perform percolate query and update docToQueries Map with all hits from percolate queries */
    private suspend fun fetchShardDataAndMaybeExecutePercolateQueries(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        indexExecutionCtx: IndexExecutionContext,
        monitorMetadata: MonitorMetadata,
        inputRunResults: MutableMap<String, MutableSet<String>>,
        docsToQueries: MutableMap<String, MutableList<String>>,
        transformedDocs: MutableList<Pair<String, TransformedDocDto>>,
        docsSizeInBytes: AtomicLong,
        monitorInputIndices: List<String>,
        concreteIndices: List<String>,
        fieldsToBeQueried: List<String>,
        nonPercolateSearchesTimeTaken: AtomicLong,
        percolateQueriesTimeTaken: AtomicLong,
        totalDocsQueried: AtomicLong,
        docTransformTimeTake: AtomicLong,
        updateLastRunContext: (String, String) -> Unit,
    ) {
        val count: Int = indexExecutionCtx.updatedLastRunContext["shards_count"] as Int
        for (i: Int in 0 until count) {
            val shard = i.toString()
            try {
                val prevSeqNo = indexExecutionCtx.lastRunContext[shard].toString().toLongOrNull()
                val from = prevSeqNo ?: SequenceNumbers.NO_OPS_PERFORMED
                var to: Long = Long.MAX_VALUE
                while (to >= from) {
                    val hits: SearchHits = searchShard(
                        monitorCtx,
                        indexExecutionCtx.concreteIndexName,
                        shard,
                        from,
                        to,
                        indexExecutionCtx.docIds,
                        fieldsToBeQueried,
                        nonPercolateSearchesTimeTaken
                    )
                    if (hits.hits.isEmpty()) {
                        updateLastRunContext(shard, (prevSeqNo ?: SequenceNumbers.NO_OPS_PERFORMED).toString())
                        break
                    }
                    if (to == Long.MAX_VALUE) { // max sequence number of shard needs to be computed

                        updateLastRunContext(shard, hits.hits[0].seqNo.toString())
                        to = hits.hits[0].seqNo - 10000L
                    } else {
                        to -= 10000L
                    }
                    val startTime = System.currentTimeMillis()
                    transformedDocs.addAll(
                        transformSearchHitsAndReconstructDocs(
                            hits,
                            indexExecutionCtx.indexName,
                            indexExecutionCtx.concreteIndexName,
                            monitor.id,
                            indexExecutionCtx.conflictingFields,
                            docsSizeInBytes
                        )
                    )
                    if (
                        transformedDocs.isNotEmpty() &&
                        shouldPerformPercolateQueryAndFlushInMemoryDocs(
                            docsSizeInBytes,
                            transformedDocs.size,
                            monitorCtx
                        )
                    ) {
                        performPercolateQueryAndResetCounters(
                            monitorCtx,
                            transformedDocs,
                            docsSizeInBytes,
                            monitor,
                            monitorMetadata,
                            monitorInputIndices,
                            concreteIndices,
                            inputRunResults,
                            docsToQueries,
                            percolateQueriesTimeTaken,
                            totalDocsQueried
                        )
                    }
                    docTransformTimeTake.getAndAdd(System.currentTimeMillis() - startTime)
                }
            } catch (e: Exception) {
                logger.error(
                    "Monitor ${monitor.id} :" +
                        "Failed to run fetch data from shard [$shard] of index [${indexExecutionCtx.concreteIndexName}]. " +
                        "Error: ${e.message}",
                    e
                )
                if (e is IndexClosedException) {
                    throw e
                }
            }
        }
    }

    /** Executes search query on given shard of given index to fetch docs with sequence number greater than prevSeqNo.
     * This method hence fetches only docs from shard which haven't been queried before
     */
    private suspend fun searchShard(
        monitorCtx: MonitorRunnerExecutionContext,
        index: String,
        shard: String,
        prevSeqNo: Long?,
        maxSeqNo: Long,
        docIds: List<String>? = null,
        fieldsToFetch: List<String>,
        nonPercolateSearchesTimeTaken: AtomicLong,
    ): SearchHits {
        if (prevSeqNo?.equals(maxSeqNo) == true && maxSeqNo != 0L) {
            return SearchHits.empty()
        }
        val boolQueryBuilder = BoolQueryBuilder()
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("_seq_no").gt(prevSeqNo).lte(maxSeqNo))

        if (!docIds.isNullOrEmpty()) {
            boolQueryBuilder.filter(QueryBuilders.termsQuery("_id", docIds))
        }

        val request: SearchRequest = SearchRequest()
            .indices(index)
            .preference("_shards:$shard")
            .source(
                SearchSourceBuilder()
                    .version(true)
                    .sort("_seq_no", SortOrder.DESC)
                    .seqNoAndPrimaryTerm(true)
                    .query(boolQueryBuilder)
                    .size(10000) // fixme: use scroll to ensure all docs are covered, when number of queryable docs are greater than 10k
            )

        if (AlertingSettings.DOC_LEVEL_MONITOR_FETCH_ONLY_QUERY_FIELDS_ENABLED.get(monitorCtx.settings) && fieldsToFetch.isNotEmpty()) {
            request.source().fetchSource(false)
            for (field in fieldsToFetch) {
                request.source().fetchField(field)
            }
        }
        val response: SearchResponse = monitorCtx.client!!.suspendUntil { monitorCtx.client!!.search(request, it) }
        if (response.status() !== RestStatus.OK) {
            throw IOException("Failed to search shard: [$shard] in index [$index]. Response status is ${response.status()}")
        }
        nonPercolateSearchesTimeTaken.getAndAdd(response.took.millis)
        return response.hits
    }

    /**
     * POJO holding information about each doc's concrete index, id, input index pattern/alias/datastream name
     * and doc source. A list of these POJOs would be passed to percolate query execution logic.
     */
    private data class TransformedDocDto(
        var indexName: String,
        var concreteIndexName: String,
        var docId: String,
        var docSource: BytesReference,
    )

    /** Executes percolate query on the docs against the monitor's query index and return the hits from the search response*/
    private suspend fun runPercolateQueryOnTransformedDocs(
        monitorCtx: MonitorRunnerExecutionContext,
        docs: MutableList<Pair<String, TransformedDocDto>>,
        monitor: Monitor,
        monitorMetadata: MonitorMetadata,
        concreteIndices: List<String>,
        monitorInputIndices: List<String>,
        percolateQueriesTimeTaken: AtomicLong,
    ): SearchHits {
        val indices = docs.stream().map { it.second.indexName }.distinct().collect(Collectors.toList())
        val boolQueryBuilder = BoolQueryBuilder().must(buildShouldClausesOverPerIndexMatchQueries(indices))
        val percolateQueryBuilder =
            PercolateQueryBuilderExt("query", docs.map { it.second.docSource }, XContentType.JSON)
        if (monitor.id.isNotEmpty()) {
            boolQueryBuilder.must(QueryBuilders.matchQuery("monitor_id", monitor.id).operator(Operator.AND))
        }
        boolQueryBuilder.filter(percolateQueryBuilder)
        val queryIndices =
            docs.map { monitorMetadata.sourceToQueryIndexMapping[it.second.indexName + monitor.id] }.distinct()
        if (queryIndices.isEmpty()) {
            val message =
                "Monitor ${monitor.id}: Failed to resolve query Indices from source indices during monitor execution!" +
                    " sourceIndices: $monitorInputIndices"
            logger.error(message)
            throw AlertingException.wrap(
                OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR)
            )
        }

        val searchRequest =
            SearchRequest().indices(*queryIndices.toTypedArray())
        val searchSourceBuilder = SearchSourceBuilder()
        searchSourceBuilder.query(boolQueryBuilder)
        searchRequest.source(searchSourceBuilder)
        logger.debug(
            "Monitor ${monitor.id}: " +
                "Executing percolate query for docs from source indices " +
                "$monitorInputIndices against query index $queryIndices"
        )
        var response: SearchResponse
        try {
            response = monitorCtx.client!!.suspendUntil {
                monitorCtx.client!!.execute(SearchAction.INSTANCE, searchRequest, it)
            }
        } catch (e: Exception) {
            throw IllegalStateException(
                "Monitor ${monitor.id}:" +
                    " Failed to run percolate search for sourceIndex [${concreteIndices.joinToString()}] " +
                    "and queryIndex [${queryIndices.joinToString()}] for ${docs.size} document(s)",
                e
            )
        }

        if (response.status() !== RestStatus.OK) {
            throw IOException(
                "Monitor ${monitor.id}: Failed to search percolate index: ${queryIndices.joinToString()}. " +
                    "Response status is ${response.status()}"
            )
        }
        logger.error("Monitor ${monitor.id} PERF_DEBUG: Percolate query time taken millis = ${response.took}")
        logger.error("Monitor ${monitor.id} PERF_DEBUG: Percolate query response = $response")
        percolateQueriesTimeTaken.getAndAdd(response.took.millis)
        return response.hits
    }

    /** we cannot use terms query because `index` field's mapping is of type TEXT and not keyword. Refer doc-level-queries.json*/
    private fun buildShouldClausesOverPerIndexMatchQueries(indices: List<String>): BoolQueryBuilder {
        val boolQueryBuilder = QueryBuilders.boolQuery()
        indices.forEach { boolQueryBuilder.should(QueryBuilders.matchQuery("index", it)) }
        return boolQueryBuilder
    }

    /** Transform field names and index names in all the search hits to format required to run percolate search against them.
     * Hits are transformed using method transformDocumentFieldNames() */
    private fun transformSearchHitsAndReconstructDocs(
        hits: SearchHits,
        index: String,
        concreteIndex: String,
        monitorId: String,
        conflictingFields: List<String>,
        docsSizeInBytes: AtomicLong,
    ): List<Pair<String, TransformedDocDto>> {
        return hits.mapNotNull(fun(hit: SearchHit): Pair<String, TransformedDocDto>? {
            try {
                val sourceMap = if (hit.hasSource()) {
                    hit.sourceAsMap
                } else {
                    constructSourceMapFromFieldsInHit(hit)
                }
                transformDocumentFieldNames(
                    sourceMap,
                    conflictingFields,
                    "_${index}_$monitorId",
                    "_${concreteIndex}_$monitorId",
                    ""
                )
                var xContentBuilder = XContentFactory.jsonBuilder().map(sourceMap)
                val sourceRef = BytesReference.bytes(xContentBuilder)
                docsSizeInBytes.getAndAdd(sourceRef.ramBytesUsed())
                return Pair(
                    hit.id,
                    TransformedDocDto(index, concreteIndex, hit.id, sourceRef)
                )
            } catch (e: Exception) {
                logger.error(
                    "Monitor $monitorId: Failed to transform payload $hit for percolate query",
                    e
                )
                // skip any document which we fail to transform because we anyway won't be able to run percolate queries on them.
                return null
            }
        })
    }

    private fun constructSourceMapFromFieldsInHit(hit: SearchHit): MutableMap<String, Any> {
        if (hit.fields == null)
            return mutableMapOf()
        val sourceMap: MutableMap<String, Any> = mutableMapOf()
        for (field in hit.fields) {
            if (field.value.values != null && field.value.values.isNotEmpty())
                if (field.value.values.size == 1) {
                    sourceMap[field.key] = field.value.values[0]
                } else sourceMap[field.key] = field.value.values
        }
        return sourceMap
    }

    /**
     * Traverses document fields in leaves recursively and appends [fieldNameSuffixIndex] to field names with same names
     * but different mappings & [fieldNameSuffixPattern] to field names which have unique names.
     *
     * Example for index name is my_log_index and Monitor ID is TReewWdsf2gdJFV:
     * {                         {
     *   "a": {                     "a": {
     *     "b": 1234      ---->       "b_my_log_index_TReewWdsf2gdJFV": 1234
     *   }                          }
     * }
     *
     * @param jsonAsMap               Input JSON (as Map)
     * @param fieldNameSuffix         Field suffix which is appended to existing field name
     */
    private fun transformDocumentFieldNames(
        jsonAsMap: MutableMap<String, Any>,
        conflictingFields: List<String>,
        fieldNameSuffixPattern: String,
        fieldNameSuffixIndex: String,
        fieldNamePrefix: String,
    ) {
        val tempMap = mutableMapOf<String, Any>()
        val it: MutableIterator<Map.Entry<String, Any>> = jsonAsMap.entries.iterator()
        while (it.hasNext()) {
            val entry = it.next()
            if (entry.value is Map<*, *>) {
                transformDocumentFieldNames(
                    entry.value as MutableMap<String, Any>,
                    conflictingFields,
                    fieldNameSuffixPattern,
                    fieldNameSuffixIndex,
                    if (fieldNamePrefix == "") entry.key else "$fieldNamePrefix.${entry.key}"
                )
            } else if (!entry.key.endsWith(fieldNameSuffixPattern) && !entry.key.endsWith(fieldNameSuffixIndex)) {
                var alreadyReplaced = false
                conflictingFields.forEach { conflictingField ->
                    if (conflictingField == "$fieldNamePrefix.${entry.key}" || (fieldNamePrefix == "" && conflictingField == entry.key)) {
                        tempMap["${entry.key}$fieldNameSuffixIndex"] = entry.value
                        it.remove()
                        alreadyReplaced = true
                    }
                }
                if (!alreadyReplaced) {
                    tempMap["${entry.key}$fieldNameSuffixPattern"] = entry.value
                    it.remove()
                }
            }
        }
        jsonAsMap.putAll(tempMap)
    }

    /**
     * Returns true, if the docs fetched from shards thus far amount to less than threshold
     * amount of percentage (default:10. setting is dynamic and configurable) of the total heap size or not.
     *
     */
    private fun isInMemoryDocsSizeExceedingMemoryLimit(
        docsBytesSize: Long,
        monitorCtx: MonitorRunnerExecutionContext,
    ): Boolean {
        var thresholdPercentage =
            AlertingSettings.PERCOLATE_QUERY_DOCS_SIZE_MEMORY_PERCENTAGE_LIMIT.get(monitorCtx.settings)
        val heapMaxBytes = monitorCtx.jvmStats!!.mem.heapMax.bytes
        val thresholdBytes = (thresholdPercentage.toDouble() / 100.0) * heapMaxBytes

        return docsBytesSize > thresholdBytes
    }

    private fun isInMemoryNumDocsExceedingMaxDocsPerPercolateQueryLimit(
        numDocs: Int,
        monitorCtx: MonitorRunnerExecutionContext,
    ): Boolean {
        var maxNumDocsThreshold = AlertingSettings.PERCOLATE_QUERY_MAX_NUM_DOCS_IN_MEMORY.get(monitorCtx.settings)
        return numDocs >= maxNumDocsThreshold
    }

    private suspend fun performPercolateQueryAndResetCounters(
        monitorCtx: MonitorRunnerExecutionContext,
        transformedDocs: MutableList<Pair<String, TransformedDocDto>>,
        docsSizeInBytes: AtomicLong,
        monitor: Monitor,
        monitorMetadata: MonitorMetadata,
        monitorInputIndices: List<String>,
        concreteIndices: List<String>,
        inputRunResults: MutableMap<String, MutableSet<String>>,
        docsToQueries: MutableMap<String, MutableList<String>>,
        percolateQueriesTimeTaken: AtomicLong,
        totalDocsQueried: AtomicLong,
    ) {
        try {
            val percolateQueryResponseHits = runPercolateQueryOnTransformedDocs(
                monitorCtx,
                transformedDocs,
                monitor,
                monitorMetadata,
                concreteIndices,
                monitorInputIndices,
                percolateQueriesTimeTaken
            )

            percolateQueryResponseHits.forEach { hit ->
                var id = hit.id
                concreteIndices.forEach { id = id.replace("_${it}_${monitor.id}", "") }
                monitorInputIndices.forEach { id = id.replace("_${it}_${monitor.id}", "") }
                val docIndices = hit.field("_percolator_document_slot").values.map { it.toString().toInt() }
                docIndices.forEach { idx ->
                    val docIndex = "${transformedDocs[idx].first}|${transformedDocs[idx].second.concreteIndexName}"
                    inputRunResults.getOrPut(id) { mutableSetOf() }.add(docIndex)
                    docsToQueries.getOrPut(docIndex) { mutableListOf() }.add(id)
                }
            }
            totalDocsQueried.getAndAdd(transformedDocs.size.toLong())
        } finally { // no catch block because exception is caught and handled in runMonitor() class
            transformedDocs.clear()
            docsSizeInBytes.set(0)
        }
    }

    private fun shouldPerformPercolateQueryAndFlushInMemoryDocs(
        docsSizeInBytes: AtomicLong,
        numDocs: Int,
        monitorCtx: MonitorRunnerExecutionContext,
    ): Boolean {
        return isInMemoryDocsSizeExceedingMemoryLimit(docsSizeInBytes.get(), monitorCtx) ||
            isInMemoryNumDocsExceedingMaxDocsPerPercolateQueryLimit(numDocs, monitorCtx)
    }
}
