package org.opensearch.alerting.core

import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.client.AdminClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType

class DocLevelQueriesIndices(private val client: AdminClient, private val clusterService: ClusterService) {

    companion object {
        @JvmStatic
        fun docLevelQueriesMappings(): String {
            return DocLevelQueriesIndices::class.java.classLoader.getResource("mappings/doc-level-queries.json").readText()
        }
    }

    fun initDocLevelQueryIndex(actionListener: ActionListener<CreateIndexResponse>) {
        if (!docLevelQueryIndexExists()) {
            var indexRequest = CreateIndexRequest(ScheduledJob.DOC_LEVEL_QUERIES_INDEX)
                .mapping(ScheduledJob.SCHEDULED_JOB_TYPE, docLevelQueriesMappings(), XContentType.JSON)
                .settings(
                    Settings.builder().put("index.hidden", true)
                        .put("index.percolator.map_unmapped_fields_as_text", true)
                        .build()
                )
            client.indices().create(indexRequest, actionListener)
        }
    }

    fun docLevelQueryIndexExists(): Boolean {
        val clusterState = clusterService.state()
        return clusterState.routingTable.hasIndex(ScheduledJob.DOC_LEVEL_QUERIES_INDEX)
    }
}
