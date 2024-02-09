package org.opensearch.alerting.action

import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.index.shard.ShardId
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import java.io.IOException

class DocLevelMonitorFanOutResponse : ActionResponse, ToXContentObject {

    val nodeId: String
    val executionId: String
    val monitorId: String
    val shardIdFailureMap: Map<ShardId, Exception>
    val findingIds: List<String>
    // for shards not delegated to nodes sequence number would be -3 (new number shard was not queried),
    val lastRunContexts: Map<String, Any>

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(),
        sin.readString(),
        sin.readString(),
        sin.readMap() as Map<ShardId, Exception>,
        sin.readStringList(),
        sin.readMap()!! as Map<String, Any>,
    )

    constructor(
        nodeId: String,
        executionId: String,
        monitorId: String,
        shardIdFailureMap: Map<ShardId, Exception>,
        findingIds: List<String>,
        lastRunContexts: Map<String, Any>,
    ) : super() {
        this.nodeId = nodeId
        this.executionId = executionId
        this.monitorId = monitorId
        this.shardIdFailureMap = shardIdFailureMap
        this.findingIds = findingIds
        this.lastRunContexts = lastRunContexts
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeMap(lastRunContexts)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field("last_run_contexts", lastRunContexts)
            .endObject()
        return builder
    }
}
