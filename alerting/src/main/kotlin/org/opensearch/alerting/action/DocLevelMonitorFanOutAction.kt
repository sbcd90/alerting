package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class DocLevelMonitorFanOutAction private constructor() : ActionType<DocLevelMonitorFanOutResponse>(NAME, ::DocLevelMonitorFanOutResponse) {
    companion object {
        val INSTANCE = DocLevelMonitorFanOutAction()
        const val NAME = "cluster:admin/opensearch/alerting/monitor/doclevel/fanout"
    }
}
