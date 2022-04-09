package processor

import (
	"encoding/json"

	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/types"
)

// eventGroup composite key
type eventGroup struct {
	sourceId      string
	destinationId string
	sourceBatchId string
	eventName     string
	eventType     string
}

// statusGroup composite key
type statusGroup struct {
	status     string
	statusCode int
}

// EventGroupInfo provides information on an event group
type EventGroupInfo interface {
	// GetKey returns the key of the event group
	GetKey() eventGroup
	// GetMetadata returns the metadata of the event group
	GetMetadata() *MetricMetadata
}

// ReportingRegistry is responsible for capturing reporting metrics
type ReportingRegistry interface {
	// NextEventPu sets the next expected PU where track events will be recorded for the event group
	NextEventPu(event EventGroupInfo, next string, terminal bool)

	// TrackEventStatus tracks the event status in the current pu of the event group
	TrackEventStatus(event EventGroupInfo, status string, code int, resp string, raw json.RawMessage)

	// GenerateReport generates the report
	GenerateReport() []*types.PUReportedMetric
}

// NewReportingRegistry creates a new registry.
// The initial parameter dictates whether reporting
// is performed from the beggining of the processing
// or from an intermediate stage
func NewReportingRegistry(initial bool) ReportingRegistry {
	return &reportingService{
		initial: initial,
		events:  map[eventGroup]*eventGroupStages{},
	}
}

// eventGroupStages holds reports for all pu stages a specific event group
type eventGroupStages struct {
	metadata *MetricMetadata
	pus      []string
	reports  map[string]*puReport
}

func (r *eventGroupStages) getCurrentPuName() string {
	if len(r.pus) == 0 {
		return ""
	}
	return r.pus[len(r.pus)-1]
}

func (r *eventGroupStages) getCurrentPu() *puReport {
	lastPu := r.getCurrentPuName()
	return r.reports[lastPu]
}

func (r *eventGroupStages) nextPu(pu string, terminal bool) {
	lastPu := r.getCurrentPuName()
	if lastPu != pu {
		r.pus = append(r.pus, pu)
		initial := lastPu == ""
		next := &puReport{
			details:  types.CreatePUDetails(lastPu, pu, terminal, initial),
			statuses: map[statusGroup]*types.StatusDetail{},
		}
		r.reports[pu] = next
	}
}

func (r *eventGroupStages) calculateDiffs(eventName, eventType string) {
	for idx, pu := range r.pus {
		if idx == 0 {
			continue
		}
		current := r.reports[pu]
		previous := r.reports[r.pus[idx-1]]
		if diff := previous.total - current.total; diff != 0 {
			diffKey := statusGroup{status: types.DiffStatus, statusCode: 0}
			current.statuses[diffKey] = types.CreateStatusDetail(
				types.DiffStatus,
				diff,
				0,
				"",
				[]byte(`{}`),
				eventName,
				eventType)
		}
	}
}

// puReport holds reports for a specific event group's pu
type puReport struct {
	completed bool
	details   *types.PUDetails
	total     int64
	statuses  map[statusGroup]*types.StatusDetail
}

func (r *puReport) getOrCreateStatus(key statusGroup, code int, resp string, raw json.RawMessage, eventName, eventType string) *types.StatusDetail {
	status, ok := r.statuses[key]
	if !ok {
		status = types.CreateStatusDetail(key.status, 0, code, resp, raw, eventName, eventType)
		r.statuses[key] = status
	}
	return status
}

type reportingService struct {
	initial bool
	events  map[eventGroup]*eventGroupStages
}

func (r *reportingService) NextEventPu(event EventGroupInfo, next string, terminal bool) {
	report := r.getOrCreateEventKeyReport(event)
	report.nextPu(next, terminal)
}

func (r *reportingService) TrackEventStatus(event EventGroupInfo, status string, code int, resp string, raw json.RawMessage) {
	evKey := event.GetKey()
	report := r.getOrCreateEventKeyReport(event)
	key := statusGroup{status, code}
	currentPuReport := report.getCurrentPu()
	currentPuReport.total++
	s := currentPuReport.getOrCreateStatus(key, code, resp, raw, evKey.eventName, evKey.eventType)
	s.Count++
}

func (r *reportingService) getOrCreateEventKeyReport(event EventGroupInfo) *eventGroupStages {
	evKey := event.GetKey()
	ekr, ok := r.events[evKey]
	if !ok {
		ekr = &eventGroupStages{
			metadata: event.GetMetadata(),
			pus:      make([]string, 1),
			reports:  map[string]*puReport{},
		}
		r.events[evKey] = ekr
	}
	return ekr
}

func (r *reportingService) GenerateReport() []*types.PUReportedMetric {
	reports := make([]*types.PUReportedMetric, 0)
	for evKey, eventReports := range r.events {
		eventReports.calculateDiffs(evKey.eventName, evKey.eventType)
		connection := *types.CreateConnectionDetail(
			eventReports.metadata.sourceID,
			eventReports.metadata.destinationID,
			eventReports.metadata.sourceBatchID,
			eventReports.metadata.sourceTaskID,
			eventReports.metadata.sourceTaskRunID,
			eventReports.metadata.sourceJobID,
			eventReports.metadata.sourceJobRunID,
			eventReports.metadata.sourceDefinitionID,
			eventReports.metadata.destinationDefinitionID,
			eventReports.metadata.sourceCategory)

		for idx, pu := range eventReports.pus {
			if idx == 0 && !r.initial {
				continue // skip first iteration
			}
			puReport := eventReports.reports[pu]
			for _, status := range puReport.statuses {
				report := &types.PUReportedMetric{
					ConnectionDetails: connection,
					PUDetails:         *puReport.details,
					StatusDetail:      status,
				}
				reports = append(reports, report)
			}
		}
	}
	return reports
}

func EventGroupInfoAdapter(event *transformer.TransformerResponseT) EventGroupInfo {
	return &transformerEventReportInfo{
		TransformerResponseT: event,
	}
}

// transformerEventReportInfo is an adapter for
type transformerEventReportInfo struct {
	*transformer.TransformerResponseT
	key      *eventGroup
	metadata *MetricMetadata
}

func (r *transformerEventReportInfo) GetKey() eventGroup {
	if r.key == nil {
		r.key = &eventGroup{
			sourceId:      r.Metadata.SourceID,
			destinationId: r.Metadata.DestinationID,
			sourceBatchId: r.Metadata.SourceBatchID,
			eventName:     r.Metadata.EventName,
			eventType:     r.Metadata.EventType,
		}
	}
	return *r.key
}

func (r *transformerEventReportInfo) GetMetadata() *MetricMetadata {
	if r.metadata == nil {
		r.metadata = &MetricMetadata{
			sourceID:                r.Metadata.SourceID,
			destinationID:           r.Metadata.DestinationID,
			sourceBatchID:           r.Metadata.SourceBatchID,
			sourceTaskID:            r.Metadata.SourceTaskID,
			sourceTaskRunID:         r.Metadata.SourceTaskRunID,
			sourceJobID:             r.Metadata.SourceJobID,
			sourceJobRunID:          r.Metadata.SourceJobRunID,
			sourceDefinitionID:      r.Metadata.SourceDefinitionID,
			destinationDefinitionID: r.Metadata.DestinationDefinitionID,
			sourceCategory:          r.Metadata.SourceCategory,
		}
	}

	return r.metadata
}
