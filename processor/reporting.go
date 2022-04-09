package processor

import (
	"encoding/json"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/types"
)

// ReportingService is responsible for capturing reporting metrics
type ReportingService interface {
	// TrackEventStatus tracks a new event status
	TrackEventStatus(pu string, event transformer.TransformerResponseT, status string, code int, resp string, raw json.RawMessage)

	// StageCompleted marks the end of the current stage
	StageCompleted(terminal bool)
	// GenerateReport generates the report
	GenerateReport() []*types.PUReportedMetric
}

// NewReportingService creates a new ReportingService.
// The initial parameter dictates whether the first PU will be
// considered to be the first one or not
func NewReportingService(initial bool) ReportingService {
	return &reportingService{
		initial: initial,
		events:  map[eventKey]eventKeyReports{},
	}
}

type reportingService struct {
	initial bool
	events  map[eventKey]eventKeyReports
}

func (r *reportingService) TrackEventStatus(pu string, event transformer.TransformerResponseT, status string, code int, resp string, raw json.RawMessage) {
	evKey := eventKey{
		sourceId:      event.Metadata.SourceID,
		destinationId: event.Metadata.DestinationID,
		sourceBatchId: event.Metadata.SourceBatchID,
		eventName:     event.Metadata.EventName,
		eventType:     event.Metadata.EventType,
	}
	ekr, ok := r.events[evKey]
	if !ok {
		ekr = eventKeyReports{
			metadata: &MetricMetadata{
				sourceID:                event.Metadata.SourceID,
				destinationID:           event.Metadata.DestinationID,
				sourceBatchID:           event.Metadata.SourceBatchID,
				sourceTaskID:            event.Metadata.SourceTaskID,
				sourceTaskRunID:         event.Metadata.SourceTaskRunID,
				sourceJobID:             event.Metadata.SourceJobID,
				sourceJobRunID:          event.Metadata.SourceJobRunID,
				sourceDefinitionID:      event.Metadata.SourceDefinitionID,
				destinationDefinitionID: event.Metadata.DestinationDefinitionID,
				sourceCategory:          event.Metadata.SourceCategory,
			},
			puReports: map[string]*puReport{},
		}
		r.events[evKey] = ekr
	}

	k := statusKey{jobsdb.Succeeded.State, code}
	currentPuReport, ok := ekr.puReports[pu]
	if !ok {

		// this marks the end of the previous pu, close previous pu if not already closed
		previousPuReport, hasPrevious := ekr.puReports[ekr.currentPU]
		if hasPrevious && !previousPuReport.completed {
			ekr.completeCurrentPU(evKey, false)
		}

		// prepare next pu
		currentPuReport = &puReport{
			details:  types.CreatePUDetails(ekr.currentPU, pu, false, !hasPrevious && r.initial),
			statuses: map[statusKey]*types.StatusDetail{},
		}
		ekr.puReports[pu] = currentPuReport
		ekr.currentPU = pu
	}
	currentPuReport.total++
	s, ok := currentPuReport.statuses[k]
	if !ok {
		s = types.CreateStatusDetail(k.status, 0, code, resp, raw, evKey.eventName, evKey.eventType)
	}
	if !currentPuReport.completed {
		s.Count++ // TODO panic?!
	}
}

func (r *reportingService) StageCompleted(terminal bool) {
	for evKey, ekr := range r.events {
		ekr.completeCurrentPU(evKey, terminal)
	}
}

func (r *reportingService) GenerateReport() []*types.PUReportedMetric {
	reports := make([]*types.PUReportedMetric, 0)
	for _, eventReports := range r.events {
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
		for _, puReport := range eventReports.puReports {
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

// eventKeyReports holds reports for a specific event key
type eventKeyReports struct {
	metadata           *MetricMetadata
	previousStageCount int64
	currentPU          string
	puReports          map[string]*puReport
}

// completeCurrentPU completes the current PU if it is not already completed
func (r *eventKeyReports) completeCurrentPU(evKey eventKey, terminal bool) {
	currentPUReport, ok := r.puReports[r.currentPU]
	if ok && !currentPUReport.completed { // ignore if already completed (idempotent operation)
		currentPUReport.completed = true // mark as completed
		currentPUReport.details.TerminalPU = terminal
		if diff := r.previousStageCount - currentPUReport.total; r.previousStageCount > 0 && diff != 0 {
			stKey := statusKey{status: types.DiffStatus, statusCode: 0}
			currentPUReport.statuses[stKey] = types.CreateStatusDetail(
				types.DiffStatus,
				diff,
				0,
				"",
				[]byte(`{}`),
				evKey.eventName,
				evKey.eventType)
		}
		r.previousStageCount = currentPUReport.total
	}
}

// puReport holds reports for a specific event key and pu
type puReport struct {
	completed bool
	details   *types.PUDetails
	total     int64
	statuses  map[statusKey]*types.StatusDetail
}

// eventKey composite key
type eventKey struct {
	sourceId      string
	destinationId string
	sourceBatchId string
	eventName     string
	eventType     string
}

// statusKey composite key
type statusKey struct {
	status     string
	statusCode int
}
