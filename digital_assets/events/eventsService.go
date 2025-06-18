package services

import (
	"context"
	"fmt"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/repository"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
)

type EventsService interface {
	GetEventsData(ctx context.Context) (*datastruct.Events, error)
}

type eventsService struct {
	dao repository.DAO
}

func NewEventsService(dao repository.DAO) EventsService {
	return &eventsService{dao: dao}
}

// GetEventsData Gets all Events from FS
// Takes a context
// Returns (*datastruct.Events, Error)
//
// Gets the Events data from firestore
// Returns the Events and no error if successful
func (e *eventsService) GetEventsData(ctx context.Context) (*datastruct.Events, error) {

	span, labels := common.GenerateSpan("V2 EventsService.GetEventsData", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 EventsService.GetEventsData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 EventsService.GetEventsData"))

	var events datastruct.Events
	queryMGR := e.dao.NewEventsQuery()

	futureEvents, err := queryMGR.GetFutureEventsData(ctx)
	if err != nil {
		log.ErrorL(labels, "Error V2 EventsService.GetEventsData FutureEvents Data from FS: %s", err)
		return nil, err
	}
	pastEvents, err := queryMGR.GetPastEventsData(ctx)
	if err != nil {
		log.ErrorL(labels, "Error V2 EventsService.GetEventsData PastEvents Data from FS: %s", err)
		return nil, err
	}
	featureEvents, err := queryMGR.GetFeaturedEventData(ctx)
	if err != nil {
		log.ErrorL(labels, "Error V2 EventsService.GetEventsData FeaturedEvent Data from FS: %s", err)
		return nil, err
	}

	events.FutureEvents = futureEvents
	events.PastEvents = pastEvents
	events.FeaturedEvent = featureEvents

	log.EndTimeL(labels, "V2 EventsService.GetEventsData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 EventsService.GetEventsData")
	return &events, nil
}
