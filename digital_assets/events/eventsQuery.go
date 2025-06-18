package repository

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/firestore"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/common"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/go-tools/log"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
)

type EventsQuery interface {
	GetFutureEventsData(ctx context.Context) ([]datastruct.Event, error)
	GetPastEventsData(ctx context.Context) ([]datastruct.Event, error)
	GetFeaturedEventData(ctx context.Context) (*datastruct.Event, error)
}

type eventsQuery struct{}

// GetFutureEventsData Gets all Future Events from FS
// Takes a context
// Returns ([]datastruct.Event, Error)
//
// Gets the Future Events data from firestore
// Returns the array of Future Events and no error if successful
func (e *eventsQuery) GetFutureEventsData(ctx context.Context) ([]datastruct.Event, error) {

	span, labels := common.GenerateSpan("V2 EventsQuery.GetFutureEventsData", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 EventsQuery.GetFutureEventsData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 EventsQuery.GetFutureEventsData"))

	fs := fsUtils.GetFirestoreClient()

	var futureEvents []datastruct.Event
	iter := fs.Collection(datastruct.EventsCollectionName).Where("startDateTime", ">=", time.Now()).OrderBy("startDateTime", firestore.Asc).Documents(ctx)

	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.ErrorL(labels, "Error V2 EventsQuery.GetFutureEventsData Getting FutureEvents Data from FS: %s", err)
			return nil, err
		}
		var event datastruct.Event
		err = doc.DataTo(&event)
		if err != nil {
			log.ErrorL(labels, "Error V2 EventsQuery.GetFutureEventsData Mapping FutureEvents Data: %s", err)
			return nil, err
		}

		event = e.SplitTimeStampsET(ctx, event)

		futureEvents = append(futureEvents, event)
	}

	log.EndTimeL(labels, "V2 EventsQuery.GetFutureEventsData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 EventsQuery.GetFutureEventsData")
	return futureEvents, nil
}

// GetPastEventsData Gets all Past Events from FS
// Takes a context
// Returns ([]datastruct.Event, Error)
//
// Gets the Past Events data from firestore
// Returns the array of Past Events and no error if successful
func (e *eventsQuery) GetPastEventsData(ctx context.Context) ([]datastruct.Event, error) {

	span, labels := common.GenerateSpan("V2 EventsQuery.GetPastEventsData", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 EventsQuery.GetPastEventsData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 EventsQuery.GetPastEventsData"))

	fs := fsUtils.GetFirestoreClient()

	var pastEvents []datastruct.Event
	iter := fs.Collection(datastruct.EventsCollectionName).Where("endDateTime", "<", time.Now()).OrderBy("endDateTime", firestore.Desc).Documents(ctx)

	for {
		var event datastruct.Event
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.ErrorL(labels, "Error V2 EventsQuery.GetPastEventsData Getting PastEvents Data from FS: %s", err)
			return nil, err
		}
		err = doc.DataTo(&event)
		if err != nil {
			log.ErrorL(labels, "Error V2 EventsQuery.GetPastEventsData Mapping PastEvents Data: %s", err)
			return nil, err
		}

		event = e.SplitTimeStampsET(ctx, event)

		event.TagText = "Past"

		pastEvents = append(pastEvents, event)
	}

	log.EndTimeL(labels, "V2 EventsQuery.GetPastEventsData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 EventsQuery.GetPastEventsData")
	return pastEvents, nil
}

// GetFeaturedEventData Gets all Featured Events from FS
// Takes a context
// Returns (*datastruct.Event, Error)
//
// Gets the Featured Events data from firestore
// Returns the array of Featured Events and no error if successful
func (e *eventsQuery) GetFeaturedEventData(ctx context.Context) (*datastruct.Event, error) {

	span, labels := common.GenerateSpan("V2 EventsQuery.GetFeaturedEventData", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "V2 EventsQuery.GetFeaturedEventData"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "V2 EventsQuery.GetFeaturedEventData"))

	fs := fsUtils.GetFirestoreClient()

	var featuredEvent datastruct.Event
	iter := fs.Collection(datastruct.EventsCollectionName).Where("featured", "==", true).Limit(1).Documents(ctx)

	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.ErrorL(labels, "Error V2 EventsQuery.GetFeaturedEventData Getting FeaturedEvent Data from FS: %s", err)
			return nil, err
		}
		err = doc.DataTo(&featuredEvent)
		if err != nil {
			log.ErrorL(labels, "Error V2 EventsQuery.GetFeaturedEventData Mapping FeaturedEvent Data: %s", err)
			return nil, err
		}

		featuredEvent = e.SplitTimeStampsET(ctx, featuredEvent)
	}

	log.EndTimeL(labels, "V2 EventsQuery.GetFeaturedEventData", startTime, nil)
	span.SetStatus(codes.Ok, "V2 EventsQuery.GetFeaturedEventData")
	return &featuredEvent, nil
}

// SplitTimeStampsET Convert the Event time to be the same of time occurs in that time's location
// Takes a (ctx context.Context, event datastruct.Event)
// Returns datastruct.Event
//
// Convert the Event time data to time's location occurs in
// Returns the Event after convert the time.
func (e *eventsQuery) SplitTimeStampsET(ctx context.Context, event datastruct.Event) datastruct.Event {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Error("Error loading location: %s", err)
	}
	event.StartDateTime = event.StartDateTime.In(loc)
	event.EndDateTime = event.EndDateTime.In(loc)
	event.StartDate = civil.DateOf(event.StartDateTime)
	event.StartTime = civil.TimeOf(event.StartDateTime)
	event.EndDate = civil.DateOf(event.EndDateTime)
	event.EndTime = civil.TimeOf(event.EndDateTime)
	return event
}
