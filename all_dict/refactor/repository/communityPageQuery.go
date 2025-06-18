package repository

import (
	"context"
	"fmt"
	"os"
	"refactor/datastruct"

	"cloud.google.com/go/firestore"
)

type CommunityPageQuery interface {
	GetCommunityPageAnnouncements(context.Context) (*datastruct.Announcements, error)
}

type CommunityPageQuery struct{}

func (c *CommunityPageQuery) GetCommunityPageAnnouncements(ctx0 context.Context) (*datastruct.Announcements, error) {
	fs := GetFirestoreClient()
	ctx, span := tracer.Start(ctx0, "GetCommunityPageAnnouncements")

	defer span.End()
	span.AddEvent("Get Community Page Announcements from FS")

	collectionName := fmt.Sprintf("%s%s", os.Getenv("ROWY_PREFIX"), "announcements")

	db := fs.Collection(collectionName).Documents(ctx)

	var announcements datastruct.Announcements

	for {
		var announcementsDetails []datastruct.AnnouncementsDetails
		doc, err := db.Next()

		if err == iterator.Done {
			break
		}
		if err := doc.DataTo(&announcements); err != nil {
			log.Error("Error Community Page Announcements Data from FS: %s", err)
			span.AddEvent(fmt.Sprintf("Error Community Page Announcements Data from FS: %s", err))
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, err
		}
		// Get Announcements Details data from FS Collection
		subCollection := fs.Collection(collectionName).Doc("announcement").Collection("lists").OrderBy("publishedDate", firestore.Desc).Documents(ctx)

		for {
			var announcementsDetail datastruct.AnnouncementsDetails
			do, err := subCollection.Next()

			if err == iterator.Done {
				break
			}

			if err := do.DataTo(&announcementsDetail); err != nil {
				log.Error("Error Community Page Announcements Data from FS: %s", err)
				span.AddEvent(fmt.Sprintf("Error Community Page Announcements Data from FS: %s", err))
				span.SetStatus(otelCodes.Error, err.Error())
				return nil, err
			}
			announcementsDetails = append(announcementsDetails, announcementsDetail)
		}
		announcements.AnnouncementsDetails = announcementsDetails
	}
	return announcements, nil
}
