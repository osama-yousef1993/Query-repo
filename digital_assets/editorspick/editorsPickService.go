package services

import (
	"context"

	"github.com/Forbes-Media/forbes-digital-assets/refactored/datastruct"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/repository"
	"github.com/Forbes-Media/go-tools/log"
)

type EditorsPickService interface {
	GetEditorsPick(ctx context.Context) (*datastruct.EditorsPick, error) // get EditorsPick Data from FS
	BuildEditorsPick(ctx context.Context) error                          // Build EditorsPick Data from ForbesAPI or BQ
}

type editorsPickService struct {
	dao repository.DAO
}

func NewEditorsPickService(dao repository.DAO) EditorsPickService {
	return &editorsPickService{dao: dao}
}

// GetEditorsPick Attempts to Get EditorsPick information
// Takes a context
// Returns (*datastruct.EditorsPick, error)
//
// Takes the context and get the EditorsPick data
// Returns a *datastruct.EditorsPick with all of the EditorsPick info
func (c *editorsPickService) GetEditorsPick(ctx context.Context) (*datastruct.EditorsPick, error) {

	editorsPick, err := c.dao.NewEditorsPickQuery().GetEditorsPick(ctx)
	if err != nil {
		log.Error("%s", err)
		return nil, err
	}
	return editorsPick, nil
}

// BuildEditorsPick Attempts to Get EditorsPick information from ForbesAPI
// Takes a context
// Returns error
//
// Takes the context and build the EditorsPick data
// Returns no nil if successful or error if failed
func (c *editorsPickService) BuildEditorsPick(ctx context.Context) error {

	queryMGR := c.dao.NewEditorsPickQuery()

	editorsPicksData, err := queryMGR.BuildEditorsPicksData(ctx)
	if err != nil {
		log.Error("%s", err)
		return err
	}
	if editorsPicksData.PromotedContent.ContentPositions == nil || len(editorsPicksData.PromotedContent.ContentPositions) == 0 {
		err := c.MapEditorsPick(ctx)
		if err != nil {
			log.Error("%s", err)
			return err
		}
	}

	return nil
}

// MapEditorsPick Attempts to Build EditorsPick information from BQ
// Takes a context
// Returns  error
//
// Takes the context and Build the EditorsPick data
// Returns no nil if successful or error if failed
func (c *editorsPickService) MapEditorsPick(ctx context.Context) error {

	queryMGR := c.dao.NewEditorsPickQuery()
	var editorsPicksData *datastruct.EditorsPick
	feed, err := queryMGR.FetchEditorsPicks(ctx)
	if err != nil {
		log.Error("%s", err)
		return err
	}

	for i, v := range feed.NewsFeedItems {

		authorGroup, err := queryMGR.GetAuthorGroup(ctx, v.PrimaryAuthor, v.Publication)
		if err != nil {
			log.Error("%s", err)
		}

		item := datastruct.ContentPositions{
			Position:    i + 1,
			Type:        "",
			Title:       v.Title,
			Image:       v.Image,
			Description: "",
			URI:         v.URI,
			ID:          "",
			Authors: []datastruct.Authors{
				{
					NaturalID:  "",
					Name:       v.Author.Name,
					Avatars:    []datastruct.Avatars{},
					URL:        "",
					Type:       v.Author.Type,
					ProfileURL: "",
					AuthorType: v.Author.AuthorType,
					Blog:       false,
					BlogName:   "",
				},
			},
			Date:                v.Date.Unix(),
			BlogType:            "",
			NaturalID:           "",
			BertieBadges:        []interface{}{},
			Magazine:            datastruct.Magazine{},
			HideDescription:     false,
			FullImage:           false,
			Sponsored:           false,
			RemoveTopPadding:    false,
			RemoveBottomPadding: false,
			AuthorGroup:         authorGroup,
			BlogName:            "",
		}
		editorsPicksData.PromotedContent.ContentPositions = append(editorsPicksData.PromotedContent.ContentPositions, item)
	}

	log.Info("Fetched editors picks from feed - Count %d", len(editorsPicksData.PromotedContent.ContentPositions))

	err = queryMGR.SaveEditorsPicks(ctx, editorsPicksData)
	if err != nil {
		log.Error("%s", err)
		return err
	}

	return nil
}
