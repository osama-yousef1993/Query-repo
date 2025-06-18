package dao

import (
	"strings"
	"time"

	"github.com/Forbes-Media/Systems-golang-common-libraries/fdao"
	"github.com/Forbes-Media/fum"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/bson"
	. "go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type VideoDAO interface {
	fdao.BaseDAO[fum.Content, primitive.ObjectID]
	GetVideosSeries(videoID, EpSortOrder string, season *int) ([]fum.Content, error)
}

var _ fdao.BaseDAO[fum.Content, primitive.ObjectID] = &videoDAOImpl{}

type videoDAOImpl struct {
	*fdao.BaseDAOMongoImpl[fum.Content, primitive.ObjectID]
}

func NewVideoDAO(contentsClient *mongo.Client) VideoDAO {
	coll := contentsClient.Database(DbContents).Collection("Content")
	return &videoDAOImpl{fdao.NewBaseDAOMongoImpl[fum.Content, primitive.ObjectID](coll, 30000*time.Second)}
}

func (v *videoDAOImpl) GetVideosSeries(videoNaturalId, EpSortOrder string, season *int) ([]fum.Content, error) {
	content, err := v.getVideosByNaturalIdIfVisible(videoNaturalId)
	if err != nil {
		return nil, err
	}
	query := M{
		"visible":   true,
		"naturalId": content.NaturalId,
	}
	fdao.AddCriteriaIf(query, M{"video.seriesName": content.Video.SeriesName}, content.Video.SeriesName != nil)
	fdao.AddCriteriaIf(query, M{"video.season": season}, season != nil)

	// // Create the index hint
	hint := bson.D{
		{Key: "video.seriesName", Value: 1},
		{Key: "video.season", Value: 1}, // Fix typo if needed (season vs season?)
		{Key: "video.episode", Value: 1},
	}
	return v.GetAllByQuery(query,
		nil,
		// fdao.QOptSort(strings.ToLower(EpSortOrder) == "desc", "video.season", "-video.season"),
		fdao.QOptSort(lo.Ternary(strings.ToLower(EpSortOrder) == "desc", "video.season", "-video.season")),
		// fdao.QOptSort(lo.Ternary(strings.ToLower(EpSortOrder) == "desc", []string{"video.season", ""}, []string{"-video.season", "-video.episode"})...),
		fdao.QOptHint(hint),
	)
}

func (v *videoDAOImpl) getVideosByNaturalIdIfVisible(videoNaturalId string) (*fum.Content, error) {
	return v.Get(&fum.Content{NaturalId: videoNaturalId, BaseContent: fum.BaseContent{Visible: lo.ToPtr(true)}})
}
