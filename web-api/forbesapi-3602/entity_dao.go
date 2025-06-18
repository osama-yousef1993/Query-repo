package dao

import (
	"time"

	"github.com/Forbes-Media/Systems-golang-common-libraries/fdao"
	"github.com/Forbes-Media/fum"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type EntityDAO interface {
	fdao.BaseDAO[fum.Content, primitive.ObjectID]
}

var _ fdao.BaseDAO[fum.Content, primitive.ObjectID] = &entityDAOImpl{}

type entityDAOImpl struct {
	*fdao.BaseDAOMongoImpl[fum.Content, primitive.ObjectID]
}

func NewEntityDAO(contentsClient *mongo.Client) EntityDAO {
	coll := contentsClient.Database(DbContents).Collection("Entity")
	return &entityDAOImpl{fdao.NewBaseDAOMongoImpl[fum.Content, primitive.ObjectID](coll, 300*time.Second)}
}
