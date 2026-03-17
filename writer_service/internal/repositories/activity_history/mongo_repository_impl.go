package repositories

import (
	"context"

	"github.com/glu/video-real-time-ranking/core/pkg/logger"
	"github.com/glu/video-real-time-ranking/core/pkg/utils"
	"github.com/glu/video-real-time-ranking/writer_service/config"
	"github.com/glu/video-real-time-ranking/writer_service/internal/domain/models"
	"github.com/glu/video-real-time-ranking/writer_service/internal/domain/repositories"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type mongoRepository struct {
	log logger.Logger
	cfg *config.Config
	db  *mongo.Client
}

func NewMongoRepository(log logger.Logger, cfg *config.Config, db *mongo.Client) repositories.IActivityHistoryRepositoryReader {
	return &mongoRepository{log: log, cfg: cfg, db: db}
}

func (p *mongoRepository) CreateActivityHistory(ctx context.Context, activityHistory *models.ActivityHistory) (*models.ActivityHistory, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "mongoRepository.CreateActivityHistory")
	defer span.Finish()

	collection := p.db.Database(p.cfg.Mongo.Db).Collection(p.cfg.MongoCollections.ActivityHistory)

	_, err := collection.InsertOne(ctx, activityHistory, &options.InsertOneOptions{})
	if err != nil {
		p.traceErr(span, err)
		return nil, errors.Wrap(err, "InsertOne")
	}

	return activityHistory, nil
}

func (p *mongoRepository) UpdateActivityHistory(ctx context.Context, activityHistory *models.ActivityHistory) (*models.ActivityHistory, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "mongoRepository.UpdateActivityHistory")
	defer span.Finish()

	collection := p.db.Database(p.cfg.Mongo.Db).Collection(p.cfg.MongoCollections.ActivityHistory)

	ops := options.FindOneAndUpdate()
	ops.SetReturnDocument(options.After)
	ops.SetUpsert(true)

	var updated models.ActivityHistory
	if err := collection.FindOneAndUpdate(ctx, bson.M{"_id": activityHistory.ID}, bson.M{"$set": activityHistory}, ops).Decode(&updated); err != nil {
		p.traceErr(span, err)
		return nil, errors.Wrap(err, "Decode")
	}

	return &updated, nil
}

func (p *mongoRepository) GetActivityHistoryById(ctx context.Context, id uint) (*models.ActivityHistory, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "mongoRepository.GetActivityHistoryById")
	defer span.Finish()

	collection := p.db.Database(p.cfg.Mongo.Db).Collection(p.cfg.MongoCollections.ActivityHistory)

	var activityHistory models.ActivityHistory
	if err := collection.FindOne(ctx, bson.M{"_id": id}).Decode(&activityHistory); err != nil {
		p.traceErr(span, err)
		return nil, errors.Wrap(err, "Decode")
	}

	return &activityHistory, nil
}

func (p *mongoRepository) DeleteActivityHistory(ctx context.Context, id uint) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "mongoRepository.DeleteActivityHistory")
	defer span.Finish()

	collection := p.db.Database(p.cfg.Mongo.Db).Collection(p.cfg.MongoCollections.ActivityHistory)

	return collection.FindOneAndDelete(ctx, bson.M{"_id": id}).Err()
}

func (p *mongoRepository) Search(ctx context.Context, search string, pagination *utils.Pagination) (*models.ActivityHistoryList, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "mongoRepository.Search")
	defer span.Finish()

	collection := p.db.Database(p.cfg.Mongo.Db).Collection(p.cfg.MongoCollections.ActivityHistory)

	filter := bson.D{
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "name", Value: primitive.Regex{Pattern: search, Options: "i"}}},
			bson.D{{Key: "description", Value: primitive.Regex{Pattern: search, Options: "i"}}},
		}},
	}

	count, err := collection.CountDocuments(ctx, filter)
	if err != nil {
		p.traceErr(span, err)
		return nil, errors.Wrap(err, "CountDocuments")
	}
	if count == 0 {
		return &models.ActivityHistoryList{ActivityHistories: make([]*models.ActivityHistory, 0)}, nil
	}

	limit := int64(pagination.GetLimit())
	skip := int64(pagination.GetOffset())
	cursor, err := collection.Find(ctx, filter, &options.FindOptions{
		Limit: &limit,
		Skip:  &skip,
	})
	if err != nil {
		p.traceErr(span, err)
		return nil, errors.Wrap(err, "Find")
	}
	defer cursor.Close(ctx) // nolint: errcheck

	activityHistorys := make([]*models.ActivityHistory, 0, pagination.GetSize())

	for cursor.Next(ctx) {
		var prod models.ActivityHistory
		if err := cursor.Decode(&prod); err != nil {
			p.traceErr(span, err)
			return nil, errors.Wrap(err, "Find")
		}
		activityHistorys = append(activityHistorys, &prod)
	}

	if err := cursor.Err(); err != nil {
		span.SetTag("error", true)
		span.LogKV("error_code", err.Error())
		return nil, errors.Wrap(err, "cursor.Err")
	}

	return models.NewActivityHistoryListWithPagination(activityHistorys, count, pagination), nil
}

func (p *mongoRepository) traceErr(span opentracing.Span, err error) {
	span.SetTag("error", true)
	span.LogKV("error_code", err.Error())
}
