package mongodbqueue

import (
	"context"
	"crypto/rand"
	"fmt"
	"reflect"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MessageSchema[T any] struct {
	ID          string     `bson:"_id"`
	CreatedAt   time.Time  `bson:"createdAt"`
	UpdatedAt   *time.Time `bson:"updatedAt"`
	Visible     time.Time  `bson:"visible"`
	Payload     T          `bson:"payload"`
	Ack         *string    `bson:"ack"`
	Tries       int        `bson:"tries"`
	Occurrences *int       `bson:"occurrences"`
	Priority    *int       `bson:"priority"`
}

type AddOptions struct {
	HashKey  string `json:"hashKey"`
	Delay    *int   `json:"delay"`
	Priority *int   `json:"priority"`
}

type CreateOptions struct {
	Visibility int  `json:"visibility"`
	Prioritize bool `json:"prioritize"`
	MaxRetries int  `json:"maxRetries"`
}

type MongoDbQueue[T any] interface {
	Add(ctx context.Context, payload T) (string, error)
	AddWithOptions(ctx context.Context, payload T, options AddOptions) (string, error)
	Ack(ctx context.Context, id string) (string, error)
	Get(ctx context.Context) (*MessageSchema[T], error)
	GetWithVisibility(ctx context.Context, visibility int) (*MessageSchema[T], error)
	Ping(ctx context.Context, ack string) (string, error)
	Total(ctx context.Context) (int64, error)
	Size(ctx context.Context) (int64, error)
	InFlight(ctx context.Context) (int64, error)
	Done(ctx context.Context) (int64, error)
	QueueName() string
	Collection() *mongo.Collection
	CreateIndexes(ctx context.Context) error
}

type MongoDbQueueImpl[T any] struct {
	db         *mongo.Database
	name       string
	visibility int
	prioritize bool
	maxRetries int
}

func NewMongoDbQueue[T any](db *mongo.Database, queueName string, options CreateOptions) MongoDbQueue[T] {
	if options.MaxRetries == 0 {
		options.MaxRetries = 1
	}
	if options.Visibility == 0 {
		options.Visibility = 30
	}
	return &MongoDbQueueImpl[T]{
		db:         db,
		name:       queueName,
		visibility: options.Visibility,
		prioritize: options.Prioritize,
		maxRetries: options.MaxRetries,
	}
}

func id() string {
	buf := make([]byte, 16)
	_, err := rand.Read(buf)
	if err != nil {
		return primitive.NewObjectID().Hex()
	}
	return fmt.Sprintf("%x", buf)
}

// CreateIndexes implements MongoDbQueue.
func (m *MongoDbQueueImpl[T]) CreateIndexes(ctx context.Context) error {
	_, err := m.Collection().Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{"deleted", 1}, {"visible", 1}, {"priority", -1}},
	})
	if err != nil {
		return err
	}
	True := true
	_, err = m.Collection().Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{"ack", 1}},
		Options: &options.IndexOptions{
			Unique: &True,
			Sparse: &True,
		},
	})
	if err != nil {
		return err
	}
	return nil
}

// Add implements MongoDbQueue.
func (m *MongoDbQueueImpl[T]) Add(ctx context.Context, payload T) (string, error) {
	return m.AddWithOptions(ctx, payload, AddOptions{})
}

func GetFieldByName(obj interface{}, fieldName string) interface{} {
	objValue := reflect.ValueOf(obj).Elem()
	fieldValue := objValue.FieldByName(fieldName)
	return fieldValue.Interface()
}

func IsStruct(obj interface{}) bool {
	return reflect.ValueOf(obj).Kind() == reflect.Struct
}

// Add implements MongoDbQueue.
func (m *MongoDbQueueImpl[T]) AddWithOptions(ctx context.Context, payload T, opts AddOptions) (string, error) {
	now := time.Now()

	visible := now
	if opts.Delay != nil {
		visible = now.Add(time.Second * time.Duration(*opts.Delay))
	}

	insertFields := bson.D{
		{"createdAt", now},
		{"visible", visible},
		{"tries", 0},
		{"payload", payload},
		{"priority", opts.Priority},
	}

	// //
	// if IsStruct(payload) {
	// 	marshalled, err := bson.Marshal(payload)
	// 	if err != nil {
	// 		return "", err
	// 	}
	// 	insertFields = append(insertFields, bson.E{"payload", marshalled})
	// } else {
	// 	insertFields = append(insertFields, bson.E{"payload", payload})
	// }

	if opts.HashKey == "" {
		insertFields = append(insertFields, bson.E{"occurences", 1})
		r, err := m.Collection().InsertOne(ctx, insertFields)
		if err != nil {
			return "", err
		}
		return r.InsertedID.(primitive.ObjectID).Hex(), nil
	}

	filter := bson.D{{"payload", opts.HashKey}}
	if IsStruct(payload) {
		filter = bson.D{{fmt.Sprintf("payload.%s", opts.HashKey), GetFieldByName(payload, opts.HashKey)}}
	}

	result := m.Collection().FindOneAndUpdate(ctx, filter, bson.D{
		{"$setOnInsert", insertFields},
		{"$set", bson.D{{"updatedAt", now}}},
		{"$inc", bson.D{{"occurrences", 1}}},
	}, options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After))
	if result.Err() != nil {
		return "", result.Err()
	}

	msg := &MessageSchema[T]{}
	if err := result.Decode(msg); err != nil {
		return "", err
	}
	return msg.ID, nil
}

// Get implements MongoDbQueue.
func (m *MongoDbQueueImpl[T]) Get(ctx context.Context) (*MessageSchema[T], error) {
	return m.GetWithVisibility(ctx, 0)
}

// GetWithVisibility implements MongoDbQueue.
func (m *MongoDbQueueImpl[T]) GetWithVisibility(ctx context.Context, visibility int) (*MessageSchema[T], error) {
	if visibility == 0 {
		visibility = m.visibility
	}
	now := time.Now()
	filter := bson.D{
		{"visible", bson.D{{"$lte", now}}},
		{"deleted", bson.D{{"$ne", true}}},
	}
	update := bson.D{
		{"$set", bson.D{
			{"visible", now.Add(time.Second * time.Duration(visibility))},
			{"ack", id()},
			{"updatedAt", now},
		}},
		{"$inc", bson.D{{"tries", 1}}},
	}
	sort := bson.D{{"_id", 1}}
	if m.prioritize {
		sort = bson.D{{"priority", -1}, {"_id", 1}}
	}
	result := m.Collection().FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetSort(sort).SetReturnDocument(options.After))
	if result.Err() != nil {
		if result.Err() == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, result.Err()
	}

	msg := &MessageSchema[T]{}
	if err := result.Decode(msg); err != nil {
		return nil, err
	}
	if msg.Ack == nil || msg.UpdatedAt == nil {
		return nil, fmt.Errorf("message missing ack or updated at")
	}

	if m.maxRetries > 0 && msg.Tries >= m.maxRetries {
		_id, err := primitive.ObjectIDFromHex(msg.ID)
		if err != nil {
			return nil, err
		}
		_, err = m.Collection().UpdateOne(ctx, bson.D{{"_id", _id}}, bson.D{{"$set", bson.D{{"deleted", now}, {"dead", true}}}})
		if err != nil {
			return nil, err
		}
		return m.GetWithVisibility(ctx, visibility)
	}

	if msg.Occurrences == nil {
		One := 1
		msg.Occurrences = &One
	}
	return msg, nil
}

// Ping implements MongoDbQueue.
func (m *MongoDbQueueImpl[T]) Ping(ctx context.Context, ack string) (string, error) {
	return m.PingWithVisibility(ctx, ack, 0)
}

// Ping implements MongoDbQueue.
func (m *MongoDbQueueImpl[T]) PingWithVisibility(ctx context.Context, ack string, visibility int) (string, error) {
	if visibility == 0 {
		visibility = m.visibility
	}
	now := time.Now()
	filter := bson.D{
		{"visible", bson.D{{"$gt", now}}},
		{"deleted", bson.D{{"$ne", true}}},
		{"ack", ack},
	}
	update := bson.D{
		{"$set", bson.D{
			{"visible", now.Add(time.Second * time.Duration(visibility))},
		}},
	}
	result := m.Collection().FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetReturnDocument(options.After))
	if result.Err() != nil {
		return "", result.Err()
	}
	msg := &MessageSchema[T]{}
	if err := result.Decode(msg); err != nil {
		return "", err
	}
	return msg.ID, nil
}

// Ack implements MongoDbQueue.
func (m *MongoDbQueueImpl[T]) Ack(ctx context.Context, ack string) (string, error) {
	now := time.Now()
	filter := bson.D{
		{"visible", bson.D{{"$gt", now}}},
		{"deleted", bson.D{{"$ne", true}}},
		{"ack", ack},
	}
	update := bson.D{
		{"$set", bson.D{
			{"deleted", now},
		}},
	}
	result := m.Collection().FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetReturnDocument(options.After))
	if result.Err() != nil {
		return "", result.Err()
	}
	msg := &MessageSchema[T]{}
	if err := result.Decode(msg); err != nil {
		return "", err
	}
	return msg.ID, nil
}

// Total implements MongoDbQueue.
func (m *MongoDbQueueImpl[T]) Total(ctx context.Context) (int64, error) {
	return m.Collection().CountDocuments(ctx, bson.D{})
}

// Size implements MongoDbQueue.
func (m *MongoDbQueueImpl[T]) Size(ctx context.Context) (int64, error) {
	return m.Collection().CountDocuments(ctx, bson.D{
		{"visible", bson.D{{"$lte", time.Now()}}},
		{"deleted", bson.D{{"$ne", true}}},
	})
}

// Done implements MongoDbQueue.
func (m *MongoDbQueueImpl[T]) Done(ctx context.Context) (int64, error) {
	return m.Collection().CountDocuments(ctx, bson.D{
		{"deleted", bson.D{{"$ne", true}}},
	})
}

// InFlight implements MongoDbQueue.
func (m *MongoDbQueueImpl[T]) InFlight(ctx context.Context) (int64, error) {
	return m.Collection().CountDocuments(ctx, bson.D{
		{"visible", bson.D{{"$gt", time.Now()}}},
		{"deleted", bson.D{{"$ne", true}}},
		{"ack", bson.D{{"$exists", true}}},
	})
}

// QueueName implements MongoDbQueue.
func (m *MongoDbQueueImpl[T]) QueueName() string {
	return m.name
}

// Collection implements MongoDbQueue.
func (m *MongoDbQueueImpl[T]) Collection() *mongo.Collection {
	return m.db.Collection(m.name)
}
