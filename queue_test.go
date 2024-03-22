package mongodbqueue

import (
	"context"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func getTestDBConnection(t *testing.T) *mongo.Database {
	viper.SetDefault("MONGO_URL", "mongodb://localhost:27017")
	viper.SetDefault("MONGO_DB_NAME", "mongodbqueue_tests")
	url := viper.GetString("MONGO_URL")
	db := viper.GetString("MONGO_DB_NAME")
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(url))
	if err != nil {
		t.Fatalf("Error connecting to MongoDB: %v", err)
	}
	return client.Database(db)
}

type TestMessage struct {
	Name      string    `bson:"name"`
	CreatedAt time.Time `bson:"createdAt"`
	Hairs     []string  `bson:"hairs"`
}

func TestMongoDbQueue(t *testing.T) {
	db := getTestDBConnection(t)
	ctx := context.Background()
	defer func() {
		err := db.Client().Disconnect(ctx)
		if err != nil {
			t.Fatalf("Error disconnecting from MongoDB: %v", err)
		}
	}()
	t.Parallel()
	t.Run("Test basics", func(t *testing.T) {
		queue := NewMongoDbQueue[string](db, "test_queue", CreateOptions{
			Visibility: 2,
			Prioritize: false,
			MaxRetries: 2,
		})
		t.Cleanup(func() {
			_, err := queue.Collection().DeleteMany(ctx, bson.D{})
			if err != nil {
				t.Errorf("Error deleting documents: %v", err)
			}
		})
		err := queue.CreateIndexes(ctx)
		if err != nil {
			t.Errorf("Error creating indexes: %v", err)
			return
		}
		msg, err := queue.Get(ctx)
		if err != nil {
			t.Errorf("Error getting message: %v", err)
			return
		}
		assert.Nil(t, msg)

		id, err := queue.Add(ctx, "test message")
		if err != nil {
			t.Errorf("Error adding message: %v", err)
			return
		}
		assert.NotEmpty(t, id)

		time.Sleep(50 * time.Millisecond)
		msg, err = queue.Get(ctx)
		if err != nil {
			t.Errorf("Error getting message: %v", err)
			return
		}
		assert.NotNil(t, msg)
		assert.Equal(t, "test message", msg.Payload)
	})

	t.Run("Test basics", func(t *testing.T) {
		queue := NewMongoDbQueue[TestMessage](db, "test_struct_queue", CreateOptions{
			Visibility: 2,
			Prioritize: false,
			MaxRetries: 2,
		})
		t.Cleanup(func() {
			_, err := queue.Collection().DeleteMany(ctx, bson.D{})
			if err != nil {
				t.Errorf("Error deleting documents: %v", err)
			}
		})

		testMessage := TestMessage{
			Name:      "test message",
			CreatedAt: time.Now(),
			Hairs:     []string{"black", "brown"},
		}
		id, err := queue.Add(ctx, testMessage)
		if err != nil {
			t.Errorf("Error adding message: %v", err)
			return
		}
		assert.NotEmpty(t, id)

		time.Sleep(50 * time.Millisecond)
		msg, err := queue.Get(ctx)
		if err != nil {
			t.Errorf("Error getting message: %v", err)
			return
		}
		assert.NotNil(t, msg)
		assert.Equal(t, testMessage.Name, msg.Payload.Name)
		assert.Equal(t, testMessage.CreatedAt.Year(), msg.Payload.CreatedAt.Year())
		assert.Equal(t, testMessage.Hairs, msg.Payload.Hairs)
		assert.Equal(t, 1, msg.Tries)
		assert.Equal(t, 1, *msg.Occurrences)

		id, err = queue.Ack(ctx, *msg.Ack)
		if err != nil {
			t.Errorf("Error acking message: %v", err)
			return
		}
		assert.NotEmpty(t, id)

		msg, err = queue.Get(ctx)
		if err != nil {
			t.Errorf("Error getting message: %v", err)
			return
		}
		assert.Nil(t, msg)

	})
}
