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
	Id        int       `bson:"id"`
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
			Visibility: 2 * time.Second,
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

	t.Run("Test priority", func(t *testing.T) {
		queue := NewMongoDbQueue[TestMessage](db, "test_priority_queue", CreateOptions{
			Visibility: 250 * time.Millisecond,
			Prioritize: true,
			MaxRetries: 2,
		})
		t.Cleanup(func() {
			_, err := queue.Collection().DeleteMany(ctx, bson.D{})
			if err != nil {
				t.Errorf("Error deleting documents: %v", err)
			}
		})

		_, err := queue.AddWithOptions(ctx, TestMessage{
			Id:   1,
			Name: "eight",
		}, AddOptions{
			Priority: 8,
		})
		if err != nil {
			t.Errorf("Error adding message: %v", err)
			return
		}
		_, err = queue.AddWithOptions(ctx, TestMessage{
			Id:   2,
			Name: "nine",
		}, AddOptions{
			Priority: 9,
		})
		if err != nil {
			t.Errorf("Error adding message: %v", err)
			return
		}
		_, err = queue.AddWithOptions(ctx, TestMessage{
			Id:   3,
			Name: "ten",
		}, AddOptions{
			Priority: 10,
		})
		if err != nil {
			t.Errorf("Error adding message: %v", err)
			return
		}

		msg1, err := queue.Get(ctx)
		if err != nil {
			t.Errorf("Error getting message: %v", err)
			return
		}
		assert.NotNil(t, msg1)
		assert.Equal(t, msg1.Payload.Id, 3)

		msg2, err := queue.Get(ctx)
		if err != nil {
			t.Errorf("Error getting message: %v", err)
			return
		}
		assert.NotNil(t, msg2)
		assert.Equal(t, msg2.Payload.Id, 2)

		msg3, err := queue.Get(ctx)
		if err != nil {
			t.Errorf("Error getting message: %v", err)
			return
		}
		assert.NotNil(t, msg3)
		assert.Equal(t, msg3.Payload.Id, 1)
	})

	t.Run("Test retries", func(t *testing.T) {
		queue := NewMongoDbQueue[TestMessage](db, "test_retry_queue", CreateOptions{
			Visibility: 50 * time.Millisecond,
			Prioritize: true,
			MaxRetries: 2,
		})
		t.Cleanup(func() {
			_, err := queue.Collection().DeleteMany(ctx, bson.D{})
			if err != nil {
				t.Errorf("Error deleting documents: %v", err)
			}
		})

		_, err := queue.AddWithOptions(ctx, TestMessage{
			Id:   1,
			Name: "eight",
		}, AddOptions{
			Priority: 8,
		})
		if err != nil {
			t.Errorf("Error adding message: %v", err)
			return
		}

		msg1, err := queue.Get(ctx)
		if err != nil {
			t.Errorf("Error getting message: %v", err)
			return
		}

		assert.NotNil(t, msg1)

		time.Sleep(60 * time.Millisecond)

		msg2, err := queue.Get(ctx)
		if err != nil {
			t.Errorf("Error getting message: %v", err)
			return
		}

		assert.Nil(t, msg2)

	})

	t.Run("Test delay", func(t *testing.T) {
		queue := NewMongoDbQueue[TestMessage](db, "test_delay_queue", CreateOptions{
			Visibility: 250 * time.Millisecond,
			Prioritize: true,
			MaxRetries: 2,
		})
		t.Cleanup(func() {
			_, err := queue.Collection().DeleteMany(ctx, bson.D{})
			if err != nil {
				t.Errorf("Error deleting documents: %v", err)
			}
		})

		testMessage := TestMessage{
			Id:        1,
			Name:      "test retry",
			CreatedAt: time.Now(),
			Hairs:     []string{"black", "brown"},
		}
		_, err := queue.AddWithOptions(ctx, testMessage, AddOptions{
			Delay:    250 * time.Millisecond,
			Priority: 10,
		})
		if err != nil {
			t.Errorf("Error adding message: %v", err)
			return
		}

		msg, err := queue.Get(ctx)
		if err != nil {
			t.Errorf("Error getting message: %v", err)
			return
		}
		assert.Nil(t, msg)

		time.Sleep(260 * time.Millisecond)

		msg, err = queue.Get(ctx)
		if err != nil {
			t.Errorf("Error getting message: %v", err)
			return
		}
		assert.NotNil(t, msg)

		time.Sleep(60 * time.Millisecond)

		_, err = queue.Ping(ctx, *msg.Ack)
		if err != nil {
			t.Errorf("Error pinging %v", err)
			return
		}

		time.Sleep(200 * time.Millisecond)

		msg, err = queue.Get(ctx)
		if err != nil {
			t.Errorf("Error getting message: %v", err)
			return
		}
		assert.Nil(t, msg)

	})

	t.Run("Test basics struct", func(t *testing.T) {
		queue := NewMongoDbQueue[TestMessage](db, "test_struct_queue", CreateOptions{
			Visibility: 200 * time.Millisecond,
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
			Id:        1,
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

		testMessage.Id = 9
		id3, err := queue.AddWithOptions(ctx, testMessage, AddOptions{
			HashKey:      "id",
			HashKeyValue: testMessage.Id,
		})
		if err != nil {
			t.Errorf("Error adding message: %v", err)
			return
		}
		id4, err := queue.AddWithOptions(ctx, testMessage, AddOptions{
			HashKey:      "id",
			HashKeyValue: testMessage.Id,
		})
		if err != nil {
			t.Errorf("Error adding message: %v", err)
			return
		}
		assert.Equal(t, id3, id4)

		total, err := queue.Total(ctx)
		if err != nil {
			t.Errorf("Error getting total: %v", err)
			return
		}
		assert.Equal(t, int64(2), total)

		size, err := queue.Size(ctx)
		if err != nil {
			t.Errorf("Error getting size: %v", err)
			return
		}
		assert.Equal(t, int64(1), size)

		inFlight, err := queue.InFlight(ctx)
		if err != nil {
			t.Errorf("Error getting in flight: %v", err)
			return
		}
		assert.Equal(t, int64(1), inFlight)

		done, err := queue.Done(ctx)
		if err != nil {
			t.Errorf("Error getting done: %v", err)
			return
		}
		assert.Equal(t, int64(2), done)
	})
}
