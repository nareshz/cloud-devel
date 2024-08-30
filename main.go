package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"cloud.google.com/go/spanner"
	mexporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric"
	texporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"

	// "golang.org/x/time/rate"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// Player is a struct that represents a player in a game.
type Player struct {
	FirstName string `spanner:"first_name"`
	LastName  string `spanner:"last_name"`
	UUID      string `spanner:"uuid"`
	Email     string `spanner:"email"`
}

func newResource() (*resource.Resource, error) {
	return resource.Merge(resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName("LongRunningApp"),
			semconv.ServiceVersion("0.1.0"),
		))
}

func performCreateOperation(ctx context.Context, client *spanner.Client, tracer trace.Tracer, playerId int64, players ...*Player) error {
	// Create a new trace for each create request.
	ctx, span := tracer.Start(ctx, "us-longrunningapp:create-player-go")
	defer span.End()

	if span.SpanContext().IsSampled() {
		fmt.Printf("trace_id for create request of playerId{%d}: %s\n", playerId, span.SpanContext().TraceID())
	}
	var ml []*spanner.Mutation
	for _, player := range players {
		m, err := spanner.InsertStruct("Players", player)
		if err != nil {
			return err
		}
		ml = append(ml, m)
	}
	span.AddEvent("Starting create operation...", trace.WithTimestamp(time.Now()))
	_, err := client.Apply(ctx, ml)
	span.AddEvent("Finished create operation...", trace.WithTimestamp(time.Now()))
	return err
}

func performReadOperation(ctx context.Context, client *spanner.Client, tracer trace.Tracer, playerId int64, playerEmail string) error {
	// Create a new trace for each read request.
	ctx, span := tracer.Start(ctx, "us-longrunningapp:read-player-go")
	defer span.End()

	if span.SpanContext().IsSampled() {
		fmt.Printf("trace_id for read request of playerId{%d}: %s\n", playerId, span.SpanContext().TraceID())
	}
	iter := client.Single().Read(ctx, "Players", spanner.Key{playerEmail}, []string{"first_name", "last_name", "email"})
	defer iter.Stop()
	span.AddEvent("Starting read operation...", trace.WithTimestamp(time.Now()))
	for {
		row, err := iter.Next()
		if err == iterator.Done || err != nil {
			break
		}
		var firstName, lastName, email string
		if err := row.Columns(&firstName, &lastName, &email); err != nil {
			return err
		}
		fmt.Printf("Read playerId{%d}: %s %s %s\n", playerId, firstName, lastName, email)
	}
	span.AddEvent("Finished read operation...", trace.WithTimestamp(time.Now()))
	return nil
}

func performQueryOperation(ctx context.Context, client *spanner.Client, tracer trace.Tracer, playerId int64, playerEmail string) error {
	// Create a new trace for each read request.
	ctx, span := tracer.Start(ctx, "us-longrunningapp:query-player-go")
	defer span.End()

	if span.SpanContext().IsSampled() {
		fmt.Printf("trace_id for query request of playerId{%d}: %s\n", playerId, span.SpanContext().TraceID())
	}
	query := fmt.Sprintf("SELECT * FROM Players WHERE email = '%s' LIMIT 1", playerEmail)
	iter := client.Single().Query(ctx, spanner.NewStatement(query))
	defer iter.Stop()
	span.AddEvent("Starting query operation...", trace.WithTimestamp(time.Now()))
	for {
		row, err := iter.Next()
		if err == iterator.Done || err != nil {
			break
		}
		var firstName, lastName, email string
		if err := row.ColumnByName("first_name", &firstName); err != nil {
			return err
		}
		if err := row.ColumnByName("last_name", &lastName); err != nil {
			return err
		}
		if err := row.ColumnByName("email", &email); err != nil {
			return err
		}

		fmt.Printf("Query playerId{%d}: %s %s %s\n", playerId, firstName, lastName, email)
	}
	span.AddEvent("Finished query operation...", trace.WithTimestamp(time.Now()))
	return nil
}

func performPlayerTableOperations(ctx context.Context, client *spanner.Client, tracer trace.Tracer) {
	playerId := rand.Int63()
	playerEmail := fmt.Sprintf("random-%d-%d@google.com", playerId, time.Now().Unix())
	players := []*Player{
		{FirstName: "Random", LastName: "Player", Email: playerEmail, UUID: "f1578551-eb4b-4ecd-aee2-9f97c37e164e"},
	}
	// Perform Write, Read and Query operation.
	if err := performCreateOperation(ctx, client, tracer, playerId, players...); err != nil {
		log.Printf("Creating newPlayers err: %v", err)
	}
	if err := performReadOperation(ctx, client, tracer, playerId, playerEmail); err != nil {
		log.Printf("Reading newPlayers err: %v", err)
	}
	if err := performQueryOperation(ctx, client, tracer, playerId, playerEmail); err != nil {
		log.Printf("Querying newPlayers err: %v", err)
	}
}

func main() {
	ctx := context.Background()
	res, _ := newResource()

	const rateLimit = time.Second / 100 // 100 calls per second
	throttle := time.Tick(rateLimit)

	projectId := "span-cloud-testing"
	instanceId := "nareshz-condor-test"
	databaseId := "test-db"

	// Set environment variable GOOGLE_API_GO_EXPERIMENTAL_TELEMETRY_PLATFORM_TRACING to "opentelemetry"
	os.Setenv("GOOGLE_API_GO_EXPERIMENTAL_TELEMETRY_PLATFORM_TRACING", "opentelemetry")

	traceClientOptions := []option.ClientOption{
		option.WithEndpoint("staging-cloudtrace.sandbox.googleapis.com:443"),
		option.WithQuotaProject(projectId),
	}
	// Setup trace exporter
	traceExporter, err := texporter.New(
		texporter.WithProjectID(projectId),
		texporter.WithTraceClientOptions(traceClientOptions))
	if err != nil {
		log.Fatalf("unable to set up tracing: %v", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExporter),
		// sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.1))),
	)
	defer tp.Shutdown(ctx)
	otel.SetTracerProvider(tp)
	// Setup trace context propagation. Register the TraceContext propagator globally.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// Setup metrics exporter.
	metricExporter, err := mexporter.New(
		mexporter.WithProjectID(projectId))
	if err != nil {
		log.Fatalf("unable to set up metrics: %v", err)
	}
	mp := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(metricExporter)),
	)

	clientOptions := []option.ClientOption{
		option.WithEndpoint("staging-wrenchworks.sandbox.googleapis.com:443"),
		option.WithQuotaProject(projectId),
	}

	databaseUri := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId)

	client, _ := spanner.NewClientWithConfig(ctx, databaseUri, spanner.ClientConfig{
		SessionPoolConfig: spanner.DefaultSessionPoolConfig,
		// Set meter provider locally
		OpenTelemetryMeterProvider: mp,
		EnableServerSideTracing:    true,
	}, clientOptions...)
	defer client.Close()

	tracer := otel.Tracer("longrunningapp.com/player-ops-tracer")
	// Create players with 100QPS.
	// limiter := rate.NewLimiter(100, 1)
	for {
		// err := limiter.Wait(ctx)
		// if err != nil {
		// 	log.Printf("Rate limiter wait error: %v", err)
		// 	break // or handle the error appropriately
		// }
		<-throttle
		go performPlayerTableOperations(ctx, client, tracer)
	}
}
