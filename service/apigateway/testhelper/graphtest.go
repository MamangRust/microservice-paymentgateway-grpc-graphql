package testhelper

import (
	"context"
	"net/http"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	graph "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/handler"
	mencache "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/redis"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/redis/go-redis/v9"
	"github.com/vektah/gqlparser/v2/ast"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ServiceConnections mirrors graph.ServiceConnections for test use.
type ServiceConnections = graph.ServiceConnections

// CreateDummyConn creates a lazy gRPC connection that will never actually connect.
func CreateDummyConn() *grpc.ClientConn {
	conn, err := grpc.NewClient("localhost:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil
	}
	return conn
}

// NewResolver creates a Resolver with the provided service connections.
func NewResolver(conns *ServiceConnections, log logger.LoggerInterface) *graph.Resolver {
	myMencache := mencache.NewCacheApiGateway(&mencache.Deps{
		Redis:  nil,
		Logger: log,
	})

	return graph.NewResolver(&graph.Deps{
		Clients:  conns,
		Logger:   log,
		Kafka:    nil,
		Mencache: myMencache,
	})
}

// NewResolverWithRedis creates a Resolver with the provided service connections and Redis client.
func NewResolverWithRedis(conns *ServiceConnections, log logger.LoggerInterface, redisClient *redis.Client) *graph.Resolver {
	myMencache := mencache.NewCacheApiGateway(&mencache.Deps{
		Redis:  redisClient,
		Logger: log,
	})

	return graph.NewResolver(&graph.Deps{
		Clients:  conns,
		Logger:   log,
		Kafka:    nil,
		Mencache: myMencache,
	})
}

// NewGraphQLHTTPHandler creates an http.Handler from a gqlgen Resolver.
func NewGraphQLHTTPHandler(resolver *graph.Resolver) http.Handler {
	srv := handler.New(graph.NewExecutableSchema(graph.Config{
		Resolvers: resolver,
	}))

	srv.AddTransport(transport.POST{})
	srv.SetQueryCache(lru.New[*ast.QueryDocument](1000))
	srv.Use(extension.Introspection{})

	return srv
}

// SeedMerchantCache writes a merchant ID-to-API-key mapping into Redis
// so the permission validation can find it without Kafka.
func SeedMerchantCache(redisClient *redis.Client, merchantID string, apiKey string) error {
	key := "merchant_api_key:" + merchantID
	return redisClient.Set(context.Background(), key, apiKey, 0).Err()
}

// SeedRoleCache writes a user role mapping into Redis
// so the permission validation can find it without Kafka.
func SeedRoleCache(redisClient *redis.Client, userID string, roles []string) error {
	key := "user_roles:" + userID
	return redisClient.Set(context.Background(), key, roles, 0).Err()
}
