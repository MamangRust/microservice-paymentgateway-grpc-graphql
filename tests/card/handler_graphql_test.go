package card_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/card"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	db "github.com/MamangRust/microservice-payment-gateway-grpc/service/card/database/schema"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/card/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/card/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/card/service"
	userdb "github.com/MamangRust/microservice-payment-gateway-grpc/service/user/database/schema"
	user_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/user/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/cache"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/domain/requests"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/observability"
	tests "github.com/MamangRust/microservice-payment-gateway-test"

	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type CardGraphQLTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	grpcServer  *grpc.Server
	graphqlH    http.Handler
	cardID      int
	cardNumber  string
	userID      int
}

func (s *CardGraphQLTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(ts.RunMigrations("user", "role", "card"))

	pool, err := pgxpool.New(ts.Ctx, ts.DBURL)
	s.Require().NoError(err)

	redisOpts, err := goredis.ParseURL(ts.RedisURL)
	s.Require().NoError(err)
	redisClient := goredis.NewClient(redisOpts)

	queries := db.New(pool)
	repos := repository.NewRepositories(queries, ts.UserQueryClient)

	// Seed a user so createCard's user validation passes.
	userdbQueries := userdb.New(pool)
	userRepo := user_repo.NewUserCommandRepository(userdbQueries)
	user, err := userRepo.CreateUser(context.Background(), &requests.CreateUserRequest{
		FirstName: "GQL", LastName: "Card", Email: "gql.card@example.com", Password: "password123",
	})
	s.Require().NoError(err)
	s.userID = int(user.UserID)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test-card-gql", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test-card-gql")
	cacheStore := cache.NewCacheStore(redisClient, log, cacheMetrics)

	svc := service.NewService(&service.Deps{
		Repositories: repos,
		UserAdapter:  ts.UserAdapter,
		Logger:       log,
		Cache:        cacheStore,
		Kafka:        nil,
		BillingCycleDay: 1,
	})

	h := handler.NewHandler(svc)
	grpcServer := grpc.NewServer()
	pb.RegisterCardCommandServiceServer(grpcServer, h)
	pb.RegisterCardQueryServiceServer(grpcServer, h)
	s.grpcServer = grpcServer

	lis, err := net.Listen("tcp", ":0")
	s.Require().NoError(err)
	go func() { _ = grpcServer.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)

	conns := &testhelper.ServiceConnections{
		AuthClient:     testhelper.CreateDummyConn(),
		RoleClient:     testhelper.CreateDummyConn(),
		UserClient:     testhelper.CreateDummyConn(),
		CardClient:     conn,
		MerchantClient: testhelper.CreateDummyConn(),
	}
	s.graphqlH = testhelper.NewGraphQLHTTPHandler(testhelper.NewResolverWithRedis(conns, log, redisClient))
}

func (s *CardGraphQLTestSuite) TearDownSuite() {
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
	s.ts.Teardown()
}

func gqlCard(h http.Handler, query string, variables map[string]interface{}) map[string]interface{} {
	body := map[string]interface{}{"query": query, "variables": variables}
	jsonBody, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var result map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &result)
	return result
}

func (s *CardGraphQLTestSuite) Test1_CreateCard() {
	vars := map[string]interface{}{
		"input": map[string]interface{}{
			"user_id":       s.userID,
			"card_type":     "debit",
			"expire_date":   time.Now().AddDate(1, 0, 0).Format("2006-01-02"),
			"cvv":           "123",
			"card_provider": "visa",
		},
	}
	result := gqlCard(s.graphqlH, `mutation CreateCard($input: CreateCardInput!) { createCard(input: $input) { status message data { id card_number card_type } } }`, vars)
	s.Equal("success", result["data"].(map[string]interface{})["createCard"].(map[string]interface{})["status"])
}

func (s *CardGraphQLTestSuite) Test2_FindAllCard() {
	vars := map[string]interface{}{
		"input": map[string]interface{}{"page": 1, "page_size": 10},
	}
	result := gqlCard(s.graphqlH, `query FindAllCard($input: FindAllCardInput!) { findAllCard(input: $input) { status message data { id card_number card_type } pagination { current_page page_size total_pages total_records } } }`, vars)
	s.Equal("success", result["data"].(map[string]interface{})["findAllCard"].(map[string]interface{})["status"])
}

func (s *CardGraphQLTestSuite) Test3_RestoreAllAndDeleteAll() {
	result := gqlCard(s.graphqlH, `mutation { restoreAllCard { status message } }`, nil)
	s.Equal("success", result["data"].(map[string]interface{})["restoreAllCard"].(map[string]interface{})["status"])

	result = gqlCard(s.graphqlH, `mutation { deleteAllCardPermanent { status message } }`, nil)
	s.Equal("success", result["data"].(map[string]interface{})["deleteAllCardPermanent"].(map[string]interface{})["status"])
}

func TestCardGraphQLSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(CardGraphQLTestSuite))
}
