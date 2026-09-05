package merchant_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	db "github.com/MamangRust/microservice-payment-gateway-grpc/service/merchant/database/schema"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/merchant/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/merchant/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/merchant/service"
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

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/merchant"
)

type MerchantGraphQLTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	grpcServer  *grpc.Server
	graphqlH    http.Handler
	merchantID  int
	userID      int
}

func (s *MerchantGraphQLTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(ts.RunMigrations("user", "role", "merchant"))

	pool, err := pgxpool.New(ts.Ctx, ts.DBURL)
	s.Require().NoError(err)

	redisOpts, err := goredis.ParseURL(ts.RedisURL)
	s.Require().NoError(err)
	redisClient := goredis.NewClient(redisOpts)

	queries := db.New(pool)
	repos := repository.NewRepositories(queries, ts.UserQueryClient)

	// Seed a user so createMerchant's user validation passes.
	userdbQueries := userdb.New(pool)
	userRepo := user_repo.NewUserCommandRepository(userdbQueries)
	user, err := userRepo.CreateUser(context.Background(), &requests.CreateUserRequest{
		FirstName: "GQL", LastName: "Merchant", Email: "gql.merchant@example.com", Password: "password123",
	})
	s.Require().NoError(err)
	s.userID = int(user.UserID)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test-merchant-gql", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test-merchant-gql")
	cacheStore := cache.NewCacheStore(redisClient, log, cacheMetrics)

	svc := service.NewService(&service.Deps{
		Kafka:        nil,
		Repositories: repos,
		UserAdapter:  ts.UserAdapter,
		Logger:       log,
		Cache:        cacheStore,
	})

	h := handler.NewHandler(svc)
	grpcServer := grpc.NewServer()
	pb.RegisterMerchantCommandServiceServer(grpcServer, h)
	pb.RegisterMerchantQueryServiceServer(grpcServer, h)
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
		CardClient:     testhelper.CreateDummyConn(),
		MerchantClient: conn,
	}
	s.graphqlH = testhelper.NewGraphQLHTTPHandler(testhelper.NewResolverWithRedis(conns, log, redisClient))
}

func (s *MerchantGraphQLTestSuite) TearDownSuite() {
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
	s.ts.Teardown()
}

func gqlMerchant(h http.Handler, query string, variables map[string]interface{}) map[string]interface{} {
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

func (s *MerchantGraphQLTestSuite) Test1_CreateMerchant() {
	vars := map[string]interface{}{
		"input": map[string]interface{}{
			"name":   "GraphQL Merchant",
			"user_id": s.userID,
		},
	}
	result := gqlMerchant(s.graphqlH, `mutation CreateMerchant($input: CreateMerchantInput!) { createMerchant(input: $input) { status message data { id name api_key status } } }`, vars)
	s.Equal("success", result["data"].(map[string]interface{})["createMerchant"].(map[string]interface{})["status"])
}

func (s *MerchantGraphQLTestSuite) Test2_FindAllMerchant() {
	vars := map[string]interface{}{
		"input": map[string]interface{}{"page": 1, "page_size": 10},
	}
	result := gqlMerchant(s.graphqlH, `query FindAllMerchant($input: FindAllMerchantInput!) { findAllMerchant(input: $input) { status message data { id name api_key status } pagination { current_page page_size total_pages total_records } } }`, vars)
	s.Equal("success", result["data"].(map[string]interface{})["findAllMerchant"].(map[string]interface{})["status"])
}

func (s *MerchantGraphQLTestSuite) Test3_RestoreAllAndDeleteAll() {
	result := gqlMerchant(s.graphqlH, `mutation { restoreAllMerchant { status message } }`, nil)
	s.Equal("success", result["data"].(map[string]interface{})["restoreAllMerchant"].(map[string]interface{})["status"])

	result = gqlMerchant(s.graphqlH, `mutation { deleteAllMerchantPermanent { status message } }`, nil)
	s.Equal("success", result["data"].(map[string]interface{})["deleteAllMerchantPermanent"].(map[string]interface{})["status"])
}

func TestMerchantGraphQLSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(MerchantGraphQLTestSuite))
}
