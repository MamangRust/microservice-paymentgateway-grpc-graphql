package user_test

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/user"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/hash"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	db "github.com/MamangRust/microservice-payment-gateway-grpc/service/user/database/schema"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/user/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/user/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/user/service"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/cache"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/observability"
	tests "github.com/MamangRust/microservice-payment-gateway-test"

	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type UserGraphQLTestSuite struct {
	suite.Suite
	ts       *tests.TestSuite
	grpcServer *grpc.Server
	graphqlH http.Handler
	userID   int
}

func (s *UserGraphQLTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(ts.RunMigrations("user", "role"))

	pool, err := pgxpool.New(ts.Ctx, ts.DBURL)
	s.Require().NoError(err)

	redisOpts, err := goredis.ParseURL(ts.RedisURL)
	s.Require().NoError(err)
	redisClient := goredis.NewClient(redisOpts)

	queries := db.New(pool)
	repos := repository.NewRepositories(queries)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test-user-gql", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test-user-gql")
	cacheStore := cache.NewCacheStore(redisClient, log, cacheMetrics)

	hasher := hash.NewHashingPassword()

	svc := service.NewService(&service.Deps{
		Repositories: repos,
		Hash:         hasher,
		Logger:       log,
		Cache:        cacheStore,
	})

	h := handler.NewHandler(svc)
	grpcServer := grpc.NewServer()
	pb.RegisterUserQueryServiceServer(grpcServer, h)
	pb.RegisterUserCommandServiceServer(grpcServer, h)
	s.grpcServer = grpcServer

	lis, err := net.Listen("tcp", ":0")
	s.Require().NoError(err)
	go func() { _ = grpcServer.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)

	conns := &testhelper.ServiceConnections{
		AuthClient:   testhelper.CreateDummyConn(),
		RoleClient:   testhelper.CreateDummyConn(),
		UserClient:   conn,
		CardClient:   testhelper.CreateDummyConn(),
		MerchantClient: testhelper.CreateDummyConn(),
	}
	s.graphqlH = testhelper.NewGraphQLHTTPHandler(testhelper.NewResolverWithRedis(conns, log, redisClient))
}

func (s *UserGraphQLTestSuite) TearDownSuite() {
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
	s.ts.Teardown()
}

func gqlUser(h http.Handler, query string, vars map[string]interface{}) map[string]interface{} {
	body, _ := json.Marshal(map[string]interface{}{"query": query, "variables": vars})
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var r map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &r)
	return r
}

func (s *UserGraphQLTestSuite) Test1_CreateUser() {
	result := gqlUser(s.graphqlH, `mutation { createUser(input: {firstname:"GQL",lastname:"User",email:"gql.user@test.com",password:"pass123",confirm_password:"pass123"}) { status data { id email } } }`, nil)
	s.Equal("success", result["data"].(map[string]interface{})["createUser"].(map[string]interface{})["status"])
}

func (s *UserGraphQLTestSuite) Test2_FindAllUser() {
	result := gqlUser(s.graphqlH, `query { findAllUser(input: {page:1, page_size:10}) { status data { id email } pagination { current_page page_size total_pages total_records } } }`, nil)
	s.Equal("success", result["data"].(map[string]interface{})["findAllUser"].(map[string]interface{})["status"])
}

func TestUserGraphQLSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(UserGraphQLTestSuite))
}
