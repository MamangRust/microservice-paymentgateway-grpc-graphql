package withdraw_test

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	db "github.com/MamangRust/microservice-payment-gateway-grpc/service/withdraw/database/schema"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/withdraw/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/withdraw/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/withdraw/service"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/cache"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/observability"
	tests "github.com/MamangRust/microservice-payment-gateway-test"

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/withdraw"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type WithdrawGraphQLTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	grpcServer  *grpc.Server
	graphqlH    http.Handler
	withdrawID  int
}

func (s *WithdrawGraphQLTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(ts.RunMigrations("user", "role", "card", "saldo", "withdraw"))

	pool, err := pgxpool.New(ts.Ctx, ts.DBURL)
	s.Require().NoError(err)

	redisOpts, err := goredis.ParseURL(ts.RedisURL)
	s.Require().NoError(err)
	redisClient := goredis.NewClient(redisOpts)

	queries := db.New(pool)
	repos := repository.NewRepositories(queries, nil, nil)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test-withdraw-gql", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test-withdraw-gql")
	cacheStore := cache.NewCacheStore(redisClient, log, cacheMetrics)

	svc := service.NewService(&service.Deps{
		Kafka:        nil,
		Repositories: repos,
		CardAdapter:  ts.CardAdapter,
		SaldoAdapter: ts.SaldoAdapter,
		Logger:       log,
		Cache:        cacheStore,
		AISecurityClient: nil,
		DailyWithdrawalLimit: 10000000,
	})

	h := handler.NewHandler(svc)
	grpcServer := grpc.NewServer()
	pb.RegisterWithdrawCommandServiceServer(grpcServer, h)
	pb.RegisterWithdrawQueryServiceServer(grpcServer, h)
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
		WithdrawClient: conn,
	}
	s.graphqlH = testhelper.NewGraphQLHTTPHandler(testhelper.NewResolverWithRedis(conns, log, redisClient))
}

func (s *WithdrawGraphQLTestSuite) TearDownSuite() {
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
	s.ts.Teardown()
}

func gqlWithdraw(h http.Handler, query string, variables map[string]interface{}) map[string]interface{} {
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

func (s *WithdrawGraphQLTestSuite) Test1_FindAllWithdraw() {
	vars := map[string]interface{}{
		"input": map[string]interface{}{"page": 1, "page_size": 10},
	}
	result := gqlWithdraw(s.graphqlH, `query FindAllWithdraw($input: FindAllWithdrawInput!) { findAllWithdraw(input: $input) { status message data { withdraw_id card_number withdraw_amount } pagination { current_page page_size total_pages total_records } } }`, vars)
	s.Equal("success", result["data"].(map[string]interface{})["findAllWithdraw"].(map[string]interface{})["status"])
}

func (s *WithdrawGraphQLTestSuite) Test2_RestoreAllAndDeleteAll() {
	result := gqlWithdraw(s.graphqlH, `mutation { restoreAllWithdraw { status message } }`, nil)
	s.Equal("success", result["data"].(map[string]interface{})["restoreAllWithdraw"].(map[string]interface{})["status"])

	result = gqlWithdraw(s.graphqlH, `mutation { deleteAllWithdrawPermanent { status message } }`, nil)
	s.Equal("success", result["data"].(map[string]interface{})["deleteAllWithdrawPermanent"].(map[string]interface{})["status"])
}

func TestWithdrawGraphQLSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(WithdrawGraphQLTestSuite))
}
