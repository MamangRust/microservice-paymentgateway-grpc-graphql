package transaction_test

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	db "github.com/MamangRust/microservice-payment-gateway-grpc/service/transaction/database/schema"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/transaction/handler"
	transaction_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/transaction/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/transaction/service"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/cache"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/observability"
	tests "github.com/MamangRust/microservice-payment-gateway-test"

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/transaction"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type TransactionGraphQLTestSuite struct {
	suite.Suite
	ts            *tests.TestSuite
	grpcServer    *grpc.Server
	graphqlH      http.Handler
	transactionID int
}

func (s *TransactionGraphQLTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(ts.RunMigrations("user", "role", "card", "merchant", "saldo", "transaction"))

	pool, err := pgxpool.New(ts.Ctx, ts.DBURL)
	s.Require().NoError(err)

	redisOpts, err := goredis.ParseURL(ts.RedisURL)
	s.Require().NoError(err)
	redisClient := goredis.NewClient(redisOpts)

	queries := db.New(pool)
	repos := transaction_repo.NewRepositories(queries, nil, nil, nil)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test-transaction-gql", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test-transaction-gql")
	cacheStore := cache.NewCacheStore(redisClient, log, cacheMetrics)

	svc := service.NewService(&service.Deps{
		Kafka:            nil,
		Repositories:     repos,
		MerchantAdapter:  ts.MerchantAdapter,
		CardAdapter:      ts.CardAdapter,
		SaldoAdapter:     ts.SaldoAdapter,
		Logger:           log,
		Cache:            cacheStore,
		AISecurityClient: nil,
	})

	h := handler.NewHandler(svc)
	grpcServer := grpc.NewServer()
	pb.RegisterTransactionCommandServiceServer(grpcServer, h)
	pb.RegisterTransactionQueryServiceServer(grpcServer, h)
	s.grpcServer = grpcServer

	lis, err := net.Listen("tcp", ":0")
	s.Require().NoError(err)
	go func() { _ = grpcServer.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)

	conns := &testhelper.ServiceConnections{
		AuthClient:        testhelper.CreateDummyConn(),
		RoleClient:        testhelper.CreateDummyConn(),
		UserClient:        testhelper.CreateDummyConn(),
		CardClient:        testhelper.CreateDummyConn(),
		MerchantClient:    testhelper.CreateDummyConn(),
		TransactionClient: conn,
	}
	s.graphqlH = testhelper.NewGraphQLHTTPHandler(testhelper.NewResolverWithRedis(conns, log, redisClient))
}

func (s *TransactionGraphQLTestSuite) TearDownSuite() {
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
	s.ts.Teardown()
}

func gqlTransaction(h http.Handler, query string, variables map[string]interface{}) map[string]interface{} {
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

func (s *TransactionGraphQLTestSuite) Test1_FindAllTransaction() {
	vars := map[string]interface{}{
		"input": map[string]interface{}{"page": 1, "page_size": 10},
	}
	result := gqlTransaction(s.graphqlH, `query FindAllTransaction($input: FindAllTransactionInput!) { findAllTransaction(input: $input) { status message data { id card_number transaction_no amount payment_method merchant_id } pagination { current_page page_size total_pages total_records } } }`, vars)
	s.Equal("success", result["data"].(map[string]interface{})["findAllTransaction"].(map[string]interface{})["status"])
}

func (s *TransactionGraphQLTestSuite) Test2_RestoreAllAndDeleteAll() {
	result := gqlTransaction(s.graphqlH, `mutation { restoreAllTransaction { status message } }`, nil)
	s.Equal("success", result["data"].(map[string]interface{})["restoreAllTransaction"].(map[string]interface{})["status"])

	result = gqlTransaction(s.graphqlH, `mutation { deleteAllTransactionPermanent { status message } }`, nil)
	s.Equal("success", result["data"].(map[string]interface{})["deleteAllTransactionPermanent"].(map[string]interface{})["status"])
}

func TestTransactionGraphQLSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(TransactionGraphQLTestSuite))
}
