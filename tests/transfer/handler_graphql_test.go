package transfer_test

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	db "github.com/MamangRust/microservice-payment-gateway-grpc/service/transfer/database/schema"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/transfer/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/transfer/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/transfer/service"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/cache"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/observability"
	tests "github.com/MamangRust/microservice-payment-gateway-test"

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/transfer"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type TransferGraphQLTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	grpcServer  *grpc.Server
	graphqlH    http.Handler
	transferID  int
}

func (s *TransferGraphQLTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(ts.RunMigrations("user", "role", "card", "saldo", "transfer"))

	pool, err := pgxpool.New(ts.Ctx, ts.DBURL)
	s.Require().NoError(err)

	redisOpts, err := goredis.ParseURL(ts.RedisURL)
	s.Require().NoError(err)
	redisClient := goredis.NewClient(redisOpts)

	queries := db.New(pool)
	repos := repository.NewRepositories(queries, nil, nil)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test-transfer-gql", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test-transfer-gql")
	cacheStore := cache.NewCacheStore(redisClient, log, cacheMetrics)

	svc := service.NewService(&service.Deps{
		Kafka:            nil,
		Repositories:     repos,
		CardAdapter:      ts.CardAdapter,
		SaldoAdapter:     ts.SaldoAdapter,
		Logger:           log,
		Cache:            cacheStore,
		AISecurityClient: nil,
	})

	h := handler.NewHandler(svc)
	grpcServer := grpc.NewServer()
	pb.RegisterTransferCommandServiceServer(grpcServer, h)
	pb.RegisterTransferQueryServiceServer(grpcServer, h)
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
		TransferClient: conn,
	}
	s.graphqlH = testhelper.NewGraphQLHTTPHandler(testhelper.NewResolverWithRedis(conns, log, redisClient))
}

func (s *TransferGraphQLTestSuite) TearDownSuite() {
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
	s.ts.Teardown()
}

func gqlTransfer(h http.Handler, query string, variables map[string]interface{}) map[string]interface{} {
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

func (s *TransferGraphQLTestSuite) Test1_FindAllTransfer() {
	vars := map[string]interface{}{
		"input": map[string]interface{}{"page": 1, "page_size": 10},
	}
	result := gqlTransfer(s.graphqlH, `query FindAllTransfer($input: FindAllTransferInput!) { findAllTransfer(input: $input) { status message data { id transfer_from transfer_to transfer_amount } pagination { current_page page_size total_pages total_records } } }`, vars)
	s.Equal("success", result["data"].(map[string]interface{})["findAllTransfer"].(map[string]interface{})["status"])
}

func (s *TransferGraphQLTestSuite) Test2_RestoreAllAndDeleteAll() {
	result := gqlTransfer(s.graphqlH, `mutation { restoreAllTransfer { status message } }`, nil)
	s.Equal("success", result["data"].(map[string]interface{})["restoreAllTransfer"].(map[string]interface{})["status"])

	result = gqlTransfer(s.graphqlH, `mutation { deleteAllTransferPermanent { status message } }`, nil)
	s.Equal("success", result["data"].(map[string]interface{})["deleteAllTransferPermanent"].(map[string]interface{})["status"])
}

func TestTransferGraphQLSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(TransferGraphQLTestSuite))
}
