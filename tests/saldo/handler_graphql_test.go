package saldo_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	carddb "github.com/MamangRust/microservice-payment-gateway-grpc/service/card/database/schema"
	card_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/card/repository"
	db "github.com/MamangRust/microservice-payment-gateway-grpc/service/saldo/database/schema"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/saldo/handler"
	saldo_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/saldo/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/saldo/service"
	userdb "github.com/MamangRust/microservice-payment-gateway-grpc/service/user/database/schema"
	user_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/user/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/cache"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/domain/requests"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/observability"
	tests "github.com/MamangRust/microservice-payment-gateway-test"

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/saldo"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type SaldoGraphQLTestSuite struct {
	suite.Suite
	ts         *tests.TestSuite
	grpcServer *grpc.Server
	graphqlH   http.Handler
	saldoID    int
	cardNumber string
}

func (s *SaldoGraphQLTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(ts.RunMigrations("user", "role", "card", "saldo"))

	pool, err := pgxpool.New(ts.Ctx, ts.DBURL)
	s.Require().NoError(err)

	redisOpts, err := goredis.ParseURL(ts.RedisURL)
	s.Require().NoError(err)
	redisClient := goredis.NewClient(redisOpts)

	queries := db.New(pool)
	repos := saldo_repo.NewRepositories(queries, nil)

	// Seed a user and a card so createSaldo's card validation passes.
	userdbQueries := userdb.New(pool)
	userRepo := user_repo.NewUserCommandRepository(userdbQueries)
	user, err := userRepo.CreateUser(context.Background(), &requests.CreateUserRequest{
		FirstName: "GQL", LastName: "Saldo", Email: "gql.saldo@example.com", Password: "password123",
	})
	s.Require().NoError(err)

	carddbQueries := carddb.New(pool)
	cardRepos := card_repo.NewRepositories(carddbQueries, nil)
	card, err := cardRepos.CardCommand.CreateCard(context.Background(), &requests.CreateCardRequest{
		UserID: int(user.UserID), CardType: "debit", ExpireDate: time.Now().AddDate(1, 0, 0), CVV: "123", CardProvider: "visa",
	})
	s.Require().NoError(err)
	s.cardNumber = card.CardNumber

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test-saldo-gql", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test-saldo-gql")
	cacheStore := cache.NewCacheStore(redisClient, log, cacheMetrics)

	svc := service.NewService(&service.Deps{
		Repositories: repos,
		CardAdapter:  ts.CardAdapter,
		Logger:       log,
		Cache:        cacheStore,
	})

	h := handler.NewHandler(svc)
	grpcServer := grpc.NewServer()
	pb.RegisterSaldoCommandServiceServer(grpcServer, h)
	pb.RegisterSaldoQueryServiceServer(grpcServer, h)
	s.grpcServer = grpcServer

	lis, err := net.Listen("tcp", ":0")
	s.Require().NoError(err)
	go func() { _ = grpcServer.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)

	conns := &testhelper.ServiceConnections{
		AuthClient:   testhelper.CreateDummyConn(),
		RoleClient:   testhelper.CreateDummyConn(),
		UserClient:   testhelper.CreateDummyConn(),
		CardClient:   testhelper.CreateDummyConn(),
		SaldoClient:  conn,
	}
	s.graphqlH = testhelper.NewGraphQLHTTPHandler(testhelper.NewResolverWithRedis(conns, log, redisClient))
}

func (s *SaldoGraphQLTestSuite) TearDownSuite() {
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
	s.ts.Teardown()
}

func gqlSaldo(h http.Handler, query string, variables map[string]interface{}) map[string]interface{} {
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

func (s *SaldoGraphQLTestSuite) Test1_CreateSaldo() {
	vars := map[string]interface{}{
		"input": map[string]interface{}{
			"card_number":   s.cardNumber,
			"total_balance": 1000000,
		},
	}
	result := gqlSaldo(s.graphqlH, `mutation CreateSaldo($input: CreateSaldoInput!) { createSaldo(input: $input) { status message data { saldo_id card_number total_balance } } }`, vars)
	s.Equal("success", result["data"].(map[string]interface{})["createSaldo"].(map[string]interface{})["status"])
}

func (s *SaldoGraphQLTestSuite) Test2_FindAllSaldo() {
	vars := map[string]interface{}{
		"input": map[string]interface{}{"page": 1, "page_size": 10},
	}
	result := gqlSaldo(s.graphqlH, `query FindAllSaldo($input: FindAllSaldoInput!) { findAllSaldo(input: $input) { status message data { saldo_id card_number total_balance } pagination { current_page page_size total_pages total_records } } }`, vars)
	s.Equal("success", result["data"].(map[string]interface{})["findAllSaldo"].(map[string]interface{})["status"])
}

func (s *SaldoGraphQLTestSuite) Test3_RestoreAllAndDeleteAll() {
	result := gqlSaldo(s.graphqlH, `mutation { restoreAllSaldo { status message } }`, nil)
	s.Equal("success", result["data"].(map[string]interface{})["restoreAllSaldo"].(map[string]interface{})["status"])

	result = gqlSaldo(s.graphqlH, `mutation { deleteAllSaldoPermanent { status message } }`, nil)
	s.Equal("success", result["data"].(map[string]interface{})["deleteAllSaldoPermanent"].(map[string]interface{})["status"])
}

func TestSaldoGraphQLSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(SaldoGraphQLTestSuite))
}
