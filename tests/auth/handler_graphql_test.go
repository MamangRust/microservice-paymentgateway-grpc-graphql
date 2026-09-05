package auth_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/auth"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/hash"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	db "github.com/MamangRust/microservice-payment-gateway-grpc/service/auth/database/schema"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/auth/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/auth/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/auth/service"
	tests "github.com/MamangRust/microservice-payment-gateway-test"

	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type AuthGraphQLTestSuite struct {
	suite.Suite
	ts       *tests.TestSuite
	graphqlH http.Handler
	email    string
	password string
}

func (s *AuthGraphQLTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(ts.RunMigrations("user", "role", "auth"))

	pool, err := pgxpool.New(ts.Ctx, ts.DBURL)
	s.Require().NoError(err)

	// Seed the ROLE_ADMIN role required by registerUser.
	_, _ = pool.Exec(context.Background(), "INSERT INTO roles (role_name) VALUES ('ROLE_ADMIN')")

	redisOpts, _ := goredis.ParseURL(ts.RedisURL)
	redisClient := goredis.NewClient(redisOpts)

	queries := db.New(pool)
	repos := repository.NewRepositories(&repository.RepositoriesDeps{
		DB:                queries,
		UserQueryClient:   ts.UserQueryClient,
		UserCommandClient: ts.UserCommandClient,
		RoleQueryClient:   ts.RoleQueryClient,
		RoleCommandClient: ts.RoleCommandClient,
	})
	tokenManager, _ := auth.NewManager("test-secret")
	hasher := hash.NewHashingPassword()
	svc := service.NewService(&service.Deps{
		Repositories: repos, Logger: ts.Logger, Cache: ts.CacheStore,
		Token: tokenManager, Hash: hasher, Kafka: nil,
	})
	h := handler.NewAuthHandleGrpc(svc, ts.Logger)
	grpcServer := grpc.NewServer()
	pb.RegisterAuthServiceServer(grpcServer, h)
	lis, _ := net.Listen("tcp", ":0")
	go func() { _ = grpcServer.Serve(lis) }()
	conn, _ := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))

	conns := &testhelper.ServiceConnections{
		AuthClient: conn, RoleClient: testhelper.CreateDummyConn(), UserClient: testhelper.CreateDummyConn(),
	}
	resolver := testhelper.NewResolverWithRedis(conns, ts.Logger, redisClient)
	s.graphqlH = testhelper.NewGraphQLHTTPHandler(resolver)
	s.email = "auth.gql@example.com"
	s.password = "password123"
}

func (s *AuthGraphQLTestSuite) TearDownSuite() { s.ts.Teardown() }

func gql(h http.Handler, query string, vars map[string]interface{}) map[string]interface{} {
	body, _ := json.Marshal(map[string]interface{}{"query": query, "variables": vars})
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var result map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &result)
	return result
}

func (s *AuthGraphQLTestSuite) Test1_Register() {
	result := gql(s.graphqlH, `mutation { registerUser(input: {firstname:"Auth",lastname:"GQL",email:"`+s.email+`",password:"`+s.password+`",confirm_password:"`+s.password+`"}) { status message data { id email } } }`, nil)
	s.Equal("success", result["data"].(map[string]interface{})["registerUser"].(map[string]interface{})["status"])
}

func (s *AuthGraphQLTestSuite) Test2_Login() {
	result := gql(s.graphqlH, `mutation { loginUser(input: {email:"`+s.email+`",password:"`+s.password+`"}) { status message data { access_token } } }`, nil)
	s.Equal("success", result["data"].(map[string]interface{})["loginUser"].(map[string]interface{})["status"])
}

func TestAuthGraphQLSuite(t *testing.T) {
	if testing.Short() { t.Skip("skipping integration test") }
	suite.Run(t, new(AuthGraphQLTestSuite))
}
