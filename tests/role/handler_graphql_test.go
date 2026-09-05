package role_test

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/role"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	db "github.com/MamangRust/microservice-payment-gateway-grpc/service/role/database/schema"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/role/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/role/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/role/service"
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

type RoleGraphQLTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	grpcServer  *grpc.Server
	graphqlH    http.Handler
	roleID      int
}

func (s *RoleGraphQLTestSuite) SetupSuite() {
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
	log, _ := logger.NewLogger("test-role-gql", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test-role-gql")
	cacheStore := cache.NewCacheStore(redisClient, log, cacheMetrics)

	roleService := service.NewService(&service.Deps{
		Repositories: repos,
		Logger:       log,
		Cache:        cacheStore,
	})

	roleHandler := handler.NewHandler(roleService)
	server := grpc.NewServer()
	pb.RegisterRoleCommandServiceServer(server, roleHandler.RoleCommand)
	pb.RegisterRoleQueryServiceServer(server, roleHandler.RoleQuery)
	s.grpcServer = server

	lis, err := net.Listen("tcp", ":0")
	s.Require().NoError(err)
	go func() { _ = server.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)

	conns := &testhelper.ServiceConnections{
		AuthClient: testhelper.CreateDummyConn(),
		RoleClient: conn,
		UserClient: testhelper.CreateDummyConn(),
	}

	resolver := testhelper.NewResolverWithRedis(conns, log, redisClient)
	s.graphqlH = testhelper.NewGraphQLHTTPHandler(resolver)
}

func (s *RoleGraphQLTestSuite) TearDownSuite() {
	s.grpcServer.Stop()
	s.ts.Teardown()
}

func (s *RoleGraphQLTestSuite) doGraphQL(query string, variables map[string]interface{}) map[string]interface{} {
	body := map[string]interface{}{
		"query":     query,
		"variables": variables,
	}
	jsonBody, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.graphqlH.ServeHTTP(rec, req)

	var result map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &result)
	return result
}

func (s *RoleGraphQLTestSuite) Test1_CreateRole() {
	query := `mutation CreateRole($input: CreateRoleInput!) {
		createRole(input: $input) {
			status
			message
			data {
				id
				name
			}
		}
	}`
	vars := map[string]interface{}{
		"input": map[string]interface{}{
			"name": "GraphQL Role",
		},
	}

	result := s.doGraphQL(query, vars)
	data := result["data"].(map[string]interface{})
	createRole := data["createRole"].(map[string]interface{})
	s.Equal("success", createRole["status"])

	roleData := createRole["data"].(map[string]interface{})
	s.Equal("GraphQL Role", roleData["name"])
	s.roleID = int(roleData["id"].(float64))
}

func (s *RoleGraphQLTestSuite) Test2_FindByIdRole() {
	s.Require().NotZero(s.roleID)
	query := `query FindByIdRole($input: FindByIdRoleInput!) {
		findByIdRole(input: $input) {
			status
			message
			data {
				id
				name
			}
		}
	}`
	vars := map[string]interface{}{
		"input": map[string]interface{}{
			"role_id": s.roleID,
		},
	}

	result := s.doGraphQL(query, vars)
	data := result["data"].(map[string]interface{})
	findById := data["findByIdRole"].(map[string]interface{})
	s.Equal("success", findById["status"])

	roleData := findById["data"].(map[string]interface{})
	s.Equal(float64(s.roleID), roleData["id"])
}

func (s *RoleGraphQLTestSuite) Test3_FindAllRole() {
	query := `query FindAllRole($input: FindAllRoleInput!) {
		findAllRole(input: $input) {
			status
			message
			data {
				id
				name
			}
			pagination {
				current_page
				page_size
				total_pages
				total_records
			}
		}
	}`
	vars := map[string]interface{}{
		"input": map[string]interface{}{
			"page":      1,
			"page_size": 10,
		},
	}

	result := s.doGraphQL(query, vars)
	data := result["data"].(map[string]interface{})
	findAll := data["findAllRole"].(map[string]interface{})
	s.Equal("success", findAll["status"])
}

func (s *RoleGraphQLTestSuite) Test4_UpdateRole() {
	s.Require().NotZero(s.roleID)
	query := `mutation UpdateRole($input: UpdateRoleInput!) {
		updateRole(input: $input) {
			status
			message
			data {
				id
				name
			}
		}
	}`
	vars := map[string]interface{}{
		"input": map[string]interface{}{
			"id":   s.roleID,
			"name": "Updated GraphQL Role",
		},
	}

	result := s.doGraphQL(query, vars)
	data := result["data"].(map[string]interface{})
	updateRole := data["updateRole"].(map[string]interface{})
	s.Equal("success", updateRole["status"])

	roleData := updateRole["data"].(map[string]interface{})
	s.Equal("Updated GraphQL Role", roleData["name"])
}

func (s *RoleGraphQLTestSuite) Test5_TrashAndRestore() {
	s.Require().NotZero(s.roleID)

	trashQuery := `mutation TrashedRole($input: FindByIdRoleInput!) {
		trashedRole(input: $input) { status message }
	}`
	result := s.doGraphQL(trashQuery, map[string]interface{}{
		"input": map[string]interface{}{"role_id": s.roleID},
	})
	s.Equal("success", result["data"].(map[string]interface{})["trashedRole"].(map[string]interface{})["status"])

	restoreQuery := `mutation RestoreRole($input: FindByIdRoleInput!) {
		restoreRole(input: $input) { status message }
	}`
	result = s.doGraphQL(restoreQuery, map[string]interface{}{
		"input": map[string]interface{}{"role_id": s.roleID},
	})
	s.Equal("success", result["data"].(map[string]interface{})["restoreRole"].(map[string]interface{})["status"])
}

func (s *RoleGraphQLTestSuite) Test6_DeleteAndRestoreAll() {
	result := s.doGraphQL(`mutation { restoreAllRole { status message } }`, nil)
	s.Equal("success", result["data"].(map[string]interface{})["restoreAllRole"].(map[string]interface{})["status"])

	result = s.doGraphQL(`mutation { deleteAllRolePermanent { status message } }`, nil)
	s.Equal("success", result["data"].(map[string]interface{})["deleteAllRolePermanent"].(map[string]interface{})["status"])
}

func TestRoleGraphQLSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(RoleGraphQLTestSuite))
}
