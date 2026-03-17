package server

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/glu/video-real-time-ranking/core/pkg/interceptors"
	kafkaClient "github.com/glu/video-real-time-ranking/core/pkg/kafka"
	"github.com/glu/video-real-time-ranking/core/pkg/logger"
	"github.com/glu/video-real-time-ranking/core/pkg/mysql"
	redisClient "github.com/glu/video-real-time-ranking/core/pkg/redis"
	"github.com/glu/video-real-time-ranking/ent"
	"github.com/glu/video-real-time-ranking/reader_service/config"
	v1 "github.com/glu/video-real-time-ranking/reader_service/internal/delivery/http/v1"
	readerKafka "github.com/glu/video-real-time-ranking/reader_service/internal/delivery/kafka"
	"github.com/glu/video-real-time-ranking/reader_service/internal/domain/usecase"
	"github.com/glu/video-real-time-ranking/reader_service/internal/metrics"
	comment_repo "github.com/glu/video-real-time-ranking/reader_service/internal/repositories/comment"
	object_repo "github.com/glu/video-real-time-ranking/reader_service/internal/repositories/object"
	reaction_repo "github.com/glu/video-real-time-ranking/reader_service/internal/repositories/reaction"
	video_repo "github.com/glu/video-real-time-ranking/reader_service/internal/repositories/video"
	viewer_repo "github.com/glu/video-real-time-ranking/reader_service/internal/repositories/viewer"
	comment_usecase "github.com/glu/video-real-time-ranking/reader_service/internal/usecase/comment"
	object_usecase "github.com/glu/video-real-time-ranking/reader_service/internal/usecase/object"
	reaction_usecase "github.com/glu/video-real-time-ranking/reader_service/internal/usecase/reaction"
	video_usecase "github.com/glu/video-real-time-ranking/reader_service/internal/usecase/video"
	viewer_usecase "github.com/glu/video-real-time-ranking/reader_service/internal/usecase/viewer"

	"github.com/georgecookeIW/fasthttprouter"
	"github.com/go-playground/validator"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/valyala/fasthttp"
	"go.mongodb.org/mongo-driver/mongo"
)

type server struct {
	log             logger.Logger
	cfg             *config.Config
	v               *validator.Validate
	kafkaConn       *kafka.Conn
	im              interceptors.InterceptorManager
	mongoClient     *mongo.Client
	redisClient     redis.UniversalClient
	pgConn          *pgxpool.Pool
	entClient       *ent.Client
	videoUsecase    usecase.IVideoUsecase
	commentUsecase  usecase.ICommentUsecase
	objectUsecase   usecase.IObjectUsecase
	reactionUsecase usecase.IReactionUsecase
	viewerUsecase   usecase.IViewerUsecase
	metrics         *metrics.ReaderServiceMetrics
}

func NewServer(log logger.Logger, cfg *config.Config) *server {
	return &server{log: log, cfg: cfg, v: validator.New()}
}

func (s *server) Run() error {
	// Initialize context and signal notifications
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// Initialize services and connections
	if err := s.initializeServices(ctx); err != nil {
		return err
	}

	// Graceful resource cleanup on shutdown
	defer func() {
		if s.redisClient != nil {
			s.redisClient.Close()
		}
		if s.kafkaConn != nil {
			s.kafkaConn.Close()
		}
		if s.mongoClient != nil {
			s.mongoClient.Disconnect(context.Background())
		}
	}()

	// Start the HTTP server in a goroutine
	go func() {
		if err := s.startHTTPServer(); err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Start the gRPC server in a goroutine
	go func() {
		if err := s.startGRPCServer(); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	// Wait for context to be done
	<-ctx.Done()

	return nil
}

func (s *server) initializeServices(ctx context.Context) error {
	s.im = interceptors.NewInterceptorManager(s.log)
	s.metrics = metrics.NewReaderServiceMetrics(s.cfg)

	// Initialize MongoDB connection
	//mongoDBConn, err := mongodb.NewMongoDBConn(ctx, s.cfg.Mongo)
	//if err != nil {
	//	return errors.Wrap(err, "NewMongoDBConn")
	//}
	//s.mongoClient = mongoDBConn
	//
	//// Initialize PostgreSQL connection
	//pgxConn, err := postgres.NewPgxConn(s.cfg.Postgresql)
	//if err != nil {
	//	return errors.Wrap(err, "postgresql.NewPgxConn")
	//}
	//s.pgConn = pgxConn
	//s.log.Infof("postgres connected: %v", pgxConn.Stat().TotalConns())
	//defer pgxConn.Close()
	//
	//s.log.Infof("Mongo connected: %v", mongoDBConn.NumberSessionsInProgress())

	// Initialize Ent client
	entConn, err := mysql.SetupEntClient(s.cfg.Postgresql)
	if err != nil {
		return errors.Wrap(err, "mysql.SetupEntClient")
	}
	s.entClient = entConn
	s.log.Infof("MySQL connected: %v", entConn)

	//if err := entConn.Schema.Create(context.Background()); err != nil {
	//	return fmt.Errorf("failed creating schema resources: %v", err)
	//}

	// Initialize Redis client
	s.redisClient = redisClient.NewUniversalRedisClient(s.cfg.Redis)

	s.log.Infof("Redis connected: %+v", s.redisClient.PoolStats())

	// Initialize repositories and use cases

	// Video
	videoRepo := video_repo.NewVideoRepository(s.log, s.cfg, s.entClient)
	redisVideoRepo := video_repo.NewRedisVideoRepository(s.log, s.cfg, s.redisClient)

	s.videoUsecase = video_usecase.NewVideoUsecase(s.log, s.cfg, redisVideoRepo, videoRepo)

	// Comment
	commentRepo := comment_repo.NewCommentRepository(s.log, s.cfg, s.entClient)
	redisCommentRepo := comment_repo.NewRedisCommentRepository(s.log, s.cfg, s.redisClient)

	s.commentUsecase = comment_usecase.NewCommentUsecase(s.log, s.cfg, redisCommentRepo, commentRepo)

	// Object
	objectRepo := object_repo.NewObjectRepository(s.log, s.cfg, s.entClient)
	redisObjectRepo := object_repo.NewRedisObjectRepository(s.log, s.cfg, s.redisClient)

	s.objectUsecase = object_usecase.NewObjectUsecase(s.log, s.cfg, redisObjectRepo, objectRepo, videoRepo)

	// Reaction
	reactionRepo := reaction_repo.NewReactionRepository(s.log, s.cfg, s.entClient)
	redisReactionRepo := reaction_repo.NewRedisReactionRepository(s.log, s.cfg, s.redisClient)

	s.reactionUsecase = reaction_usecase.NewReactionUsecase(s.log, s.cfg, redisReactionRepo, reactionRepo)

	// Viewer
	viewerRepo := viewer_repo.NewViewerRepository(s.log, s.cfg, s.entClient)
	redisViewerRepo := viewer_repo.NewRedisViewerRepository(s.log, s.cfg, s.redisClient)

	s.viewerUsecase = viewer_usecase.NewViewerUsecase(s.log, s.cfg, redisViewerRepo, viewerRepo)

	// Initialize Kafka consumer
	readerMessageProcessor := readerKafka.NewReaderMessageProcessor(s.log, s.cfg, s.v, s.videoUsecase, s.metrics)
	s.log.Info("Starting Reader Kafka consumers")
	cg := kafkaClient.NewConsumerGroup(s.cfg.Kafka.Brokers, s.cfg.Kafka.GroupID, s.log)
	go cg.ConsumeTopic(ctx, s.getConsumerGroupTopics(), readerKafka.PoolSize, readerMessageProcessor.ProcessMessages)

	if err := s.connectKafkaBrokers(ctx); err != nil {
		return errors.Wrap(err, "s.connectKafkaBrokers")
	}

	return nil
}

func (s *server) startHTTPServer() error {
	routerInit := fasthttprouter.New()
	routerInit.HandleOPTIONS = true
	routerInit.HandleCORS.Handle = true
	routerInit.HandleCORS.AllowOrigin = "*"
	routerInit.HandleCORS.AllowMethods = []string{"GET", "HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS"}

	productHandlers := v1.NewReaderHttpService(
		routerInit,
		s.log,
		s.cfg,
		s.v,
		s.videoUsecase,
		s.commentUsecase,
		s.objectUsecase,
		s.reactionUsecase,
		s.viewerUsecase,
		s.metrics,
	)
	productHandlers.MapRoutes()

	port := s.cfg.Http.Port
	addr := fmt.Sprintf("%s", port)

	fmt.Printf("Server is listening on %s\n", addr)
	if err := fasthttp.ListenAndServe(addr, routerInit.Handler); err != nil {
		return fmt.Errorf("Error starting HTTP server: %v", err)
	}

	return nil
}

func (s *server) startGRPCServer() error {
	_, _, err := s.newReaderGrpcServer()
	if err != nil {
		return errors.Wrap(err, "NewScmGrpcServer")
	}
	//defer closeGrpcServer()

	//grpcServer.GracefulStop()
	return nil
}
