package server

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	upload_services "github.com/glu/video-real-time-ranking/writer_service/internal/services/upload"

	kafkaClient "github.com/glu/video-real-time-ranking/core/pkg/kafka"
	"github.com/glu/video-real-time-ranking/core/pkg/grpc_client"
	"github.com/glu/video-real-time-ranking/core/pkg/interceptors"
	"github.com/glu/video-real-time-ranking/core/pkg/logger"
	"github.com/glu/video-real-time-ranking/core/pkg/mongodb"
	"github.com/glu/video-real-time-ranking/core/pkg/mysql"
	"github.com/glu/video-real-time-ranking/core/pkg/tracing"
	readerService "github.com/glu/video-real-time-ranking/core/proto/services/reader/proto_buf"
	"github.com/glu/video-real-time-ranking/ent"
	"github.com/glu/video-real-time-ranking/writer_service/config"
	v1 "github.com/glu/video-real-time-ranking/writer_service/internal/delivery/http/v1"
	kafkaConsumer "github.com/glu/video-real-time-ranking/writer_service/internal/delivery/kafka"
	"github.com/glu/video-real-time-ranking/writer_service/internal/domain/services"
	"github.com/glu/video-real-time-ranking/writer_service/internal/domain/usecase"
	metrics "github.com/glu/video-real-time-ranking/writer_service/internal/metrics"
	"github.com/glu/video-real-time-ranking/writer_service/internal/middlewares"
	activity_history_repo "github.com/glu/video-real-time-ranking/writer_service/internal/repositories/activity_history"
	comment_repo "github.com/glu/video-real-time-ranking/writer_service/internal/repositories/comment"
	object_repo "github.com/glu/video-real-time-ranking/writer_service/internal/repositories/object"
	reaction_repo "github.com/glu/video-real-time-ranking/writer_service/internal/repositories/reaction"
	video_repo "github.com/glu/video-real-time-ranking/writer_service/internal/repositories/video"
	viewer_repo "github.com/glu/video-real-time-ranking/writer_service/internal/repositories/viewer"
	activity_history_usecase "github.com/glu/video-real-time-ranking/writer_service/internal/usecase/activity_history"
	comment_usecase "github.com/glu/video-real-time-ranking/writer_service/internal/usecase/comment"
	object_usecase "github.com/glu/video-real-time-ranking/writer_service/internal/usecase/object"
	reaction_usecase "github.com/glu/video-real-time-ranking/writer_service/internal/usecase/reaction"
	video_usecase "github.com/glu/video-real-time-ranking/writer_service/internal/usecase/video"
	viewer_usecase "github.com/glu/video-real-time-ranking/writer_service/internal/usecase/viewer"

	"github.com/georgecookeIW/fasthttprouter"
	"github.com/go-playground/validator"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/valyala/fasthttp"
	"go.mongodb.org/mongo-driver/mongo"
)

type server struct {
	log                    logger.Logger
	cfg                    *config.Config
	v                      *validator.Validate
	kafkaConn              *kafka.Conn
	videoUsecase           usecase.IVideoUsecase
	viewerUsecase          usecase.IViewerUsecase
	objectUsecase          usecase.IObjectUsecase
	reactionUsecase        usecase.IReactionUsecase
	commentUsecase         usecase.ICommentUsecase
	activityHistoryUsecase usecase.IActivityHistoryUsecase
	uploadServices         services.IUploadServices
	im                     interceptors.InterceptorManager
	mw                     middlewares.MiddlewareManager
	entClient              *ent.Client
	mongoClient            *mongo.Client
	metrics                *metrics.WriterServiceMetrics
}

func NewServer(log logger.Logger, cfg *config.Config) *server {
	return &server{log: log, cfg: cfg, v: validator.New()}
}

func (s *server) Run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	s.im = interceptors.NewInterceptorManager(s.log)
	s.metrics = metrics.NewWriterServiceMetrics(s.cfg)

	if err := s.initializeDatabaseConnections(); err != nil {
		return err
	}

	if err := s.initializeKafka(ctx); err != nil {
		return err
	}

	if err := s.startHTTPServer(); err != nil {
		return err
	}

	if s.cfg.Kafka.InitTopics {
		s.initKafkaTopics(ctx)
	}

	s.runHealthCheck(ctx)
	s.runMetrics(cancel)
	s.setupJaegerTracer()

	<-ctx.Done()
	//s.shutdownGrpcServer()
	//defer s.kafkaConn.Close()

	return nil
}

func (s *server) initializeDatabaseConnections() error {
	entConn, err := mysql.SetupEntClient(s.cfg.Postgresql)
	if err != nil {
		return errors.Wrap(err, "mysql.SetupEntClient")
	}
	s.entClient = entConn
	s.log.Infof("MySQL connected: %v", entConn)

	mongoDBConn, err := mongodb.NewMongoDBConn(context.Background(), s.cfg.Mongo)
	if err != nil {
		s.log.WarnMsg("mongodb.NewMongoDBConn", err)
		// Non-fatal: activity history simply won't log if Mongo is unavailable
	} else {
		s.mongoClient = mongoDBConn
		s.log.Infof("MongoDB connected")
	}

	return nil
}

func (s *server) initializeKafka(ctx context.Context) error {
	kafkaProducer := kafkaClient.NewProducer(s.log, s.cfg.Kafka.Brokers)
	defer kafkaProducer.Close()

	videoRepo := video_repo.NewVideoRepository(s.log, s.cfg, s.entClient)
	reactionRepo := reaction_repo.NewReactionRepository(s.log, s.cfg, s.entClient)
	commentRepo := comment_repo.NewCommentRepository(s.log, s.cfg, s.entClient)
	objectRepo := object_repo.NewObjectRepository(s.log, s.cfg, s.entClient)
	viewerRepo := viewer_repo.NewViewerRepository(s.log, s.cfg, s.entClient)

	readerServiceConn, err := grpc_client.NewReaderServiceConn(ctx, s.cfg.GRPC.ReaderServicePort, s.im)
	if err != nil {
		return err
	}
	rsClient := readerService.NewReaderRedisServiceClient(readerServiceConn)

	s.uploadServices = upload_services.NewUploadService(s.log, s.cfg)

	// Activity history - wire if MongoDB is available
	if s.mongoClient != nil {
		activityHistoryRepo := activity_history_repo.NewMongoRepository(s.log, s.cfg, s.mongoClient)
		s.activityHistoryUsecase = activity_history_usecase.NewActivityHistoryUsecase(s.log, activityHistoryRepo)
	}

	s.videoUsecase = video_usecase.NewVideoUsecase(s.log, s.cfg, videoRepo, commentRepo, viewerRepo, reactionRepo, objectRepo, rsClient, s.uploadServices, kafkaProducer)
	s.commentUsecase = comment_usecase.NewCommentUsecase(s.log, s.cfg, commentRepo, nil)
	s.viewerUsecase = viewer_usecase.NewViewerUsecase(s.log, s.cfg, viewerRepo, nil)
	s.reactionUsecase = reaction_usecase.NewReactionUsecase(s.log, s.cfg, reactionRepo, nil)
	s.objectUsecase = object_usecase.NewObjectUsecase(s.log, s.cfg, objectRepo, nil)

	messageProcessor := kafkaConsumer.NewMessageProcessor(s.log, s.cfg, s.v, s.videoUsecase, s.metrics)

	s.log.Info("Starting Writer Kafka consumers")
	cg := kafkaClient.NewConsumerGroup(s.cfg.Kafka.Brokers, s.cfg.Kafka.GroupID, s.log)
	go cg.ConsumeTopic(ctx, s.getConsumerGroupTopics(), kafkaConsumer.PoolSize, messageProcessor.ProcessMessages)

	if err := s.connectKafkaBrokers(ctx); err != nil {
		return errors.Wrap(err, "s.connectKafkaBrokers")
	}
	defer s.kafkaConn.Close()

	return nil
}


func (s *server) startHTTPServer() error {
	routerInit := fasthttprouter.New()
	routerInit.HandleOPTIONS = true
	routerInit.HandleCORS.Handle = true
	routerInit.HandleCORS.AllowOrigin = "*"
	routerInit.HandleCORS.AllowMethods = []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}

	productHandlers := v1.NewWriterHttpService(
		routerInit,
		s.log,
		s.cfg,
		s.v,
		s.videoUsecase,
		s.viewerUsecase,
		s.objectUsecase,
		s.reactionUsecase,
		s.commentUsecase,
		s.uploadServices,
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

func (s *server) setupJaegerTracer() {
	if s.cfg.Jaeger.Enable {
		tracer, closer, err := tracing.NewJaegerTracer(s.cfg.Jaeger)
		if err == nil {
			defer closer.Close()
			opentracing.SetGlobalTracer(tracer)
		}
	}
}

//func (s *server) shutdownGrpcServer() {
//	closeGrpcServer, grpcServer, err := s.newWriterGrpcServer()
//	if err == nil {
//		defer closeGrpcServer()
//		grpcServer.GracefulStop()
//	}
//}
