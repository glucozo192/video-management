package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"strconv"
	"strings"

	vidio "github.com/AlexEidt/Vidio"

	"github.com/360EntSecGroup-Skylar/excelize"
	kafkaClient "github.com/glu/video-real-time-ranking/core/pkg/kafka"
	"github.com/glu/video-real-time-ranking/core/pkg/logger"
	"github.com/glu/video-real-time-ranking/core/pkg/utils"
	kafkaMessages "github.com/glu/video-real-time-ranking/core/proto/kafka"
	readerService "github.com/glu/video-real-time-ranking/core/proto/services/reader/proto_buf"
	"github.com/glu/video-real-time-ranking/ent"
	"github.com/glu/video-real-time-ranking/writer_service/config"
	"github.com/glu/video-real-time-ranking/writer_service/internal/domain/models"
	"github.com/glu/video-real-time-ranking/writer_service/internal/domain/repositories"
	"github.com/glu/video-real-time-ranking/writer_service/internal/domain/services"
	"github.com/glu/video-real-time-ranking/writer_service/internal/domain/usecase"
	"github.com/glu/video-real-time-ranking/writer_service/internal/dto"
	"github.com/go-resty/resty/v2"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type videoUsecase struct {
	log                logger.Logger
	cfg                *config.Config
	videoRepository    repositories.IVideoRepositoryWriter
	commentRepository  repositories.ICommentRepositoryWriter
	viewerRepository   repositories.IViewerRepositoryWriter
	reactionRepository repositories.IReactionRepositoryWriter
	objectRepository   repositories.IObjectRepositoryWriter
	uploadServices     services.IUploadServices
	rsClient           readerService.ReaderRedisServiceClient
	kafkaProducer      kafkaClient.Producer
}

func NewVideoUsecase(
	log logger.Logger,
	cfg *config.Config,
	videoRepository repositories.IVideoRepositoryWriter,
	commentRepository repositories.ICommentRepositoryWriter,
	viewerRepository repositories.IViewerRepositoryWriter,
	reactionRepository repositories.IReactionRepositoryWriter,
	objectRepository repositories.IObjectRepositoryWriter,
	rsClient readerService.ReaderRedisServiceClient,
	uploadServices services.IUploadServices,
	kafkaProducer kafkaClient.Producer,
) usecase.IVideoUsecase {
	return &videoUsecase{
		log:                log,
		cfg:                cfg,
		videoRepository:    videoRepository,
		commentRepository:  commentRepository,
		viewerRepository:   viewerRepository,
		reactionRepository: reactionRepository,
		objectRepository:   objectRepository,
		rsClient:           rsClient,
		kafkaProducer:      kafkaProducer,
		uploadServices:     uploadServices,
	}
}

func (p *videoUsecase) GetMaxVersion(ctx context.Context) (int64, error) {
	return p.videoRepository.GetMaxVersion(ctx)
}

func (p *videoUsecase) CreateVideoItemConfig(ctx context.Context, videoId uint) error {
	video, err := p.videoRepository.GetVideoById(ctx, videoId)
	if err != nil {
		return err
	}

	comments, err := p.commentRepository.GetListCommentByVideoID(ctx, videoId)
	if err != nil {
		return err
	}

	viewers, err := p.viewerRepository.GetListViewerByVideoID(ctx, videoId)
	if err != nil {
		return err
	}

	reactions, err := p.reactionRepository.GetListReactionByVideoID(ctx, videoId)
	if err != nil {
		return err
	}

	objects, err := p.objectRepository.GetListObjectByVideoID(ctx, videoId)
	if err != nil {
		return err
	}

	err = p.SaveJsonVideoItem(ctx, video, comments, viewers, reactions, objects)
	if err != nil {
		return err
	}

	return nil
}

func (p *videoUsecase) SaveJsonVideoItem(ctx context.Context, video *ent.Videos, comments []*ent.Comments, viewers []*ent.Viewers, reactions []*ent.Reactions, objects []*ent.Objects) error {
	videoItem := models.VideoItem{
		VideoID:      video.ID,
		Name:         video.Name,
		VideoURL:     models.FolderResource + "/" + video.VideoURL,
		PathResource: video.PathResource,
	}

	var dataComment = make([]models.CommentItem, 0)
	if comments != nil {
		for _, comment := range comments {
			commentItemInit := models.CommentItem{
				CommentID: comment.ID,
				Comment:   comment.Comment,
				UserName:  comment.UserName,
				TimePoint: comment.TimePoint,
			}
			if strings.Contains(comment.Avatar, "avatar/avatar") {
				commentItemInit.Avatar = comment.Avatar
			} else {
				commentItemInit.Avatar = models.FolderImage + "/" + comment.Avatar
			}
			dataComment = append(dataComment, commentItemInit)
		}
	}
	videoItem.Comments = dataComment

	var dataViewer = make([]models.ViewerItem, 0)
	if viewers != nil {
		for _, viewer := range viewers {
			dataViewer = append(dataViewer, models.ViewerItem{
				ViewerID:  viewer.ID,
				Number:    viewer.Number,
				TimePoint: viewer.TimePoint,
			})
		}
	}
	videoItem.Viewers = dataViewer

	var dataReactions = make([]models.ReactionItem, 0)
	if reactions != nil {
		for _, reaction := range reactions {
			dataReactions = append(dataReactions, models.ReactionItem{
				ReactionID: reaction.ID,
				Name:       reaction.Name,
				Number:     reaction.Number,
				TimePoint:  reaction.TimePoint,
			})
		}
	}
	videoItem.Reactions = dataReactions

	var dataObjects = make([]models.ObjectItem, 0)
	if objects != nil {
		for _, object := range objects {
			dataObjects = append(dataObjects, models.ObjectItem{
				ObjectID:    object.ID,
				CoordinateX: object.CoordinateX,
				CoordinateY: object.CoordinateY,
				Length:      object.Length,
				Width:       object.Width,
				Order:       object.Order,
				TimeStart:   object.TimeStart,
				TimeEnd:     object.TimeEnd,
			})
		}
	}
	videoItem.Objects = dataObjects

	buf := new(bytes.Buffer)
	err := json.NewEncoder(buf).Encode(videoItem)
	if err != nil {
		return err
	}

	video.Config = buf.String()
	_, err = p.videoRepository.UpdateVideo(ctx, video)
	if err != nil {
		return err
	}
	return nil
}

func (p *videoUsecase) CreateInBulk(ctx context.Context, videos []*ent.Videos) error {
	err := p.videoRepository.CreateInBulk(ctx, videos)
	if err != nil {
		return errors.Wrap(err, "videoRepository.CreateInBulk")
	}
	return nil
}

func (p *videoUsecase) Delete(ctx context.Context, id uint) error {
	err := p.videoRepository.DeleteVideo(ctx, id)
	if err != nil {
		return errors.Wrap(err, "videoRepository.DeleteVideo")
	}

	// Publish VideoDeleted event (non-blocking)
	if p.kafkaProducer != nil {
		go func() {
			msg, err := proto.Marshal(&kafkaMessages.VideoDelete{VideoID: uint32(id)})
			if err != nil {
				p.log.WarnMsg("proto.Marshal VideoDelete", err)
				return
			}
			if err := p.kafkaProducer.PublishMessage(context.Background(), kafka.Message{
				Topic: p.cfg.KafkaTopics.VideoDeleted.TopicName,
				Value: msg,
			}); err != nil {
				p.log.WarnMsg("kafkaProducer.PublishMessage VideoDeleted", err)
			}
		}()
	}

	return nil
}

func (p *videoUsecase) Update(ctx context.Context, video *ent.Videos) (*ent.Videos, error) {
	_, err := p.rsClient.RemoveCachingByKey(ctx, &readerService.RemoveCachingByKeyReq{
		PrefixKey: p.cfg.ServiceSettings.RedisVideoPrefixKey,
		Key:       strconv.Itoa(int(video.ID)),
	})

	if err != nil {
		return nil, errors.Wrap(err, "Fail to remove caching")
	}

	videoDB, err := p.videoRepository.UpdateVideo(ctx, video)
	if err != nil {
		return nil, errors.Wrap(err, "videoRepository.UpdateVideo")
	}

	// Publish VideoUpdated event (non-blocking)
	if p.kafkaProducer != nil {
		go func() {
			msg, err := proto.Marshal(&kafkaMessages.VideoUpdate{
				VideoID:      uint32(videoDB.ID),
				Name:         videoDB.Name,
				Description:  videoDB.Description,
				VideoUrl:     videoDB.VideoURL,
				Config:       videoDB.Config,
				PathResource: videoDB.PathResource,
				LevelSystem:  videoDB.LevelSystem,
				Status:       videoDB.Status,
				Note:         videoDB.Note,
				Assign:       videoDB.Assign,
				Author:       videoDB.Author,
			})
			if err != nil {
				p.log.WarnMsg("proto.Marshal VideoUpdate", err)
				return
			}
			if err := p.kafkaProducer.PublishMessage(context.Background(), kafka.Message{
				Topic: p.cfg.KafkaTopics.VideoUpdated.TopicName,
				Value: msg,
			}); err != nil {
				p.log.WarnMsg("kafkaProducer.PublishMessage VideoUpdated", err)
			}
		}()
	}

	return videoDB, nil
}

func (p *videoUsecase) Create(ctx context.Context, video *ent.Videos) (*ent.Videos, error) {
	videoDB, err := p.videoRepository.CreateVideo(ctx, video)
	if err != nil {
		return nil, errors.Wrap(err, "videoRepository.createdVideo")
	}

	// Publish VideoCreated event (non-blocking)
	if p.kafkaProducer != nil {
		go func() {
			msg, err := proto.Marshal(&kafkaMessages.VideoCreate{
				VideoID:      uint32(videoDB.ID),
				Name:         videoDB.Name,
				Description:  videoDB.Description,
				VideoUrl:     videoDB.VideoURL,
				Config:       videoDB.Config,
				PathResource: videoDB.PathResource,
				LevelSystem:  videoDB.LevelSystem,
				Status:       videoDB.Status,
				Note:         videoDB.Note,
				Assign:       videoDB.Assign,
				Author:       videoDB.Author,
			})
			if err != nil {
				p.log.WarnMsg("proto.Marshal VideoCreate", err)
				return
			}
			if err := p.kafkaProducer.PublishMessage(context.Background(), kafka.Message{
				Topic: p.cfg.KafkaTopics.VideoCreated.TopicName,
				Value: msg,
			}); err != nil {
				p.log.WarnMsg("kafkaProducer.PublishMessage VideoCreated", err)
			}
		}()
	}

	return videoDB, nil
}

func (p *videoUsecase) BulkImportByExcel(ctx context.Context, file *multipart.FileHeader) error {
	f, err := file.Open()
	if err != nil {
		return errors.Wrap(err, "videoUsecase: file.Open")
	}
	defer f.Close()

	xlsx, errOpenReader := excelize.OpenReader(f)
	if errOpenReader != nil {
		return errors.Wrap(err, "videoUsecase: excelize.OpenReader")
	}
	dataVideos, err := p.toDataVideos(xlsx)
	if err != nil {
		return errors.Wrap(err, "videoUsecase: toDataVideos()")
	}

	positiveByPass := 1 //Positive adds a minimum value validator with the value of 1. Operation fails if the validator fails.
	for _, dataVideo := range dataVideos {
		videoID, err := p.uploadAndCreateVideo(ctx, dataVideo)
		if err != nil {
			return errors.Wrap(err, "videoUsecase: uploadVideo")
		}

		var objects = make([]*ent.Objects, 0)
		for _, object := range dataVideo.DataObjects {
			objects := append(objects, &ent.Objects{
				VideoID:     videoID,
				Description: dataVideo.Unit,
				CoordinateX: positiveByPass,
				CoordinateY: positiveByPass,
				Length:      positiveByPass,
				Width:       positiveByPass,
				Order:       positiveByPass,
				MarkerName:  object.MarkerName,
				TimeStart:   float64(object.TimeStart),
				TimeEnd:     float64(object.TimeEnd),
				TouchVector: object.TouchVector,
			})

			if err := p.objectRepository.CreateInBulk(ctx, objects); err != nil {
				return errors.Wrap(err, "videoUsecase: objectRepository.CreateInBulk")
			}
		}

	}
	return nil
}

func (p *videoUsecase) toDataVideos(xlsx *excelize.File) ([]dto.ImportDataVideo, error) {
	sheetName := "videos"
	rowsVideo, err := xlsx.Rows(sheetName)
	if err != nil {
		return nil, errors.Wrap(err, "videoUsecase: xlsx.Rows(sheetName)")
	}

	byPassRowOfTitle := true
	videos := make([]dto.ImportDataVideo, 0)
	index := -1
	for {
		for rowsVideo.Next() {
			if byPassRowOfTitle {
				byPassRowOfTitle = false
				continue
			}
			row := rowsVideo.Columns()
			if row[0] == "end" {
				return videos, nil
			}
			if row[0] == "" {
				if row[3] == "" {
					return videos, nil
				}
				var object dto.DataObjects

				if row[3] != "" {
					object.MarkerName = row[3]
				}
				if row[4] != "" {
					object.TimeStart, _ = utils.ConvertStringToInt(row[4])
				}
				if row[5] != "" {
					object.TimeEnd, _ = utils.ConvertStringToInt(row[5])
				}
				videos[index].DataObjects = append(videos[index].DataObjects, object)
			} else {
				var video dto.ImportDataVideo
				if row[1] != "" {
					video.Unit = row[1]
				}
				if row[2] != "" {
					video.LinkVideo = row[2]
				}
				var object dto.DataObjects
				if row[3] != "" {
					object.MarkerName = row[3]
				}
				if row[4] != "" {
					object.TimeStart, _ = utils.ConvertStringToInt(row[4])
				}
				if row[5] != "" {
					object.TimeEnd, _ = utils.ConvertStringToInt(row[5])
				}
				video.DataObjects = append(video.DataObjects, object)
				videos = append(videos, video)
				index++
			}
		}
	}

}

const PathStorage = "storage/app/download"
const FolderUploadVideo = "upload/cms_platform/video"

func (p *videoUsecase) uploadAndCreateVideo(ctx context.Context, dataVideo dto.ImportDataVideo) (uint, error) {

	fileNameVideo := uuid.NewString() + ".mp4"
	path, err := p.uploadServices.DownloadAndUploadFile(ctx, dataVideo.LinkVideo, fileNameVideo, PathStorage, FolderUploadVideo)
	if err != nil {
		return 0, err
	}

	videoDB, err := p.videoRepository.CreateVideo(ctx, &ent.Videos{
		Name:        fileNameVideo,
		Description: dataVideo.Unit,
		VideoURL:    path,
	})
	if err != nil {
		return 0, errors.Wrap(err, "videoRepository.createdVideo")
	}
	duration := GetDurationVideo(fmt.Sprintf("%s/%s", PathStorage, fileNameVideo))
	fmt.Println("duration", duration)

	// insert to platform
	if err := p.createVideoToOtherService(path, fileNameVideo, dataVideo, duration); err != nil {
		return 0, err
	}

	return videoDB.ID, nil

}

func GetDurationVideo(path string) int {
	videoFile, err := vidio.NewVideo(path)
	if err != nil {
		fmt.Println(err)
		return 0
	}
	defer videoFile.Close()

	durationInt := videoFile.Duration()

	return int(durationInt)
}

type responseResult struct {
	Data map[string]interface{} `json:"data"`
}

func (p *videoUsecase) createVideoToOtherService(path, fileNameVideo string, dataVideo dto.ImportDataVideo, duration int) error {

	client := resty.New()
	var errResponse interface{}
	var response responseResult

	data := map[string]string{}
	data["device_type"] = "4"
	data["name"] = fileNameVideo
	data["description"] = dataVideo.Unit
	data["path"] = path
	data["video_categories_id"] = "7"
	data["tag_title"] = dataVideo.Unit
	data["is_assign"] = "0"
	data["duration"] = strconv.Itoa(duration)

	url := fmt.Sprintf("%s/%s", config.GetInstance().GetPlatformService(), "api/v1/video")
	_, err := client.R().
		SetError(&errResponse).
		SetResult(&response).
		SetQueryParams(data).
		SetHeader("token", config.GetInstance().GetTokenToServer()).
		Post(url)
	if err != nil {
		return err
	}

	return nil
}
