package router

import (
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"embedded-minio-go/gominio/model"
)

// RegisterApiRouter 注册S3请求的路由
func RegisterApiRouter(router *gin.Engine) {
	// Bucket相关路由
	router.HEAD("/:bucket/", HeadBucket)
	router.GET("/:bucket/", GetBucketLocation)
	router.PUT("/:bucket/", PutBucket)
	router.DELETE("/:bucket/", DeleteBucket)

	// Object相关路由
	router.PUT("/:bucket/:object", PutObject)
	router.GET("/:bucket/:object", GetObject)
}

// HeadBucket head bucket
func HeadBucket(ctx *gin.Context) {
	var (
		bucket string
	)

	bucket = ctx.Param("bucket")
	if model.GetMS().BucketExists(bucket) {
		ctx.Writer.WriteHeader(http.StatusOK)
		return
	}

	// bucket 不存在
	ctx.Writer.WriteHeader(model.ErrNoSuchBucket.HTTPStatusCode)
}

// GetBucketLocation 获取bucket的location
func GetBucketLocation(ctx *gin.Context) {
	var (
		bucket string
	)

	bucket = ctx.Param("bucket")
	if model.GetMS().BucketExists(bucket) {
		ctx.Writer.WriteHeader(http.StatusOK)
		_, err := ctx.Writer.Write(model.LocationResponse{}.Encode())
		if err != nil {
			log.Println("write response err", err)
		}
		return
	}

	// bucket 不存在
	ErrResponse(ctx, "", bucket, model.ErrNoSuchBucket)
}

// PutBucket 创建存储桶bucket
func PutBucket(ctx *gin.Context) {
	var (
		bucket string
	)

	bucket = ctx.Param("bucket")
	if model.GetMS().MakeBucket(bucket) {
		ctx.Writer.WriteHeader(http.StatusOK)
		return
	}

	// bucket exists
	ErrResponse(ctx, "", bucket, model.ErrBucketAlreadyOwnedByYou)
}

func DeleteBucket(ctx *gin.Context) {
	var (
		bucket string
		force  bool
		err    error
	)

	bucket = ctx.Param("bucket")
	forceArg := ctx.Request.Header.Get("x-minio-force-delete")
	if forceArg != "" {
		force, err = strconv.ParseBool(forceArg)
		if err != nil {
			apiErr := model.ErrInvalidRequest
			apiErr.Description = err.Error()
			ErrResponse(ctx, "", bucket, apiErr)
			return
		}
	}
	err = model.GetMS().DelBucket(bucket, force)
	if err != nil {
		ErrResponse(ctx, "", bucket, model.ErrBucketNotEmpty)
		return
	}

	ctx.Writer.WriteHeader(http.StatusNoContent)
}

func PutObject(ctx *gin.Context) {
	var (
		bucket string
		object string
		err    error
	)

	bucket = ctx.Param("bucket")
	object = ctx.Param("object")
	data, err := io.ReadAll(ctx.Request.Body)
	if err != nil {
		ErrResponse(ctx, object, bucket, model.ErrInvalidRequest)
		return
	}

	content := ""
	datas := strings.Split(string(data), "\n")
	for i := 1; i < len(datas)-3; i++ {
		content += datas[i] + "\n"
	}
	if len(content) > 1 {
		content = content[:len(content)-2]
	}

	err = model.GetMS().PutObject(bucket, object, content)
	if err == nil {
		ctx.Writer.WriteHeader(http.StatusOK)
		return
	}

	ErrResponse(ctx, object, bucket, model.ErrNoSuchBucket)
}

func GetObject(ctx *gin.Context) {
	var (
		bucket string
		object string
		oi     *model.ObjectInfo
		err    error
	)

	bucket = ctx.Param("bucket")
	object = ctx.Param("object")
	oi, err = model.GetMS().GetObject(bucket, object)
	if err == nil {
		ctx.Writer.Header().Set("Last-Modified", oi.LastModified.Format(http.TimeFormat))
		ctx.Writer.WriteHeader(http.StatusOK)
		_, err := ctx.Writer.Write([]byte(oi.Data))
		if err != nil {
			log.Println("write response err", err)
		}
		return
	}

	ErrResponse(ctx, object, bucket, model.ErrNoSuchKey)
}

// ErrResponse 统一处理失败响应
func ErrResponse(ctx *gin.Context, key, bucket string, apiErr model.APIError) {
	apiRsp := model.APIErrorResponse{
		Code:       apiErr.Code,
		Message:    apiErr.Description,
		Key:        key,
		BucketName: bucket,
	}
	ctx.Writer.WriteHeader(apiErr.HTTPStatusCode)
	_, err := ctx.Writer.Write(apiRsp.Encode())
	if err != nil {
		log.Println("write response err", err)
	}
}
