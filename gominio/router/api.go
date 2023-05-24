package router

import (
	"embedded-minio-go/gominio/model"
	"fmt"
	"github.com/gin-gonic/gin"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
)

// RegisterApiRouter 注册S3请求的路由
func RegisterApiRouter(router *gin.Engine) {
	// Bucket相关路由
	router.HEAD("/:bucket/", HeadBucket)
	router.GET("/:bucket/", GetBucketLocation)
	router.PUT("/:bucket/", PutBucket)
	router.DELETE("/:bucket/", DeleteBucket)

	// Object相关路由
	router.HEAD("/:bucket/:object", HeadObject)
	router.PUT("/:bucket/:object", PutObject)
	router.POST("/:bucket/:object", MultipartObject)
	router.DELETE("/:bucket/:object", DeleteObject)
	router.GET("/:bucket/:object", GetObject)
}

// HeadBucket head bucket
func HeadBucket(ctx *gin.Context) {
	var (
		bucket string
	)

	bucket = ctx.Param("bucket")
	if model.GetMS().BucketExists(bucket) {
		SuccessResponse(ctx, http.StatusOK, nil)
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
		SuccessResponse(ctx, http.StatusOK, model.LocationResponse{}.Encode())
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
		SuccessResponse(ctx, http.StatusOK, nil)
		return
	}

	// bucket exists
	ErrResponse(ctx, "", bucket, model.ErrBucketAlreadyOwnedByYou)
}

// DeleteBucket 删除存储桶
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

	SuccessResponse(ctx, http.StatusNoContent, nil)
}

// HeadObject 判断是分片上传还是直接上传
// TODO:: 分片上传的下载处理
func HeadObject(ctx *gin.Context) {
	var (
		bucket string
		object string
		oi     *model.ObjectInfo
		err    error
	)

	bucket = ctx.Param("bucket")
	object = ctx.Param("object")

	oi, err = model.GetMS().GetObject(bucket, object)
	if err != nil {
		apiErr := model.ErrInvalidRequest
		apiErr.Description = err.Error()
		ErrResponse(ctx, object, bucket, apiErr)
		return
	}
	ctx.Writer.Header().Set("Last-Modified", oi.LastModified.Format(http.TimeFormat))
	ctx.Writer.Header()["ETag"] = []string{"\"" + oi.Etag + "\""}
	ctx.Writer.Header().Set("Content-Length", fmt.Sprintf("%d", oi.Size))
	SuccessResponse(ctx, http.StatusOK, nil)
}

// PutObject 上传对象，包括直接上传和分片上传
func PutObject(ctx *gin.Context) {
	var (
		bucket     string
		object     string
		etag       string
		partNumber int
		uploadId   string
		err        error
	)

	etag = model.GetUid()
	bucket = ctx.Param("bucket")
	object = ctx.Param("object")
	data, err := io.ReadAll(ctx.Request.Body)
	if err != nil {
		ErrResponse(ctx, object, bucket, model.ErrInvalidRequest)
		return
	}

	// 获取上传的数据
	content := ""
	datas := strings.Split(string(data), "\n")
	for i := 1; i < len(datas)-3; i++ {
		content += datas[i] + "\n"
	}
	if len(content) > 1 {
		content = content[:len(content)-2]
	}

	// 分片上传处理
	if part, ok := ctx.GetQuery("partNumber"); ok {
		partNumber, err = strconv.Atoi(part)
		if err != nil {
			ErrResponse(ctx, object, bucket, model.ErrInvalidRequest)
			return
		}
		uploadId, ok = ctx.GetQuery("uploadId")
		if !ok {
			ErrResponse(ctx, object, bucket, model.ErrInvalidRequest)
			return
		}
		err = model.GetMS().PutObjectPart(bucket, object, uploadId, etag, partNumber, []byte(content))
	} else {
		err = model.GetMS().PutObject(bucket, object, etag, []byte(content))
	}

	if err == nil {
		ctx.Writer.Header().Set("ETag", etag)
		SuccessResponse(ctx, http.StatusOK, nil)
		return
	}

	ErrResponse(ctx, object, bucket, model.ErrNoSuchBucket)
}

// MultipartObject 包括创建分片上传id 和 完成分片上传
func MultipartObject(ctx *gin.Context) {
	var (
		bucket   string
		object   string
		uploadId string
		uploads  bool
		complete bool
		etag     string
		err      error
	)
	bucket = ctx.Param("bucket")
	object = ctx.Param("object")

	// 创建分片上传Id的处理
	_, uploads = ctx.GetQuery("uploads")
	if uploads {
		uploadId = model.GetUid()
		err = model.GetMS().PutObjectPart(bucket, object, uploadId, "", 0, nil)
		if err != nil {
			ErrResponse(ctx, object, bucket, model.ErrInvalidRequest)
			return
		}
		SuccessResponse(ctx, http.StatusOK, model.InitiateMultipartUploadResult{
			Bucket:   bucket,
			Key:      object,
			UploadID: uploadId,
		}.Encode())
		return
	}

	// 处理 complete multipart upload
	uploadId, complete = ctx.GetQuery("uploadId")
	if !complete {
		ErrResponse(ctx, object, bucket, model.ErrInvalidRequest)
		return
	}
	var parts = new(model.CompleteMultiPart)
	parts.Decode(ctx.Request.Body)
	etag, err = model.GetMS().CompleteObjectPart(bucket, object, uploadId, parts)
	if err != nil {
		ErrResponse(ctx, object, bucket, model.ErrInvalidRequest)
		return
	}
	ctx.Writer.Header().Set("ETag", etag)
	SuccessResponse(ctx, http.StatusOK, model.CompleteMultipartUploadResponse{
		Bucket: bucket,
		Key:    object,
		ETag:   etag,
	}.Encode())
}

func DeleteObject(ctx *gin.Context) {
	var (
		bucket string
		object string
		err    error
	)

	bucket = ctx.Param("bucket")
	object = ctx.Param("object")

	err = model.GetMS().DeleteObject(bucket, object)
	if err != nil {
		apiErr := model.ErrInvalidRequest
		apiErr.Description = err.Error()
		ErrResponse(ctx, object, bucket, apiErr)
		return
	}
	SuccessResponse(ctx, http.StatusNoContent, nil)
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
		ctx.Writer.Header().Set("Content-Length", fmt.Sprintf("%d", oi.Size))
		ctx.Writer.Header().Set("Last-Modified", oi.LastModified.Format(http.TimeFormat))
		SuccessResponse(ctx, http.StatusOK, oi.Data)
		return
	}

	ErrResponse(ctx, object, bucket, model.ErrNoSuchKey)
}

// SuccessResponse 成功响应
func SuccessResponse(ctx *gin.Context, status int, data []byte) {
	ctx.Writer.WriteHeader(status)
	_, err := ctx.Writer.Write(data)
	if err != nil {
		log.Println("write response err", err)
	}
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
