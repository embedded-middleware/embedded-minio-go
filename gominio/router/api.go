package router

import (
	"embedded-minio-go/gominio/model"
	"github.com/gin-gonic/gin"
	"io"
	"log"
	"net/http"
	"strconv"
)

// RegisterApiRouter 注册S3请求的路由
func RegisterApiRouter(router *gin.Engine) {
	// Bucket相关路由
	router.GET("/", ListBucket)
	router.HEAD("/:bucket/", HeadBucket)
	router.GET("/:bucket/", GetBucket)
	router.PUT("/:bucket/", PutBucket)
	router.DELETE("/:bucket/", DeleteBucket)
}

// HeadBucket head bucket
func HeadBucket(ctx *gin.Context) {
	var (
		bucket string
	)

	bucket = ctx.Param("bucket")
	if !model.GetMS().BucketExists(bucket) {
		// bucket 不存在
		ctx.Writer.WriteHeader(model.ErrNoSuchBucket.HTTPStatusCode)
		return
	}

	SuccessResponse(ctx, http.StatusOK, nil)
}

// ListBucket 列出bucket
func ListBucket(ctx *gin.Context) {
	// list bucket
	lr := model.GetMS().ListBucket()
	SuccessResponse(ctx, http.StatusOK, lr.Encode())
}

// GetBucket 获取bucket的信息
func GetBucket(ctx *gin.Context) {
	var (
		location   bool
		policy     bool
		lifecycle  bool
		encryption bool
		versioning bool
		bucket     string
	)

	_, location = ctx.GetQuery("location")
	_, policy = ctx.GetQuery("policy")
	_, lifecycle = ctx.GetQuery("lifecycle")
	_, encryption = ctx.GetQuery("encryption")
	_, versioning = ctx.GetQuery("versioning")
	if location || policy || lifecycle || encryption || versioning {
		bucket = ctx.Param("bucket")
		if !model.GetMS().BucketExists(bucket) {
			// bucket 不存在
			ErrResponse(ctx, "", bucket, model.ErrNoSuchBucket)
			return
		}
	}

	if location {
		SuccessResponse(ctx, http.StatusOK, model.LocationResponse{}.Encode())
		return
	}

	if policy {
		content, ok := model.GetMS().GetBucketPolicy(bucket)
		if !ok {
			ErrResponse(ctx, "", bucket, model.ErrNoSuchBucket)
			return
		}
		SuccessResponse(ctx, http.StatusOK, []byte(content))
	}
}

// PutBucket 创建存储桶bucket
func PutBucket(ctx *gin.Context) {
	var (
		policy     bool
		lifecycle  bool
		encryption bool
		versioning bool
		content    []byte
		bucket     string
		err        error
	)

	_, policy = ctx.GetQuery("policy")
	_, lifecycle = ctx.GetQuery("lifecycle")
	_, encryption = ctx.GetQuery("encryption")
	_, versioning = ctx.GetQuery("versioning")
	if policy || lifecycle || encryption || versioning {
		bucket = ctx.Param("bucket")
		if !model.GetMS().BucketExists(bucket) {
			// bucket 不存在
			ErrResponse(ctx, "", bucket, model.ErrNoSuchBucket)
			return
		}

		content, err = io.ReadAll(ctx.Request.Body)
		if err != nil {
			ErrResponse(ctx, "", bucket, model.ErrInvalidRequest)
			return
		}
	}

	if policy {
		ok := model.GetMS().SetBucketPolicy(bucket, string(content))
		if !ok {
			ErrResponse(ctx, "", bucket, model.ErrBucketAlreadyOwnedByYou)
			return
		}
		SuccessResponse(ctx, http.StatusNoContent, nil)
		return
	}

	bucket = ctx.Param("bucket")
	if !model.GetMS().MakeBucket(bucket) {
		// bucket exists
		ErrResponse(ctx, "", bucket, model.ErrBucketAlreadyOwnedByYou)
		return
	}

	SuccessResponse(ctx, http.StatusOK, nil)
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
