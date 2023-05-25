package router

import (
	"embedded-minio-go/gominio/model"
	"github.com/gin-gonic/gin"
	"io"
	"log"
	"net/http"
	"strconv"
)

type ApiServer struct {
	ms *model.MinioServer
}

func (api *ApiServer) GetMS() *model.MinioServer {
	return api.ms
}

// RegisterApiRouter register S3 requests routers
func RegisterApiRouter(router *gin.Engine, minioServer *model.MinioServer) *ApiServer {
	api := &ApiServer{
		ms: minioServer,
	}

	// Bucket routers
	router.GET("/", api.ListBucket)
	router.HEAD("/:bucket/", api.HeadBucket)
	router.GET("/:bucket/", api.GetBucket)
	router.PUT("/:bucket/", api.PutBucket)
	router.DELETE("/:bucket/", api.DeleteBucket)

	return api
}

// HeadBucket head bucket
func (api *ApiServer) HeadBucket(ctx *gin.Context) {
	var (
		bucket string
	)

	bucket = ctx.Param("bucket")
	if !api.GetMS().BucketExists(bucket) {
		// Bucket not exists
		ctx.Writer.WriteHeader(model.ErrNoSuchBucket.HTTPStatusCode)
		return
	}

	SuccessResponse(ctx, http.StatusOK, nil)
}

// ListBucket list bucket
func (api *ApiServer) ListBucket(ctx *gin.Context) {
	// list bucket
	lr := api.GetMS().ListBucket()
	SuccessResponse(ctx, http.StatusOK, lr.Encode())
}

// GetBucket get bucket information
func (api *ApiServer) GetBucket(ctx *gin.Context) {
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
		if !api.GetMS().BucketExists(bucket) {
			// Bucket not exists
			ErrResponse(ctx, "", bucket, model.ErrNoSuchBucket)
			return
		}
	}

	if location {
		SuccessResponse(ctx, http.StatusOK, model.LocationResponse{}.Encode())
		return
	}

	if policy {
		content, ok := api.GetMS().GetBucketPolicy(bucket)
		if !ok {
			ErrResponse(ctx, "", bucket, model.ErrNoSuchBucket)
			return
		}
		SuccessResponse(ctx, http.StatusOK, []byte(content))
	}
}

// PutBucket create bucket
func (api *ApiServer) PutBucket(ctx *gin.Context) {
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
		if !api.GetMS().BucketExists(bucket) {
			// Bucket not exists
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
		ok := api.GetMS().SetBucketPolicy(bucket, string(content))
		if !ok {
			ErrResponse(ctx, "", bucket, model.ErrBucketAlreadyOwnedByYou)
			return
		}
		SuccessResponse(ctx, http.StatusNoContent, nil)
		return
	}

	bucket = ctx.Param("bucket")
	if !api.GetMS().MakeBucket(bucket) {
		// bucket exists
		ErrResponse(ctx, "", bucket, model.ErrBucketAlreadyOwnedByYou)
		return
	}

	SuccessResponse(ctx, http.StatusOK, nil)
}

// DeleteBucket delete bucket
func (api *ApiServer) DeleteBucket(ctx *gin.Context) {
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
	err = api.GetMS().DelBucket(bucket, force)
	if err != nil {
		ErrResponse(ctx, "", bucket, model.ErrBucketNotEmpty)
		return
	}

	SuccessResponse(ctx, http.StatusNoContent, nil)
}

// SuccessResponse success response
func SuccessResponse(ctx *gin.Context, status int, data []byte) {
	ctx.Writer.WriteHeader(status)
	_, err := ctx.Writer.Write(data)
	if err != nil {
		log.Println("write response err", err)
	}
}

// ErrResponse error response
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
