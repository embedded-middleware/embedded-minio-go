package gominio

import (
	"encoding/xml"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/minio/minio-go/v7/pkg/tags"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
)

type ApiServer struct {
	ms *MinioServer
}

func (api *ApiServer) GetMS() *MinioServer {
	return api.ms
}

// RegisterApiRouter register S3 requests routers
func RegisterApiRouter(router *gin.Engine, minioServer *MinioServer) *ApiServer {
	api := &ApiServer{
		ms: minioServer,
	}

	// Bucket routers
	router.GET("/", api.ListBucket)
	router.HEAD("/:bucket/", api.HeadBucket)
	router.GET("/:bucket/", api.GetBucket)
	router.PUT("/:bucket/", api.PutBucket)
	router.DELETE("/:bucket/", api.DeleteBucket)

	// Object相关路由
	router.HEAD("/:bucket/:object", api.HeadObject)
	router.PUT("/:bucket/:object", api.PutObject)
	router.POST("/:bucket/:object", api.MultipartObject)
	router.DELETE("/:bucket/:object", api.DeleteObject)
	router.GET("/:bucket/:object", api.GetObject)

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
		ctx.Writer.WriteHeader(ErrNoSuchBucket.HTTPStatusCode)
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
			ErrResponse(ctx, "", bucket, ErrNoSuchBucket)
			return
		}
	}

	if location {
		SuccessResponse(ctx, http.StatusOK, LocationResponse{}.Encode())
		return
	}

	if policy {
		content, ok := api.GetMS().GetBucketPolicy(bucket)
		if !ok {
			ErrResponse(ctx, "", bucket, ErrNoSuchBucket)
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
			ErrResponse(ctx, "", bucket, ErrNoSuchBucket)
			return
		}

		content, err = io.ReadAll(ctx.Request.Body)
		if err != nil {
			ErrResponse(ctx, "", bucket, ErrInvalidRequest)
			return
		}
	}

	if policy {
		ok := api.GetMS().SetBucketPolicy(bucket, string(content))
		if !ok {
			ErrResponse(ctx, "", bucket, ErrBucketAlreadyOwnedByYou)
			return
		}
		SuccessResponse(ctx, http.StatusNoContent, nil)
		return
	}

	bucket = ctx.Param("bucket")
	if !api.GetMS().MakeBucket(bucket) {
		// bucket exists
		ErrResponse(ctx, "", bucket, ErrBucketAlreadyOwnedByYou)
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
			apiErr := ErrInvalidRequest
			apiErr.Description = err.Error()
			ErrResponse(ctx, "", bucket, apiErr)
			return
		}
	}
	err = api.GetMS().DelBucket(bucket, force)
	if err != nil {
		ErrResponse(ctx, "", bucket, ErrBucketNotEmpty)
		return
	}

	SuccessResponse(ctx, http.StatusNoContent, nil)
}

// HeadObject Determine whether to upload in shards or directly
// TODO:: Download processing of sharded upload
func (api *ApiServer) HeadObject(ctx *gin.Context) {
	var (
		bucket string
		object string
		oi     *ObjectInfo
		err    error
	)

	bucket = ctx.Param("bucket")
	object = ctx.Param("object")

	oi, err = api.GetMS().GetObject(bucket, object)
	if err != nil {
		apiErr := ErrInvalidRequest
		apiErr.Description = err.Error()
		ErrResponse(ctx, object, bucket, apiErr)
		return
	}
	ctx.Writer.Header().Set("Last-Modified", oi.LastModified.Format(http.TimeFormat))
	ctx.Writer.Header()["ETag"] = []string{"\"" + oi.Etag + "\""}
	ctx.Writer.Header().Set("Content-Length", fmt.Sprintf("%d", oi.Size))
	SuccessResponse(ctx, http.StatusOK, nil)
}

func (api *ApiServer) putObjectTagging(ctx *gin.Context) {
	var (
		bucket string
		object string
	)
	bucket = ctx.Param("bucket")
	object = ctx.Param("object")

	tag, err := tags.MapToObjectTags(map[string]string{})
	if err != nil {
		ErrResponse(ctx, object, bucket, ErrInvalidRequest)
		return
	}

	err = xml.NewDecoder(ctx.Request.Body).Decode(tag)
	if err != nil {
		ErrResponse(ctx, object, bucket, ErrInvalidRequest)
		return
	}

	err = api.GetMS().PutObjectTagging(bucket, object, tag)
	if err != nil {
		ErrResponse(ctx, object, bucket, ErrNoSuchKey)
		return
	}

	SuccessResponse(ctx, http.StatusOK, nil)
}

func (api *ApiServer) putObjectOrPart(ctx *gin.Context) {
	var (
		bucket     string
		object     string
		etag       string
		partNumber int
		uploadId   string
		err        error
	)

	etag = GetUid()
	bucket = ctx.Param("bucket")
	object = ctx.Param("object")
	data, err := io.ReadAll(ctx.Request.Body)
	if err != nil {
		ErrResponse(ctx, object, bucket, ErrInvalidRequest)
		return
	}

	// get upload data
	content := ""
	datas := strings.Split(string(data), "\n")
	for i := 1; i < len(datas)-3; i++ {
		content += datas[i] + "\n"
	}
	if len(content) > 1 {
		content = content[:len(content)-2]
	}

	// upload part processing
	if part, ok := ctx.GetQuery("partNumber"); ok {
		partNumber, err = strconv.Atoi(part)
		if err != nil {
			ErrResponse(ctx, object, bucket, ErrInvalidRequest)
			return
		}
		uploadId, ok = ctx.GetQuery("uploadId")
		if !ok {
			ErrResponse(ctx, object, bucket, ErrInvalidRequest)
			return
		}
		err = api.GetMS().PutObjectPart(bucket, object, uploadId, etag, partNumber, []byte(content))
	} else {
		err = api.GetMS().PutObject(bucket, object, etag, []byte(content))
	}

	if err == nil {
		ctx.Writer.Header().Set("ETag", etag)
		SuccessResponse(ctx, http.StatusOK, nil)
		return
	}

	ErrResponse(ctx, object, bucket, ErrNoSuchBucket)
}

// PutObject Upload objects, including direct upload and sharded upload
func (api *ApiServer) PutObject(ctx *gin.Context) {
	var (
		tagging   bool
		retention bool
		legalHold bool
	)

	_, tagging = ctx.GetQuery("tagging")
	_, retention = ctx.GetQuery("retention")
	_, legalHold = ctx.GetQuery("legal-hold")

	if tagging {
		api.putObjectTagging(ctx)
		return
	}

	if retention {
		return
	}

	if legalHold {
		return
	}

	api.putObjectOrPart(ctx)
}

// MultipartObject Including creating a shard upload ID and completing shard upload
func (api *ApiServer) MultipartObject(ctx *gin.Context) {
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

	// Processing of creating sharded upload ID
	_, uploads = ctx.GetQuery("uploads")
	if uploads {
		uploadId = GetUid()
		err = api.GetMS().PutObjectPart(bucket, object, uploadId, "", 0, nil)
		if err != nil {
			ErrResponse(ctx, object, bucket, ErrInvalidRequest)
			return
		}
		SuccessResponse(ctx, http.StatusOK, InitiateMultipartUploadResult{
			Bucket:   bucket,
			Key:      object,
			UploadID: uploadId,
		}.Encode())
		return
	}

	// resolve complete multipart upload
	uploadId, complete = ctx.GetQuery("uploadId")
	if !complete {
		ErrResponse(ctx, object, bucket, ErrInvalidRequest)
		return
	}
	var parts = new(CompleteMultiPart)
	parts.Decode(ctx.Request.Body)
	etag, err = api.GetMS().CompleteObjectPart(bucket, object, uploadId, parts)
	if err != nil {
		ErrResponse(ctx, object, bucket, ErrInvalidRequest)
		return
	}
	ctx.Writer.Header().Set("ETag", etag)
	SuccessResponse(ctx, http.StatusOK, CompleteMultipartUploadResponse{
		Bucket: bucket,
		Key:    object,
		ETag:   etag,
	}.Encode())
}

func (api *ApiServer) DeleteObject(ctx *gin.Context) {
	var (
		bucket    string
		object    string
		tagging   bool
		retention bool
		legalHold bool
		err       error
	)

	bucket = ctx.Param("bucket")
	object = ctx.Param("object")

	_, tagging = ctx.GetQuery("tagging")
	_, retention = ctx.GetQuery("retention")
	_, legalHold = ctx.GetQuery("legal-hold")

	if tagging {
		err = api.GetMS().RemoveObjectTagging(bucket, object)
		if err != nil {
			ErrResponse(ctx, object, bucket, ErrNoSuchKey)
			return
		}
		SuccessResponse(ctx, http.StatusNoContent, nil)
		return
	}

	if retention {
		return
	}

	if legalHold {
		return
	}

	err = api.GetMS().DeleteObject(bucket, object)
	if err != nil {
		apiErr := ErrInvalidRequest
		apiErr.Description = err.Error()
		ErrResponse(ctx, object, bucket, apiErr)
		return
	}
	SuccessResponse(ctx, http.StatusNoContent, nil)
}

func (api *ApiServer) GetObject(ctx *gin.Context) {
	var (
		bucket    string
		object    string
		tagging   bool
		retention bool
		legalHold bool
		oi        *ObjectInfo
		err       error
	)

	bucket = ctx.Param("bucket")
	object = ctx.Param("object")
	oi, err = api.GetMS().GetObject(bucket, object)
	if err != nil {
		ErrResponse(ctx, object, bucket, ErrNoSuchKey)
		return
	}

	_, tagging = ctx.GetQuery("tagging")
	_, retention = ctx.GetQuery("retention")
	_, legalHold = ctx.GetQuery("legal-hold")

	if tagging {
		SuccessResponse(ctx, http.StatusOK, encodeAny(oi.Tags))
		return
	}

	if retention {
		return
	}

	if legalHold {
		return
	}

	ctx.Writer.Header().Set("Content-Length", fmt.Sprintf("%d", oi.Size))
	ctx.Writer.Header().Set("Last-Modified", oi.LastModified.Format(http.TimeFormat))
	SuccessResponse(ctx, http.StatusOK, oi.Data)
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
func ErrResponse(ctx *gin.Context, key, bucket string, apiErr APIError) {
	apiRsp := APIErrorResponse{
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
