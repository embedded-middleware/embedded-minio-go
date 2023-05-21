package gominio

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"embedded-minio-go/gominio/model"
	"embedded-minio-go/gominio/router"
)

type ConfigValue struct {
	Value string `json:"value"`
}

type ServerConfig struct {
	Access string
	Secret string
	Port   int
}

type Server struct {
	config *ServerConfig
	router *gin.Engine
	server *http.Server
	done   chan struct{}
}

func NewServer(cfg *ServerConfig) (*Server, error) {
	// Initialize Gin engine
	router := gin.Default()

	return &Server{
		config: cfg,
		router: router,
		done:   make(chan struct{}),
	}, nil
}

// Start starts the server and returns the listen port.
func (s *Server) Start() (int, error) {
	// Init minio server
	model.InitMinioServer(s.config.Access, s.config.Secret)

	// Define routes
	router.RegisterApiRouter(s.router)

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", s.config.Port))
	if err != nil {
		return 0, err
	}

	s.server = &http.Server{Handler: s.router}
	go func() {
		if err := s.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("Error serving requests: %v", err)
		}
	}()

	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		_ = s.server.Close()
		return 0, fmt.Errorf("failed to get listen port")
	}
	return addr.Port, nil
}

// Close shuts down the server.
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.server.Shutdown(ctx); err != nil {
		return err
	}

	select {
	case <-s.done:
		// Wait for Serve() goroutine to exit
	case <-time.After(10 * time.Second):
		// Serve() goroutine is stuck, force exit
		log.Println("Serve() goroutine stuck, force exit")
	}

	return nil
}
