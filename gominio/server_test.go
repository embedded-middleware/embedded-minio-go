package gominio

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestServerStart(t *testing.T) {
	// Initialize server
	cfg := &ServerConfig{
		Port: 0,
	}
	srv, err := NewServer(cfg)
	require.NoError(t, err)

	// Start server
	port, err := srv.Start()
	require.NoError(t, err)
	defer srv.Close()

	require.Greater(t, port, 0)
}
