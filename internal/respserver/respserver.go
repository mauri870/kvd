package respserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mauri870/kvd/internal/kvstore"
	"github.com/mauri870/kvd/internal/raftstore"
	"github.com/tidwall/redcon"
)

type Server struct {
	store *raftstore.Store
	srv   *redcon.Server
}

func New(store *raftstore.Store) (*Server, error) {
	return &Server{store: store}, nil
}

func (s *Server) Run(ctx context.Context, addr string, idleTimeout time.Duration) error {
	var wg sync.WaitGroup
	var closed int32

	// There is some machinery here to manage context and cancelation.
	// Unfortunately this library is not context-aware.
	srv := redcon.NewServer(addr,
		// handler
		func(conn redcon.Conn, cmd redcon.Command) {
			if atomic.LoadInt32(&closed) != 0 {
				// server closed, close connection
				conn.Close()
				return
			}

			err := s.handler(conn, cmd)
			if err == nil {
				return
			}

			slog.Debug("Failed to execute command", "err", err)
			switch {
			case errors.Is(err, kvstore.ErrKeyNotFound):
				conn.WriteNull()
			default:
				slog.Error("unhandled error", "err", err)
				conn.WriteError("ERR " + err.Error())
			}
		},
		// accept
		func(conn redcon.Conn) bool {
			if atomic.LoadInt32(&closed) != 0 {
				// Server closed, do not accept this connection
				return false
			}
			// Add connection to a wait group
			wg.Add(1)
			return true
		},
		// close
		func(conn redcon.Conn, err error) {
			// Remove connection from wait group
			wg.Done()
		},
	)
	s.srv = srv

	// Set a max amount of time a connection can stay idle.
	s.srv.SetIdleClose(idleTimeout)

	go func() {
		<-ctx.Done()
		slog.Warn("Waiting for open connections to close")
		atomic.StoreInt32(&closed, 1)
		wg.Wait()

		slog.Warn("Shutting down server")
		s.srv.Close()
	}()
	if err := s.srv.ListenAndServe(); err != nil {
		return err
	}

	return nil
}

func (s *Server) handler(conn redcon.Conn, cmd redcon.Command) error {
	slog.Debug("Executing command", "raw", cmd.Raw)

	args := cmd.Args
	cmdname := string(args[0])

	switch cmdname {
	case "PING":
		conn.WriteString("PONG")
	case "QUIT":
		conn.WriteString("OK")
		conn.Close()
	case "SET":
		if len(cmd.Args) < 3 {
			return fmt.Errorf("wrong number of arguments for 'set' command")
		}
		err := s.store.Set(args[1], args[2])
		if err != nil {
			return fmt.Errorf("failed to set key: %w", err)
		}
		conn.WriteString("OK")
	case "GET":
		if len(cmd.Args) < 2 {
			return fmt.Errorf("wrong number of arguments for 'get' command")
		}
		val, err := s.store.Get(args[1])
		if err != nil {
			return fmt.Errorf("failed to get key: %w", err)
		}
		conn.WriteBulkString(string(val))
	case "DEL":
		if len(cmd.Args) < 2 {
			return fmt.Errorf("wrong number of arguments for 'del' command")
		}
		err := s.store.Delete(args[1])
		if err != nil {
			if errors.Is(err, kvstore.ErrKeyNotFound) {
				conn.WriteInt(0)
				return nil
			}
			return fmt.Errorf("failed to delete key: %w", err)
		}
		conn.WriteString("OK")
	case "JOIN":
		if len(cmd.Args) < 2 {
			return fmt.Errorf("wrong number of arguments for 'join' command")
		}
		slog.Info("Joining node", "nodeid", args[1], "address", args[2])
		if err := s.store.Join(string(args[1]), string(args[2])); err != nil {
			return fmt.Errorf("failed to join node: %w", err)
		}
		conn.WriteString("OK")
		conn.Close()
	case "LEAVE":
		if len(cmd.Args) < 1 {
			return fmt.Errorf("wrong number of arguments for 'LEAVE' command")
		}
		slog.Info("Removing node", "nodeid", args[1])
		if err := s.store.Leave(string(args[1])); err != nil {
			return fmt.Errorf("failed to join node: %w", err)
		}
		conn.WriteString("OK")
		conn.Close()
	case "CONFIG":
		if len(cmd.Args) < 3 {
			return fmt.Errorf("wrong number of arguments for 'config' command")
		}

		if string(args[1]) == "SET" {
			return fmt.Errorf("unknown subcommand '%s' for 'config'", args[1])
		}

		// only the basics for the redis-cli to work
		switch string(args[2]) {
		case "save":
			conn.WriteArray(2)
			conn.WriteBulkString("save")
			conn.WriteBulkString("")
		case "appendonly":
			conn.WriteArray(2)
			conn.WriteBulkString("appendonly")
			conn.WriteBulkString("no")
		default:
			return fmt.Errorf("unknown subcommand '%s' for 'config'", args[2])
		}

	default:
		return fmt.Errorf("unknown command '%s'", cmd.Raw)
	}

	return nil
}
