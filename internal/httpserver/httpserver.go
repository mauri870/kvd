package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/mauri870/kvd/internal/kvstore"
	"github.com/mauri870/kvd/internal/raftstore"
	"go.uber.org/zap"
)

type Server struct {
	store  *raftstore.Store
	srv    *http.Server
	mux    *mux.Router
	logger *zap.Logger
}

// New creates a new http server.
func New(store *raftstore.Store, logger *zap.Logger) (*Server, error) {
	s := &Server{store: store, mux: mux.NewRouter(), logger: logger}

	h := func(f func(w http.ResponseWriter, r *http.Request) error) http.HandlerFunc {
		return s.handleErr(f)
	}

	s.mux.HandleFunc("/kv/{key}", h(s.handleKvGet)).Methods("GET")
	s.mux.HandleFunc("/kv", h(s.handleKvSet)).Methods("POST")
	s.mux.HandleFunc("/kv/{key}", h(s.handleKvDelete)).Methods("DELETE")

	s.mux.HandleFunc("/kv/join", h(s.handleKvJoin)).Methods("POST")
	s.mux.HandleFunc("/kv/leave/{nodeID}", h(s.handleKvLeave)).Methods("POST")

	return s, nil
}

// Run starts the http server and blocks until the context is canceled.
func (s *Server) Run(ctx context.Context, address string) error {
	srv := &http.Server{
		Addr:    address,
		Handler: s.mux,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("failed to start http server", zap.Error(err))
		}
	}()

	<-ctx.Done()

	s.logger.Warn("Shutting down server")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		s.logger.Error("server shutdown failed", zap.Error(err))
	}
	return nil
}

func (s *Server) handleErr(f func(http.ResponseWriter, *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := f(w, r)
		if err == nil {
			return
		}

		s.logger.Debug("http request failed", zap.Error(err))

		type httpError struct {
			Error string `json:"error"`
		}

		w.Header().Set("Content-Type", "application/json")

		var errStr string
		switch {
		case errors.Is(err, kvstore.ErrKeyNotFound):
			errStr = err.Error()
		case errors.Is(err, raftstore.ErrNotALeader):
			errStr = err.Error()
		case errors.Is(err, raftstore.ErrNodeNotFound):
			errStr = err.Error()
		default:
			s.logger.Warn("unhandled error", zap.Error(err))
			errStr = "something went wrong"
		}

		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(httpError{Error: errStr})
	}
}

type kvPair struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

func (s *Server) handleKvGet(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := s.store.Get([]byte(key))
	if err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(kvPair{Key: key, Value: string(value)})
}

func (s *Server) handleKvSet(w http.ResponseWriter, r *http.Request) error {
	var kv kvPair
	err := json.NewDecoder(r.Body).Decode(&kv)
	if err != nil {
		return err
	}

	err = s.store.Set([]byte(kv.Key), []byte(kv.Value))
	if err != nil {
		return err
	}

	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) handleKvDelete(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	key := vars["key"]

	err := s.store.Delete([]byte(key))
	if err != nil {
		return err
	}

	w.WriteHeader(http.StatusNoContent)
	return nil
}

type kvJoin struct {
	NodeID string `json:"nodeID"`
	Addr   string `json:"addr"`
}

func (s *Server) handleKvJoin(w http.ResponseWriter, r *http.Request) error {
	var payload kvJoin
	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		return err
	}

	err = s.store.Join(payload.NodeID, payload.Addr)
	if err != nil {
		return err
	}

	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) handleKvLeave(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	nodeID := vars["nodeID"]

	err := s.store.Leave(nodeID)
	if err != nil {
		return err
	}

	w.WriteHeader(http.StatusNoContent)
	return nil
}
