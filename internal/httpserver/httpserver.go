package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/mauri870/kvd/internal/kvstore"
	"github.com/mauri870/kvd/internal/raftstore"
)

type Server struct {
	store *raftstore.Store
	srv   *http.Server
	mux   *mux.Router
}

// New creates a new http server.
func New(store *raftstore.Store) (*Server, error) {
	s := &Server{store: store, mux: mux.NewRouter()}

	s.mux.HandleFunc("/kv/{key}", handleErr(s.handleKvGet)).Methods("GET")
	s.mux.HandleFunc("/kv", handleErr(s.handleKvSet)).Methods("POST")
	s.mux.HandleFunc("/kv/{key}", handleErr(s.handleKvDelete)).Methods("DELETE")

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
			slog.Error("failed to start http server", "error", err)
		}
	}()

	<-ctx.Done()

	slog.Warn("Shutting down server")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("server shutdown failed", "error", err)
	}
	return nil
}

func handleErr(f func(http.ResponseWriter, *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := f(w, r)
		if err == nil {
			return
		}

		slog.Debug("http request failed", "error", err)

		switch {
		case errors.Is(err, kvstore.ErrKeyNotFound):
			http.Error(w, "key not found", http.StatusNotFound)
		case errors.Is(err, raftstore.ErrNotALeader):
			http.Error(w, "not a leader", http.StatusServiceUnavailable)
		default:
			slog.Warn("unhandled error", "error", err)
			http.Error(w, "something went wrong", http.StatusInternalServerError)
		}
	}
}

type kvPair struct {
	Key   []byte `json:"key,omitempty"`
	Value []byte `json:"value,omitempty"`
}

func (s *Server) handleKvGet(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := s.store.Get([]byte(key))
	if err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(kvPair{Key: []byte(key), Value: value})
}

func (s *Server) handleKvSet(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	key := vars["key"]

	var kv kvPair
	err := json.NewDecoder(r.Body).Decode(&kv)
	if err != nil {
		return err
	}

	err = s.store.Set([]byte(key), kv.Value)
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
