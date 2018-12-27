package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

	"github.com/sirupsen/logrus"
	"goji.io/pat"
)

type Server struct {
	store *StateStore
}

func NewServer(Zks []string, logger *logrus.Logger, logSetup func(log *logrus.Logger)) *Server {
	logSetup(logger)

	return &Server{
		store: &StateStore{
			Zks:    Zks,
			logger: logger,
		},
	}
}

func (s *Server) Get(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	state, err := s.store.Get(pat.Param(r, "name"))

	switch err {
	case ErrConn, ErrRead:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, err.Error())
	case ErrNotExist:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, err.Error())
	case nil:
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		w.Write(state)
	}
}

func (s *Server) Update(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	name := pat.Param(r, "name")

	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, r.Body); err != nil {
		s.store.logger.Error("Cannot read request body")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	state := buf.Bytes()

	err := s.store.Update(name, state)
	switch err {
	case ErrConn, ErrRead, ErrCreate, ErrWrite:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, err.Error())
	case nil:
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *Server) Delete(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	err := s.store.Delete(pat.Param(r, "name"))
	switch err {
	case ErrConn, ErrNotExist, ErrDelete:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, err.Error())
	case ErrNotExist:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, err.Error())
	case nil:
		w.WriteHeader(http.StatusOK)
	}
}

func (s *Server) Lock(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	name := pat.Param(r, "name")

	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, r.Body); err != nil {
		s.store.logger.Error("Cannot read request body")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	lockinfo := buf.Bytes()

	alreadyLocked, returnedLockinfo, err := s.store.Lock(name, lockinfo)

	switch err {
	case ErrConn, ErrRead, ErrCreate:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, err.Error())
	case nil:
		if alreadyLocked {
			w.WriteHeader(http.StatusLocked)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(returnedLockinfo)
	}
}

func (s *Server) Unlock(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	err := s.store.Unlock(pat.Param(r, "name"))
	switch err {
	case ErrConn, ErrNotExist, ErrDelete:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, err.Error())
	case ErrNotExist:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, err.Error())
	case nil:
		w.WriteHeader(http.StatusOK)
	}
}
