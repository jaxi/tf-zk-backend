package main

import (
	"net/http"
	"os"
	"strings"

	"github.com/goji/httpauth"
	"github.com/sirupsen/logrus"
	"goji.io"
	"goji.io/pat"
)

func main() {
	zksStr := os.Getenv("ZKS")
	zksList := []string{}
	for _, zk := range strings.Split(zksStr, ",") {
		if zk != "" {
			zksList = append(zksList, zk)
		}
	}

	zks := make([]string, len(zksList))
	copy(zks, zksList)

	server := NewServer(zks, logrus.New(), func(log *logrus.Logger) {
		log.SetFormatter(&logrus.JSONFormatter{})
		log.SetOutput(os.Stdout)
		log.SetLevel(logrus.DebugLevel)
	})

	mux := goji.NewMux()
	mux.Use(httpauth.SimpleBasicAuth("admin", "password"))

	mux.HandleFunc(pat.Get("/:name"), server.Get)
	mux.HandleFunc(pat.Post("/:name"), server.Update)
	mux.HandleFunc(pat.Delete("/:name"), server.Delete)
	mux.HandleFunc(pat.NewWithMethods("/:name", "LOCK"), server.Lock)
	mux.HandleFunc(pat.NewWithMethods("/:name", "UNLOCK"), server.Unlock)
	http.ListenAndServe("localhost:8000", mux)
}
