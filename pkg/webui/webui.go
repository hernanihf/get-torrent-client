package webui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed assets/*
var assetsFS embed.FS

// Handler returns an http.Handler that serves the embedded web UI assets.
func Handler() http.Handler {
	sub, err := fs.Sub(assetsFS, "assets")
	if err != nil {
		panic(err)
	}
	return http.FileServer(http.FS(sub))
}

// IndexHTML returns the raw bytes of the embedded index.html file.
func IndexHTML() ([]byte, error) {
	return assetsFS.ReadFile("assets/index.html")
}
