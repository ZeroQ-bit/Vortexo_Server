package main

import (
	"compress/gzip"
	"net/http"
	"strconv"
	"strings"
)

// gzipMiddleware compresses text-like HTTP responses when the client advertises
// gzip support. It skips range requests so media playback and partial-content
// streams are not accidentally compressed.
func gzipMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !clientAcceptsGzip(r) || r.Method == http.MethodHead || r.Header.Get("Range") != "" {
			next.ServeHTTP(w, r)
			return
		}

		writer := &gzipResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		defer writer.Close()

		next.ServeHTTP(writer, r)
	})
}

func clientAcceptsGzip(r *http.Request) bool {
	for _, part := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		encoding, params, _ := strings.Cut(strings.TrimSpace(strings.ToLower(part)), ";")
		if encoding != "gzip" {
			continue
		}
		if strings.TrimSpace(params) == "" {
			return true
		}
		for _, param := range strings.Split(params, ";") {
			key, value, ok := strings.Cut(strings.TrimSpace(param), "=")
			if !ok || strings.TrimSpace(key) != "q" {
				continue
			}
			quality, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
			return err != nil || quality > 0
		}
		return true
	}
	return false
}

type gzipResponseWriter struct {
	http.ResponseWriter
	gzipWriter *gzip.Writer
	statusCode int
	wrote      bool
	gzip       bool
}

func (w *gzipResponseWriter) Header() http.Header {
	return w.ResponseWriter.Header()
}

func (w *gzipResponseWriter) WriteHeader(statusCode int) {
	if w.wrote {
		return
	}
	w.statusCode = statusCode
	w.wrote = true
}

func (w *gzipResponseWriter) Write(data []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}

	if !w.shouldCompress(data) {
		w.ResponseWriter.WriteHeader(w.statusCode)
		return w.ResponseWriter.Write(data)
	}

	if !w.gzip {
		w.gzip = true
		w.Header().Del("Content-Length")
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Add("Vary", "Accept-Encoding")
		w.ResponseWriter.WriteHeader(w.statusCode)
		w.gzipWriter = gzip.NewWriter(w.ResponseWriter)
	}

	return w.gzipWriter.Write(data)
}

func (w *gzipResponseWriter) Flush() {
	if w.gzipWriter != nil {
		_ = w.gzipWriter.Flush()
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *gzipResponseWriter) Close() {
	if w.gzipWriter != nil {
		_ = w.gzipWriter.Close()
	}
}

func (w *gzipResponseWriter) shouldCompress(data []byte) bool {
	if w.statusCode < http.StatusOK || w.statusCode == http.StatusNoContent || w.statusCode == http.StatusNotModified {
		return false
	}
	if w.Header().Get("Content-Encoding") != "" {
		return false
	}
	contentType := w.Header().Get("Content-Type")
	if contentType == "" {
		contentType = http.DetectContentType(data)
		w.Header().Set("Content-Type", contentType)
	}
	return isCompressibleContentType(contentType)
}

func isCompressibleContentType(contentType string) bool {
	contentType = strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
	if strings.HasPrefix(contentType, "text/") {
		return true
	}
	switch contentType {
	case "application/json",
		"application/javascript",
		"application/x-javascript",
		"application/xml",
		"application/rss+xml",
		"application/atom+xml",
		"image/svg+xml":
		return true
	default:
		return false
	}
}
