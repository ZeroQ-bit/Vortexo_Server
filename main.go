package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultListenAddr = ":8080"
	defaultUsername   = "vortexo"
	defaultPassword   = "vortexo"
)

var srtTimestampPattern = regexp.MustCompile(`(\d{2}:\d{2}:\d{2}),(\d{3})`)

type appState struct {
	mu       sync.RWMutex
	dataDir  string
	config   bridgeConfig
	client   *http.Client
	manifest map[string]manifestCacheEntry
}

type bridgeConfig struct {
	AdminUsername string              `json:"admin_username"`
	AdminPassword string              `json:"admin_password"`
	AuthToken     string              `json:"auth_token"`
	Manifests     []installedManifest `json:"manifests"`
}

type installedManifest struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	URL       string    `json:"url"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type perfectSetupRequest struct {
	Install         bool                    `json:"install"`
	ReplaceExisting bool                    `json:"replace_existing"`
	Password        string                  `json:"password"`
	AIOMetadata     aiometadataSetupRequest `json:"aiometadata"`
	AIOStreams      aiostreamsSetupRequest  `json:"aiostreams"`
}

type aiometadataSetupRequest struct {
	Enabled         bool     `json:"enabled"`
	Instance        string   `json:"instance"`
	Instances       []string `json:"instances"`
	Language        string   `json:"language"`
	TMDBAPIKey      string   `json:"tmdb_api_key"`
	TMDBAccessToken string   `json:"tmdb_access_token"`
	TVDBAPIKey      string   `json:"tvdb_api_key"`
	GeminiAPIKey    string   `json:"gemini_api_key"`
	RPDBAPIKey      string   `json:"rpdb_api_key"`
	IncludeAdult    bool     `json:"include_adult"`
}

type aiostreamsSetupRequest struct {
	Enabled         bool     `json:"enabled"`
	Instance        string   `json:"instance"`
	Instances       []string `json:"instances"`
	DebridProvider  string   `json:"debrid_provider"`
	DebridAPIKey    string   `json:"debrid_api_key"`
	TMDBAPIKey      string   `json:"tmdb_api_key"`
	TMDBAccessToken string   `json:"tmdb_access_token"`
	TVDBAPIKey      string   `json:"tvdb_api_key"`
	RPDBAPIKey      string   `json:"rpdb_api_key"`
	Languages       []string `json:"languages"`
	TimeoutMS       int      `json:"timeout_ms"`
	IncludeP2P      bool     `json:"include_p2p"`
}

type perfectSetupResponse struct {
	OK          bool                   `json:"ok"`
	Generated   []generatedManifest    `json:"generated"`
	Installed   []installedManifest    `json:"installed"`
	Warnings    []string               `json:"warnings,omitempty"`
	Credentials perfectSetupCredential `json:"credentials"`
}

type generatedManifest struct {
	Kind        string `json:"kind"`
	Name        string `json:"name"`
	Instance    string `json:"instance"`
	ManifestURL string `json:"manifest_url"`
	UUID        string `json:"uuid,omitempty"`
}

type perfectSetupCredential struct {
	Password string `json:"password"`
}

type manifestCacheEntry struct {
	manifest stremioManifest
	baseURL  string
	expires  time.Time
}

type stremioManifest struct {
	ID          string           `json:"id"`
	Name        string           `json:"name"`
	Description string           `json:"description"`
	Resources   []any            `json:"resources"`
	Types       []string         `json:"types"`
	Catalogs    []stremioCatalog `json:"catalogs"`
}

type stremioCatalog struct {
	Type  string                `json:"type"`
	ID    string                `json:"id"`
	Name  string                `json:"name"`
	Extra []stremioCatalogExtra `json:"extra"`
}

type stremioCatalogExtra struct {
	Name       string   `json:"name"`
	IsRequired bool     `json:"isRequired"`
	Options    []string `json:"options"`
}

type stremioCatalogResponse struct {
	Metas []stremioMeta `json:"metas"`
	Items []stremioMeta `json:"items"`
}

type stremioMetaResponse struct {
	Meta  stremioMeta   `json:"meta"`
	Metas []stremioMeta `json:"metas"`
	Items []stremioMeta `json:"items"`
}

type stremioMeta struct {
	ID             string                 `json:"id"`
	Type           string                 `json:"type"`
	Name           string                 `json:"name"`
	Title          string                 `json:"title"`
	Description    string                 `json:"description"`
	Poster         string                 `json:"poster"`
	PosterShape    string                 `json:"posterShape"`
	Background     string                 `json:"background"`
	Logo           string                 `json:"logo"`
	ReleaseInfo    string                 `json:"releaseInfo"`
	Year           any                    `json:"year"`
	Genres         []string               `json:"genres"`
	IMDBRating     string                 `json:"imdbRating"`
	Runtime        string                 `json:"runtime"`
	TMDBID         any                    `json:"tmdb_id"`
	IMDBID         string                 `json:"imdb_id"`
	OriginalTitle  string                 `json:"originalTitle"`
	OriginalName   string                 `json:"originalName"`
	Released       string                 `json:"released"`
	FirstAired     string                 `json:"firstAired"`
	BehaviorHints  any                    `json:"behaviorHints"`
	Trailers       []stremioTrailer       `json:"trailers"`
	TrailerStreams []stremioTrailerStream `json:"trailerStreams"`
	Videos         []stremioVideo         `json:"videos"`
}

type stremioTrailer struct {
	Source string `json:"source"`
	Type   string `json:"type"`
}

type stremioTrailerStream struct {
	Title     string `json:"title"`
	YTID      string `json:"ytId"`
	YouTubeID string `json:"youtubeId"`
	URL       string `json:"url"`
}

type stremioVideo struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Name        string `json:"name"`
	Overview    string `json:"overview"`
	Description string `json:"description"`
	Thumbnail   string `json:"thumbnail"`
	Poster      string `json:"poster"`
	Released    string `json:"released"`
	FirstAired  string `json:"firstAired"`
	Runtime     string `json:"runtime"`
	TMDBID      any    `json:"tmdb_id"`
	Season      any    `json:"season"`
	Episode     any    `json:"episode"`
}

type stremioStreamResponse struct {
	Streams []stremioStream `json:"streams"`
}

type stremioSubtitleResponse struct {
	Subtitles []stremioSubtitle `json:"subtitles"`
	Subs      []stremioSubtitle `json:"subs"`
}

type stremioSubtitle struct {
	ID       string `json:"id"`
	URL      string `json:"url"`
	Lang     string `json:"lang"`
	Language string `json:"language"`
	Name     string `json:"name"`
	Title    string `json:"title"`
}

type stremioStream struct {
	Name          string              `json:"name"`
	Title         string              `json:"title"`
	Description   string              `json:"description"`
	URL           string              `json:"url"`
	ExternalURL   string              `json:"externalUrl"`
	InfoHash      string              `json:"infoHash"`
	FileIdx       int                 `json:"fileIdx"`
	BehaviorHints streamBehaviorHints `json:"behaviorHints"`
}

type streamBehaviorHints struct {
	Filename  string            `json:"filename"`
	VideoSize int64             `json:"videoSize"`
	Headers   map[string]string `json:"proxyHeaders"`
}

type vortexoHomeFeed struct {
	GeneratedAt  time.Time        `json:"generated_at"`
	RefreshAfter time.Time        `json:"refresh_after"`
	Rows         []vortexoHomeRow `json:"rows"`
}

type vortexoHomeRow struct {
	ID           string            `json:"id"`
	Title        string            `json:"title"`
	Reason       string            `json:"reason,omitempty"`
	RefreshAfter time.Time         `json:"refresh_after"`
	Items        []vortexoHomeItem `json:"items"`
}

type vortexoHomeItem struct {
	ID               string   `json:"id"`
	RatingKey        string   `json:"rating_key,omitempty"`
	Key              string   `json:"key,omitempty"`
	GUID             string   `json:"guid,omitempty"`
	MediaType        string   `json:"media_type"`
	TMDBID           int      `json:"tmdb_id,omitempty"`
	IMDBID           string   `json:"imdb_id,omitempty"`
	Title            string   `json:"title"`
	OriginalTitle    string   `json:"original_title,omitempty"`
	Overview         string   `json:"overview,omitempty"`
	PosterPath       string   `json:"poster_path,omitempty"`
	BackdropPath     string   `json:"backdrop_path,omitempty"`
	LandscapePath    string   `json:"landscape_path,omitempty"`
	LogoPath         string   `json:"logo_path,omitempty"`
	OriginalLanguage string   `json:"original_language,omitempty"`
	Keywords         []string `json:"keywords,omitempty"`
	Year             int      `json:"year,omitempty"`
	Runtime          int      `json:"runtime,omitempty"`
	Genres           []string `json:"genres,omitempty"`
	VoteAverage      float64  `json:"vote_average,omitempty"`
	ReleaseDate      string   `json:"release_date,omitempty"`
	FirstAirDate     string   `json:"first_air_date,omitempty"`
	AddedAt          int64    `json:"added_at,omitempty"`
	UpdatedAt        int64    `json:"updated_at,omitempty"`
}

type vortexoManifestDetail struct {
	vortexoHomeItem
	NumberOfSeasons  int                      `json:"number_of_seasons,omitempty"`
	NumberOfEpisodes int                      `json:"number_of_episodes,omitempty"`
	Metadata         *vortexoManifestMetadata `json:"metadata,omitempty"`
}

type vortexoManifestMetadata struct {
	IMDBID           string   `json:"imdb_id,omitempty"`
	ReleaseDate      string   `json:"release_date,omitempty"`
	FirstAirDate     string   `json:"first_air_date,omitempty"`
	OriginalLanguage string   `json:"original_language,omitempty"`
	NumberOfSeasons  int      `json:"number_of_seasons,omitempty"`
	NumberOfEpisodes int      `json:"number_of_episodes,omitempty"`
	Status           string   `json:"status,omitempty"`
	Tagline          string   `json:"tagline,omitempty"`
	OriginCountry    []string `json:"origin_country,omitempty"`
}

type vortexoManifestEpisode struct {
	ID            string  `json:"id"`
	TMDBID        int     `json:"tmdb_id,omitempty"`
	Title         string  `json:"title"`
	Overview      string  `json:"overview,omitempty"`
	StillPath     string  `json:"still_path,omitempty"`
	SeasonNumber  int     `json:"season_number"`
	EpisodeNumber int     `json:"episode_number,omitempty"`
	Runtime       int     `json:"runtime,omitempty"`
	AirDate       string  `json:"air_date,omitempty"`
	VoteAverage   float64 `json:"vote_average,omitempty"`
	AddedAt       int64   `json:"added_at,omitempty"`
	UpdatedAt     int64   `json:"updated_at,omitempty"`
}

type vortexoSourcesRequest struct {
	Type        string `json:"type"`
	Title       string `json:"title"`
	Year        int    `json:"year,omitempty"`
	TMDBID      int    `json:"tmdb_id,omitempty"`
	IMDBID      string `json:"imdb_id,omitempty"`
	Season      int    `json:"season,omitempty"`
	Episode     int    `json:"episode,omitempty"`
	ParentTitle string `json:"parent_title,omitempty"`
}

type vortexoSourcesResponse struct {
	Matched   bool            `json:"matched"`
	Available bool            `json:"available"`
	Sources   []vortexoSource `json:"sources"`
}

type vortexoSource struct {
	ID           string  `json:"id"`
	Label        string  `json:"label"`
	Title        string  `json:"title,omitempty"`
	Quality      string  `json:"quality,omitempty"`
	Cached       bool    `json:"cached"`
	HDR          bool    `json:"hdr"`
	DynamicRange string  `json:"dynamic_range,omitempty"`
	Codec        string  `json:"codec,omitempty"`
	Audio        string  `json:"audio,omitempty"`
	Source       string  `json:"source,omitempty"`
	SizeGB       float64 `json:"size_gb,omitempty"`
	FileName     string  `json:"file_name,omitempty"`
	Season       int     `json:"season,omitempty"`
	Episode      int     `json:"episode,omitempty"`
	Priority     int     `json:"priority,omitempty"`
	PlayURL      string  `json:"play_url"`
}

type playToken struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
	Title   string            `json:"title,omitempty"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dataDir := firstNonEmpty(os.Getenv("VORTEXO_DATA_DIR"), os.Getenv("DATA_DIR"), "/data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}

	state := &appState{
		dataDir:  dataDir,
		client:   &http.Client{Timeout: 20 * time.Second},
		manifest: map[string]manifestCacheEntry{},
	}
	if err := state.load(); err != nil {
		return err
	}

	mux := http.NewServeMux()
	state.registerRoutes(mux)

	addr := firstNonEmpty(os.Getenv("VORTEXO_LISTEN_ADDR"), os.Getenv("PORT"), defaultListenAddr)
	if !strings.HasPrefix(addr, ":") && !strings.Contains(addr, ":") {
		addr = ":" + addr
	}
	log.Printf("Vortexo Manifest Server listening on %s", addr)
	return http.ListenAndServe(addr, state.withCORS(mux))
}

func (s *appState) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/v1/health", s.handleHealth)
	mux.HandleFunc("/api/v1/auth/status", s.handleAuthStatus)
	mux.HandleFunc("/api/v1/auth/login", s.handleLogin)
	mux.HandleFunc("/api/v1/auth/verify", s.requireAuth(s.handleVerify))
	mux.HandleFunc("/api/v1/settings", s.handleSettings)
	mux.HandleFunc("/api/v1/bridge/perfect-setup", s.requireAuth(s.handlePerfectSetup))
	mux.HandleFunc("/api/v1/bridge/manifests", s.requireAuth(s.handleManifests))
	mux.HandleFunc("/api/v1/bridge/manifests/", s.requireAuth(s.handleManifestByID))
	mux.HandleFunc("/api/v1/movies", s.handleMovies)
	mux.HandleFunc("/api/v1/movies/", s.handleMovieByID)
	mux.HandleFunc("/api/v1/series", s.handleSeries)
	mux.HandleFunc("/api/v1/series/", s.handleSeriesByID)
	mux.HandleFunc("/api/v1/search", s.handleSearch)
	mux.HandleFunc("/api/v1/stats", s.handleStats)
	mux.HandleFunc("/api/v1/channels", s.handleEmptyList)
	mux.HandleFunc("/api/v1/channels/", s.handleEmptyList)
	mux.HandleFunc("/api/v1/discover/trending", s.handleDiscoverList)
	mux.HandleFunc("/api/v1/discover/popular", s.handleDiscoverList)
	mux.HandleFunc("/api/v1/vortexo/capabilities", s.handleCapabilities)
	mux.HandleFunc("/api/v1/vortexo/home", s.handleVortexoHome)
	mux.HandleFunc("/api/v1/vortexo/sources", s.handleVortexoSources)
	mux.HandleFunc("/api/v1/vortexo/play/", s.handleVortexoPlay)
	mux.HandleFunc("/api/v1/vortexo/subtitles/", s.handleVortexoSubtitles)
	mux.HandleFunc("/player_api.php", s.handleXtreamPlayerAPI)
}

func (s *appState) load() error {
	path := filepath.Join(s.dataDir, "config.json")
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &s.config); err != nil {
			return err
		}
	}

	changed := false
	if s.config.AdminUsername == "" {
		s.config.AdminUsername = firstNonEmpty(os.Getenv("VORTEXO_ADMIN_USERNAME"), defaultUsername)
		changed = true
	}
	if s.config.AdminPassword == "" {
		s.config.AdminPassword = firstNonEmpty(os.Getenv("VORTEXO_ADMIN_PASSWORD"), defaultPassword)
		changed = true
	}
	if s.config.AuthToken == "" {
		s.config.AuthToken = randomToken()
		changed = true
	}
	if s.config.Manifests == nil {
		s.config.Manifests = []installedManifest{}
		changed = true
	}
	for i := range s.config.Manifests {
		normalized := normalizeManifestURL(s.config.Manifests[i].URL)
		if normalized != "" && normalized != s.config.Manifests[i].URL {
			s.config.Manifests[i].URL = normalized
			s.config.Manifests[i].UpdatedAt = time.Now().UTC()
			changed = true
		}
	}
	if changed {
		return s.saveLocked()
	}
	return nil
}

func (s *appState) saveLocked() error {
	data, err := json.MarshalIndent(s.config, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(s.dataDir, "config.json"), data, 0o600)
}

func (s *appState) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *appState) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			respondError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		next(w, r)
	}
}

func (s *appState) authorized(r *http.Request) bool {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		token := strings.TrimSpace(auth[7:])
		s.mu.RLock()
		ok := token != "" && token == s.config.AuthToken
		s.mu.RUnlock()
		return ok
	}
	user, pass, ok := r.BasicAuth()
	if ok {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return user == s.config.AdminUsername && pass == s.config.AdminPassword
	}
	return false
}

func (s *appState) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, indexHTML)
}

func (s *appState) handleHealth(w http.ResponseWriter, _ *http.Request) {
	respondJSON(w, http.StatusOK, map[string]any{"ok": true, "name": "Vortexo Manifest Server"})
}

func (s *appState) handleAuthStatus(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	count := 0
	if s.config.AdminUsername != "" {
		count = 1
	}
	s.mu.RUnlock()
	respondJSON(w, http.StatusOK, map[string]any{"setupRequired": false, "userCount": count})
}

func (s *appState) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	s.mu.RLock()
	ok := req.Username == s.config.AdminUsername && req.Password == s.config.AdminPassword
	token := s.config.AuthToken
	s.mu.RUnlock()
	if !ok {
		respondError(w, http.StatusUnauthorized, "invalid username or password")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{"token": token, "access_token": token})
}

func (s *appState) handleVerify(w http.ResponseWriter, _ *http.Request) {
	respondJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *appState) handleSettings(w http.ResponseWriter, _ *http.Request) {
	respondJSON(w, http.StatusOK, map[string]any{
		"opensubtitles_languages": "en",
		"manifest_bridge":         true,
	})
}

func (s *appState) handleCapabilities(w http.ResponseWriter, _ *http.Request) {
	respondJSON(w, http.StatusOK, map[string]any{
		"name":                 "Vortexo Manifest Server",
		"home":                 true,
		"source_api":           true,
		"playback":             true,
		"metadata":             true,
		"seasons":              true,
		"episodes":             true,
		"manifest_bridge":      true,
		"requires_app_changes": false,
		"types":                []string{"movie", "show", "season", "episode"},
	})
}

func (s *appState) handlePerfectSetup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req perfectSetupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if !req.AIOMetadata.Enabled && !req.AIOStreams.Enabled {
		req.AIOMetadata.Enabled = true
		req.AIOStreams.Enabled = true
	}
	if req.Password == "" {
		req.Password = randomSetupPassword()
	}
	install := req.Install
	if !req.Install {
		install = true
	}

	var generated []generatedManifest
	var warnings []string
	if req.AIOMetadata.Enabled {
		result, tried, err := s.createAIOMetadataSetup(r.Context(), req.AIOMetadata, req.Password)
		warnings = append(warnings, tried...)
		if err != nil {
			respondError(w, http.StatusBadGateway, "AIOMetadata setup failed: "+err.Error()+" "+strings.Join(tried, " "))
			return
		}
		generated = append(generated, result)
	}
	if req.AIOStreams.Enabled {
		result, tried, err := s.createAIOStreamsSetup(r.Context(), req.AIOStreams, req.Password)
		warnings = append(warnings, tried...)
		if err != nil {
			respondError(w, http.StatusBadGateway, "AIOStreams setup failed: "+err.Error()+" "+strings.Join(tried, " "))
			return
		}
		generated = append(generated, result)
	}

	var installed []installedManifest
	if install {
		if req.ReplaceExisting {
			s.removeGeneratedManifests()
		}
		for _, item := range generated {
			manifest, err := s.installManifest(r.Context(), installedManifest{
				ID:      "vortexo-" + item.Kind,
				Name:    item.Name,
				URL:     item.ManifestURL,
				Enabled: true,
			})
			if err != nil {
				respondError(w, http.StatusBadGateway, "generated manifest install failed: "+err.Error())
				return
			}
			installed = append(installed, manifest)
		}
	}

	respondJSON(w, http.StatusOK, perfectSetupResponse{
		OK:        true,
		Generated: generated,
		Installed: installed,
		Warnings:  warnings,
		Credentials: perfectSetupCredential{
			Password: req.Password,
		},
	})
}

func (s *appState) handleManifests(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.mu.RLock()
		items := append([]installedManifest(nil), s.config.Manifests...)
		s.mu.RUnlock()
		if items == nil {
			items = []installedManifest{}
		}
		respondJSON(w, http.StatusOK, map[string]any{"manifests": items})
	case http.MethodPost:
		var req installedManifest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		manifest, err := s.installManifest(r.Context(), req)
		if err != nil {
			respondError(w, http.StatusBadGateway, err.Error())
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"manifest": manifest})
	default:
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *appState) handleManifestByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/bridge/manifests/")
	id = strings.Trim(id, "/")
	if id == "" {
		respondError(w, http.StatusBadRequest, "missing manifest id")
		return
	}
	switch r.Method {
	case http.MethodDelete:
		s.mu.Lock()
		next := s.config.Manifests[:0]
		found := false
		for _, item := range s.config.Manifests {
			if item.ID == id {
				found = true
				continue
			}
			next = append(next, item)
		}
		s.config.Manifests = next
		err := s.saveLocked()
		s.mu.Unlock()
		if err != nil {
			respondError(w, http.StatusInternalServerError, "failed to save manifest")
			return
		}
		if !found {
			respondError(w, http.StatusNotFound, "manifest not found")
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"ok": true})
	default:
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *appState) installManifest(ctx context.Context, req installedManifest) (installedManifest, error) {
	req.URL = normalizeManifestURL(req.URL)
	if req.URL == "" {
		return installedManifest{}, fmt.Errorf("manifest URL is required")
	}
	manifest, _, err := s.fetchManifest(ctx, req.URL, true)
	if err != nil {
		return installedManifest{}, fmt.Errorf("manifest validation failed: %w", err)
	}
	now := time.Now().UTC()
	if req.ID == "" {
		req.ID = slug(firstNonEmpty(req.Name, manifest.Name, req.URL))
	}
	if req.Name == "" {
		req.Name = firstNonEmpty(manifest.Name, req.ID)
	}
	req.Enabled = true
	req.CreatedAt = now
	req.UpdatedAt = now

	s.mu.Lock()
	replaced := false
	for i := range s.config.Manifests {
		if s.config.Manifests[i].ID == req.ID || strings.EqualFold(s.config.Manifests[i].URL, req.URL) {
			req.CreatedAt = s.config.Manifests[i].CreatedAt
			s.config.Manifests[i] = req
			replaced = true
			break
		}
	}
	if !replaced {
		s.config.Manifests = append(s.config.Manifests, req)
	}
	err = s.saveLocked()
	s.mu.Unlock()
	if err != nil {
		return installedManifest{}, fmt.Errorf("failed to save manifest: %w", err)
	}
	return req, nil
}

func (s *appState) removeGeneratedManifests() {
	s.mu.Lock()
	next := s.config.Manifests[:0]
	for _, item := range s.config.Manifests {
		if item.ID == "vortexo-aiometadata" || item.ID == "vortexo-aiostreams" {
			continue
		}
		next = append(next, item)
	}
	s.config.Manifests = next
	_ = s.saveLocked()
	s.mu.Unlock()
}

func (s *appState) handleMovies(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	limit := boundedInt(r.URL.Query().Get("limit"), 200, 1, 500)
	offset := boundedInt(r.URL.Query().Get("offset"), 0, 0, 10_000)
	respondJSON(w, http.StatusOK, map[string]any{
		"movies": s.collectManifestItems(r.Context(), "movie", limit, offset),
	})
}

func (s *appState) handleSeries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	limit := boundedInt(r.URL.Query().Get("limit"), 200, 1, 500)
	offset := boundedInt(r.URL.Query().Get("offset"), 0, 0, 10_000)
	respondJSON(w, http.StatusOK, map[string]any{
		"series": s.collectManifestItems(r.Context(), "tv", limit, offset),
	})
}

func (s *appState) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	query := strings.TrimSpace(firstNonEmpty(r.URL.Query().Get("query"), r.URL.Query().Get("q")))
	if len(query) < 2 {
		respondJSON(w, http.StatusOK, map[string]any{"items": []vortexoHomeItem{}, "results": []vortexoHomeItem{}})
		return
	}
	limit := boundedInt(r.URL.Query().Get("limit"), 60, 1, 100)
	mediaType := normalizeCatalogType(r.URL.Query().Get("media_type"))
	items := s.searchManifestItems(r.Context(), query, mediaType, limit)
	respondJSON(w, http.StatusOK, map[string]any{"items": items, "results": items})
}

func (s *appState) handleDiscoverList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{"items": []any{}})
}

func (s *appState) handleEmptyList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{"items": []any{}, "channels": []any{}})
}

func (s *appState) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"total_movies":   0,
		"total_series":   0,
		"total_episodes": 0,
	})
}

func (s *appState) handleVortexoHome(w http.ResponseWriter, r *http.Request) {
	rowLimit := boundedInt(r.URL.Query().Get("row_limit"), 8, 1, 12)
	itemLimit := boundedInt(r.URL.Query().Get("item_limit"), 30, 6, 50)

	installed := s.enabledManifests()
	rows := make([]vortexoHomeRow, 0, rowLimit)
	used := map[string]bool{}
	now := time.Now().UTC()

	for _, item := range installed {
		if len(rows) >= rowLimit {
			break
		}
		manifest, base, err := s.fetchManifest(r.Context(), item.URL, false)
		if err != nil {
			log.Printf("home manifest %s failed: %v", item.URL, err)
			continue
		}
		for _, catalog := range manifest.Catalogs {
			if len(rows) >= rowLimit {
				break
			}
			mediaType := normalizeCatalogType(catalog.Type)
			if mediaType == "" {
				continue
			}
			items, err := s.fetchCatalog(r.Context(), base, catalog, itemLimit*2)
			if err != nil {
				log.Printf("catalog %s/%s failed: %v", catalog.Type, catalog.ID, err)
				continue
			}
			rowItems := make([]vortexoHomeItem, 0, itemLimit)
			for _, meta := range items {
				homeItem := homeItemFromStremio(meta, mediaType)
				key := homeDedupeKey(homeItem)
				if key == "" || used[key] {
					continue
				}
				used[key] = true
				rowItems = append(rowItems, homeItem)
				if len(rowItems) >= itemLimit {
					break
				}
			}
			if len(rowItems) == 0 {
				continue
			}
			title := firstNonEmpty(catalog.Name, item.Name, manifest.Name, "Recommended")
			rows = append(rows, vortexoHomeRow{
				ID:           slug(item.ID + "-" + catalog.Type + "-" + catalog.ID),
				Title:        title,
				Reason:       "Installed manifest catalog",
				RefreshAfter: now.Add(time.Hour),
				Items:        rowItems,
			})
		}
	}

	respondJSON(w, http.StatusOK, vortexoHomeFeed{
		GeneratedAt:  now,
		RefreshAfter: now.Add(time.Hour),
		Rows:         rows,
	})
}

func (s *appState) collectManifestItems(ctx context.Context, mediaType string, limit int, offset int) []vortexoHomeItem {
	installed := s.enabledManifests()
	seen := map[string]bool{}
	collected := make([]vortexoHomeItem, 0, limit)
	skip := offset

	for _, item := range installed {
		if len(collected) >= limit {
			break
		}
		manifest, base, err := s.fetchManifest(ctx, item.URL, false)
		if err != nil {
			log.Printf("library manifest %s failed: %v", item.URL, err)
			continue
		}
		for _, catalog := range manifest.Catalogs {
			if len(collected) >= limit {
				break
			}
			if normalizeCatalogType(catalog.Type) != mediaType {
				continue
			}
			items, err := s.fetchCatalog(ctx, base, catalog, limit+offset+25)
			if err != nil {
				log.Printf("library catalog %s/%s failed: %v", catalog.Type, catalog.ID, err)
				continue
			}
			for _, meta := range items {
				homeItem := homeItemFromStremio(meta, mediaType)
				key := homeDedupeKey(homeItem)
				if key == "" || seen[key] {
					continue
				}
				seen[key] = true
				if skip > 0 {
					skip--
					continue
				}
				collected = append(collected, homeItem)
				if len(collected) >= limit {
					break
				}
			}
		}
	}

	return collected
}

func (s *appState) searchManifestItems(ctx context.Context, query string, mediaType string, limit int) []vortexoHomeItem {
	installed := s.enabledManifests()
	seen := map[string]bool{}
	collected := make([]vortexoHomeItem, 0, limit)
	normalizedQuery := strings.ToLower(strings.TrimSpace(query))

	for _, item := range installed {
		if len(collected) >= limit {
			break
		}
		manifest, base, err := s.fetchManifest(ctx, item.URL, false)
		if err != nil {
			log.Printf("search manifest %s failed: %v", item.URL, err)
			continue
		}
		if !manifestSupportsResource(manifest, "catalog") {
			continue
		}
		for _, catalog := range manifest.Catalogs {
			if len(collected) >= limit {
				break
			}
			catalogType := normalizeCatalogType(catalog.Type)
			if catalogType == "" || (mediaType != "" && catalogType != mediaType) {
				continue
			}
			if !catalogSupportsSearch(catalog) {
				continue
			}
			items, err := s.fetchCatalogSearch(ctx, base, catalog, query, limit*2)
			if err != nil {
				log.Printf("search catalog %s/%s failed: %v", catalog.Type, catalog.ID, err)
				continue
			}
			for _, meta := range items {
				homeItem := homeItemFromStremio(meta, catalogType)
				if !homeItemMatchesSearch(homeItem, normalizedQuery) {
					continue
				}
				key := homeDedupeKey(homeItem)
				if key == "" || seen[key] {
					continue
				}
				seen[key] = true
				collected = append(collected, homeItem)
				if len(collected) >= limit {
					break
				}
			}
		}
	}

	return collected
}

func (s *appState) handleMovieByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	id, tail, ok := splitAPIIDTail(r.URL.Path, "/api/v1/movies/")
	if !ok || id == "" {
		respondError(w, http.StatusNotFound, "movie not found")
		return
	}
	if tail == "videos" {
		videos, err := s.findManifestVideos(r.Context(), "movie", id)
		if err != nil {
			respondError(w, http.StatusNotFound, err.Error())
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"videos": videos})
		return
	}

	meta, err := s.findManifestMeta(r.Context(), "movie", id)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	if tail != "" {
		respondError(w, http.StatusNotFound, "movie endpoint not found")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{"movie": manifestDetailFromStremio(meta, "movie")})
}

func (s *appState) handleSeriesByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	id, tail, ok := splitAPIIDTail(r.URL.Path, "/api/v1/series/")
	if !ok || id == "" {
		respondError(w, http.StatusNotFound, "series not found")
		return
	}
	if tail == "videos" {
		videos, err := s.findManifestVideos(r.Context(), "series", id)
		if err != nil {
			respondError(w, http.StatusNotFound, err.Error())
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"videos": videos})
		return
	}

	meta, err := s.findManifestMeta(r.Context(), "series", id)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	switch tail {
	case "":
		respondJSON(w, http.StatusOK, map[string]any{"series": manifestDetailFromStremio(meta, "series")})
	case "episodes":
		respondJSON(w, http.StatusOK, map[string]any{"episodes": manifestEpisodesFromStremio(meta)})
	default:
		respondError(w, http.StatusNotFound, "series endpoint not found")
	}
}

func (s *appState) handleXtreamPlayerAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	switch r.URL.Query().Get("action") {
	case "":
		respondJSON(w, http.StatusOK, map[string]any{
			"user_info": map[string]any{
				"auth":   1,
				"status": "Active",
			},
			"server_info": map[string]any{
				"server_name": "Vortexo Manifest Server",
			},
		})
	case "get_vod_info":
		id := strings.TrimSpace(r.URL.Query().Get("vod_id"))
		meta, err := s.findManifestMeta(r.Context(), "movie", id)
		if err != nil {
			respondError(w, http.StatusNotFound, err.Error())
			return
		}
		respondJSON(w, http.StatusOK, xtreamMovieInfoFromStremio(meta))
	case "get_series_info":
		id := strings.TrimSpace(r.URL.Query().Get("series_id"))
		meta, err := s.findManifestMeta(r.Context(), "series", id)
		if err != nil {
			respondError(w, http.StatusNotFound, err.Error())
			return
		}
		respondJSON(w, http.StatusOK, xtreamSeriesInfoFromStremio(meta))
	case "get_vod_streams", "get_series", "get_vod_categories", "get_series_categories", "get_live_categories":
		respondJSON(w, http.StatusOK, []any{})
	default:
		respondJSON(w, http.StatusOK, []any{})
	}
}

func (s *appState) handleVortexoSources(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req vortexoSourcesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.Type = normalizeVortexoType(req.Type)
	if req.Type == "" {
		respondError(w, http.StatusBadRequest, "type must be movie or episode")
		return
	}
	imdbID := strings.TrimSpace(req.IMDBID)
	if imdbID == "" {
		respondJSON(w, http.StatusOK, vortexoSourcesResponse{Matched: false, Available: false, Sources: []vortexoSource{}})
		return
	}

	var all []vortexoSource
	seen := map[string]bool{}
	for _, item := range s.enabledManifests() {
		manifest, base, err := s.fetchManifest(r.Context(), item.URL, false)
		if err != nil || !manifestSupportsResource(manifest, "stream") {
			continue
		}
		streams, err := s.fetchStreams(r.Context(), base, req, imdbID)
		if err != nil {
			log.Printf("streams %s failed: %v", item.URL, err)
			continue
		}
		for _, stream := range streams {
			source, ok := vortexoSourceFromStream(stream, item.Name, req)
			if !ok {
				continue
			}
			key := firstNonEmpty(stream.URL, stream.ExternalURL, stream.InfoHash, source.FileName, source.Title)
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			all = append(all, source)
		}
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].Cached != all[j].Cached {
			return all[i].Cached
		}
		return all[i].SizeGB > all[j].SizeGB
	})
	respondJSON(w, http.StatusOK, vortexoSourcesResponse{Matched: true, Available: len(all) > 0, Sources: all})
}

func (s *appState) handleVortexoPlay(w http.ResponseWriter, r *http.Request) {
	tokenValue := strings.TrimPrefix(r.URL.Path, "/api/v1/vortexo/play/")
	tokenValue = strings.Trim(tokenValue, "/")
	var token playToken
	data, err := base64.RawURLEncoding.DecodeString(tokenValue)
	if err != nil || json.Unmarshal(data, &token) != nil || token.URL == "" {
		respondError(w, http.StatusBadRequest, "invalid source token")
		return
	}
	if wantsJSON(r) {
		respondJSON(w, http.StatusOK, map[string]any{
			"url":          token.URL,
			"stream_url":   token.URL,
			"direct_url":   token.URL,
			"download_url": token.URL,
		})
		return
	}
	http.Redirect(w, r, token.URL, http.StatusFound)
}

func (s *appState) handleVortexoSubtitles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	tokenValue, language, ok := splitSubtitlePath(r.URL.Path)
	if !ok {
		respondError(w, http.StatusBadRequest, "invalid subtitle request")
		return
	}
	var req vortexoSourcesRequest
	data, err := base64.RawURLEncoding.DecodeString(tokenValue)
	if err != nil || json.Unmarshal(data, &req) != nil {
		respondError(w, http.StatusBadRequest, "invalid subtitle token")
		return
	}
	req.Type = normalizeVortexoType(req.Type)
	if req.Type == "" {
		respondError(w, http.StatusBadRequest, "type must be movie or episode")
		return
	}
	imdbID := strings.TrimSpace(req.IMDBID)
	if imdbID == "" {
		respondError(w, http.StatusNotFound, "missing imdb id")
		return
	}

	subtitle, base, err := s.findSubtitle(r.Context(), req, imdbID, language)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	subtitleURL := absoluteURL(base, subtitle.URL)
	if subtitleURL == "" {
		respondError(w, http.StatusNotFound, "subtitle URL not found")
		return
	}
	body, err := s.fetchSubtitleBody(r.Context(), subtitleURL)
	if err != nil {
		respondError(w, http.StatusBadGateway, "subtitle download failed: "+err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/vtt; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=900")
	_, _ = w.Write(webVTTBody(body))
}

func (s *appState) enabledManifests() []installedManifest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []installedManifest
	for _, item := range s.config.Manifests {
		if item.Enabled && strings.TrimSpace(item.URL) != "" {
			out = append(out, item)
		}
	}
	return out
}

func (s *appState) fetchManifest(ctx context.Context, rawURL string, force bool) (stremioManifest, string, error) {
	rawURL = normalizeManifestURL(rawURL)
	if rawURL == "" {
		return stremioManifest{}, "", fmt.Errorf("empty manifest URL")
	}
	now := time.Now()
	s.mu.RLock()
	cached, ok := s.manifest[rawURL]
	s.mu.RUnlock()
	if ok && !force && now.Before(cached.expires) {
		return cached.manifest, cached.baseURL, nil
	}

	var manifest stremioManifest
	if err := s.getJSON(ctx, rawURL, &manifest); err != nil {
		return manifest, "", err
	}
	if manifest.Name == "" && manifest.ID == "" {
		return manifest, "", fmt.Errorf("not a Stremio manifest")
	}
	base := strings.TrimSuffix(rawURL, "/manifest.json")
	base = strings.TrimRight(base, "/")
	s.mu.Lock()
	s.manifest[rawURL] = manifestCacheEntry{manifest: manifest, baseURL: base, expires: now.Add(10 * time.Minute)}
	s.mu.Unlock()
	return manifest, base, nil
}

func (s *appState) fetchCatalog(ctx context.Context, base string, catalog stremioCatalog, limit int) ([]stremioMeta, error) {
	return s.fetchCatalogExtra(ctx, base, catalog, "", limit)
}

func (s *appState) fetchCatalogSearch(ctx context.Context, base string, catalog stremioCatalog, query string, limit int) ([]stremioMeta, error) {
	return s.fetchCatalogExtra(ctx, base, catalog, "search="+query, limit)
}

func (s *appState) fetchCatalogExtra(ctx context.Context, base string, catalog stremioCatalog, extra string, limit int) ([]stremioMeta, error) {
	path := fmt.Sprintf("%s/catalog/%s/%s", strings.TrimRight(base, "/"), url.PathEscape(catalog.Type), url.PathEscape(catalog.ID))
	if strings.TrimSpace(extra) != "" {
		path += "/" + url.PathEscape(extra)
	}
	u := path + ".json"
	var response stremioCatalogResponse
	if err := s.getJSON(ctx, u, &response); err != nil {
		return nil, err
	}
	items := response.Metas
	if len(items) == 0 {
		items = response.Items
	}
	for i := range items {
		items[i] = canonicalStremioMeta(items[i], "", catalog.Type)
	}
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

func (s *appState) findManifestMeta(ctx context.Context, mediaType, id string) (stremioMeta, error) {
	stremioType := normalizeStremioType(mediaType)
	if stremioType == "" {
		return stremioMeta{}, fmt.Errorf("unsupported media type")
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return stremioMeta{}, fmt.Errorf("missing media id")
	}

	var lastErr error
	for _, item := range s.enabledManifests() {
		manifest, base, err := s.fetchManifest(ctx, item.URL, false)
		if err != nil {
			lastErr = err
			continue
		}
		if !manifestSupportsResource(manifest, "meta") || !manifestSupportsType(manifest, stremioType) {
			continue
		}
		meta, err := s.fetchMeta(ctx, base, stremioType, id)
		if err != nil {
			lastErr = err
			continue
		}
		meta = canonicalStremioMeta(meta, id, stremioType)
		if strings.TrimSpace(meta.ID) != "" || strings.TrimSpace(meta.Name) != "" {
			return meta, nil
		}
	}

	if lastErr != nil {
		return stremioMeta{}, fmt.Errorf("manifest metadata not found: %w", lastErr)
	}
	return stremioMeta{}, fmt.Errorf("manifest metadata not found")
}

func (s *appState) findManifestVideos(ctx context.Context, mediaType, id string) ([]map[string]any, error) {
	stremioType := normalizeStremioType(mediaType)
	if stremioType == "" {
		return nil, fmt.Errorf("unsupported media type")
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, fmt.Errorf("missing media id")
	}

	var lastErr error
	foundMetadata := false
	for _, item := range s.enabledManifests() {
		manifest, base, err := s.fetchManifest(ctx, item.URL, false)
		if err != nil {
			lastErr = err
			continue
		}
		if !manifestSupportsResource(manifest, "meta") || !manifestSupportsType(manifest, stremioType) {
			continue
		}
		meta, err := s.fetchMeta(ctx, base, stremioType, id)
		if err != nil {
			lastErr = err
			continue
		}
		meta = canonicalStremioMeta(meta, id, stremioType)
		if strings.TrimSpace(meta.ID) == "" && strings.TrimSpace(meta.Name) == "" {
			continue
		}

		foundMetadata = true
		if videos := manifestVideosFromStremio(meta); len(videos) > 0 {
			return videos, nil
		}
	}

	if foundMetadata {
		return []map[string]any{}, nil
	}
	if lastErr != nil {
		return nil, fmt.Errorf("manifest metadata not found: %w", lastErr)
	}
	return nil, fmt.Errorf("manifest metadata not found")
}

func (s *appState) fetchMeta(ctx context.Context, base string, mediaType string, id string) (stremioMeta, error) {
	u := fmt.Sprintf("%s/meta/%s/%s.json", strings.TrimRight(base, "/"), url.PathEscape(mediaType), url.PathEscape(id))
	var response stremioMetaResponse
	if err := s.getJSON(ctx, u, &response); err != nil {
		return stremioMeta{}, err
	}
	if strings.TrimSpace(response.Meta.ID) != "" || strings.TrimSpace(response.Meta.Name) != "" {
		return canonicalStremioMeta(response.Meta, id, mediaType), nil
	}
	if len(response.Metas) > 0 {
		return canonicalStremioMeta(response.Metas[0], id, mediaType), nil
	}
	if len(response.Items) > 0 {
		return canonicalStremioMeta(response.Items[0], id, mediaType), nil
	}
	return stremioMeta{}, fmt.Errorf("empty meta response")
}

func (s *appState) fetchStreams(ctx context.Context, base string, req vortexoSourcesRequest, imdbID string) ([]stremioStream, error) {
	var path string
	if req.Type == "episode" {
		if req.Season <= 0 || req.Episode <= 0 {
			return nil, fmt.Errorf("season and episode are required")
		}
		path = fmt.Sprintf("stream/series/%s:%d:%d.json", url.PathEscape(imdbID), req.Season, req.Episode)
	} else {
		path = fmt.Sprintf("stream/movie/%s.json", url.PathEscape(imdbID))
	}
	u := strings.TrimRight(base, "/") + "/" + path
	var response stremioStreamResponse
	if err := s.getJSON(ctx, u, &response); err != nil {
		return nil, err
	}
	return response.Streams, nil
}

func (s *appState) findSubtitle(ctx context.Context, req vortexoSourcesRequest, imdbID string, language string) (stremioSubtitle, string, error) {
	aliases := subtitleLanguageAliasSet(language)
	var lastErr error
	for _, item := range s.enabledManifests() {
		manifest, base, err := s.fetchManifest(ctx, item.URL, false)
		if err != nil {
			lastErr = err
			continue
		}
		if !manifestSupportsResource(manifest, "subtitles") || !manifestSupportsType(manifest, normalizeStremioTypeForVortexo(req.Type)) {
			continue
		}
		subtitles, err := s.fetchSubtitles(ctx, base, req, imdbID)
		if err != nil {
			lastErr = err
			log.Printf("subtitles %s failed: %v", item.URL, err)
			continue
		}
		for _, subtitle := range subtitles {
			if subtitle.URL == "" {
				continue
			}
			if subtitleMatchesLanguage(subtitle, aliases) {
				return subtitle, base, nil
			}
		}
	}
	if lastErr != nil {
		return stremioSubtitle{}, "", fmt.Errorf("subtitle not found: %w", lastErr)
	}
	return stremioSubtitle{}, "", fmt.Errorf("subtitle not found")
}

func (s *appState) fetchSubtitles(ctx context.Context, base string, req vortexoSourcesRequest, imdbID string) ([]stremioSubtitle, error) {
	var path string
	if req.Type == "episode" {
		if req.Season <= 0 || req.Episode <= 0 {
			return nil, fmt.Errorf("season and episode are required")
		}
		path = fmt.Sprintf("subtitles/series/%s:%d:%d.json", url.PathEscape(imdbID), req.Season, req.Episode)
	} else {
		path = fmt.Sprintf("subtitles/movie/%s.json", url.PathEscape(imdbID))
	}
	u := strings.TrimRight(base, "/") + "/" + path
	var response stremioSubtitleResponse
	if err := s.getJSON(ctx, u, &response); err != nil {
		return nil, err
	}
	if len(response.Subtitles) > 0 {
		return response.Subtitles, nil
	}
	return response.Subs, nil
}

func (s *appState) fetchSubtitleBody(ctx context.Context, rawURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/vtt,application/x-subrip,text/plain,*/*")
	req.Header.Set("User-Agent", "VortexoManifestServer/1.0")
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 4*1024*1024))
}

func (s *appState) getJSON(ctx context.Context, rawURL string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "VortexoManifestServer/1.0")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		mediaType, _, _ := mime.ParseMediaType(ct)
		if mediaType != "" && !strings.Contains(mediaType, "json") && mediaType != "text/plain" {
			log.Printf("warning: %s returned content-type %s", rawURL, ct)
		}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
	if err != nil {
		return err
	}
	return json.Unmarshal(body, target)
}

func (s *appState) postJSON(ctx context.Context, rawURL string, payload any, target any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "VortexoManifestServer/1.0")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
	if err != nil {
		return err
	}
	if len(respBody) > 0 && target != nil {
		_ = json.Unmarshal(respBody, target)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		detail := responseMessage(respBody)
		if detail == "" {
			detail = string(respBody)
		}
		if len(detail) > 500 {
			detail = detail[:500]
		}
		if detail != "" {
			return fmt.Errorf("HTTP %d: %s", resp.StatusCode, detail)
		}
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

func (s *appState) createAIOMetadataSetup(ctx context.Context, req aiometadataSetupRequest, password string) (generatedManifest, []string, error) {
	instances := normalizedInstances(req.Instance, req.Instances, []string{
		"https://aiometadata.viren070.me",
		"https://aiometadatafortheweebs.midnightignite.me",
	})
	language := firstNonEmpty(req.Language, "en-US")
	rpdb := firstNonEmpty(req.RPDBAPIKey, "t0-free-rpdb")
	config := buildAIOMetadataConfig(aiometadataConfigOptions{
		Language:        language,
		TMDBAPIKey:      req.TMDBAPIKey,
		TMDBAccessToken: req.TMDBAccessToken,
		TVDBAPIKey:      req.TVDBAPIKey,
		GeminiAPIKey:    req.GeminiAPIKey,
		RPDBAPIKey:      rpdb,
		IncludeAdult:    req.IncludeAdult,
	})

	var warnings []string
	for _, instance := range instances {
		base := strings.TrimRight(instance, "/")
		var response struct {
			Success    bool   `json:"success"`
			UserUUID   string `json:"userUUID"`
			InstallURL string `json:"installUrl"`
			Message    string `json:"message"`
			Error      any    `json:"error"`
		}
		err := s.postJSON(ctx, base+"/api/config/save", map[string]any{
			"password": password,
			"config":   config,
		}, &response)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("AIOMetadata %s failed: %v", base, err))
			continue
		}
		if response.UserUUID == "" && response.InstallURL == "" {
			warnings = append(warnings, fmt.Sprintf("AIOMetadata %s returned no manifest URL", base))
			continue
		}
		manifestURL := firstNonEmpty(response.InstallURL, base+"/stremio/"+response.UserUUID+"/manifest.json")
		return generatedManifest{
			Kind:        "aiometadata",
			Name:        "AIOMetadata Catalogs",
			Instance:    base,
			ManifestURL: manifestURL,
			UUID:        response.UserUUID,
		}, warnings, nil
	}
	return generatedManifest{}, warnings, fmt.Errorf("all AIOMetadata instances failed")
}

func (s *appState) createAIOStreamsSetup(ctx context.Context, req aiostreamsSetupRequest, password string) (generatedManifest, []string, error) {
	instances := normalizedInstances(req.Instance, req.Instances, []string{
		"https://aiostreams.fortheweak.cloud",
		"https://aiostreamsfortheweebsstable.midnightignite.me",
		"https://aiostreams.viren070.me",
	})
	config, err := buildAIOStreamsConfig(req)
	if err != nil {
		return generatedManifest{}, nil, err
	}

	var warnings []string
	for _, instance := range instances {
		base := strings.TrimRight(instance, "/")
		var response struct {
			Success bool `json:"success"`
			Data    struct {
				UUID              string `json:"uuid"`
				EncryptedPassword string `json:"encryptedPassword"`
			} `json:"data"`
			Error any `json:"error"`
		}
		err := s.postJSON(ctx, base+"/api/v1/user", map[string]any{
			"password": password,
			"config":   config,
		}, &response)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("AIOStreams %s failed: %v", base, err))
			continue
		}
		if response.Data.UUID == "" || response.Data.EncryptedPassword == "" {
			warnings = append(warnings, fmt.Sprintf("AIOStreams %s returned no manifest URL", base))
			continue
		}
		manifestURL := base + "/stremio/" + response.Data.UUID + "/" + response.Data.EncryptedPassword + "/manifest.json"
		return generatedManifest{
			Kind:        "aiostreams",
			Name:        "AIOStreams Sources",
			Instance:    base,
			ManifestURL: manifestURL,
			UUID:        response.Data.UUID,
		}, warnings, nil
	}
	return generatedManifest{}, warnings, fmt.Errorf("all AIOStreams instances failed")
}

type aiometadataConfigOptions struct {
	Language        string
	TMDBAPIKey      string
	TMDBAccessToken string
	TVDBAPIKey      string
	GeminiAPIKey    string
	RPDBAPIKey      string
	IncludeAdult    bool
}

func buildAIOMetadataConfig(opts aiometadataConfigOptions) map[string]any {
	return map[string]any{
		"language":                      firstNonEmpty(opts.Language, "en-US"),
		"includeAdult":                  opts.IncludeAdult,
		"searchEnabled":                 true,
		"tvdbSeasonType":                "default",
		"usePosterProxy":                true,
		"displayAgeRating":              false,
		"showRateMeButton":              false,
		"posterRatingProvider":          "rpdb",
		"showDisabledCatalogs":          false,
		"hideUnreleasedDigital":         true,
		"hideUnreleasedDigitalSearch":   false,
		"showMetaProviderAttribution":   false,
		"enableRatingPostersForLibrary": true,
		"catalogSetupComplete":          true,
		"ageRating":                     "None",
		"castCount":                     10,
		"providers": map[string]any{
			"anime":                     "kitsu",
			"movie":                     "tmdb",
			"series":                    "tmdb",
			"anime_id_provider":         "imdb",
			"forceAnimeForDetectedImdb": true,
		},
		"artProviders": map[string]any{
			"anime":          "tvdb",
			"movie":          "tmdb",
			"series":         "tvdb",
			"englishArtOnly": false,
		},
		"mal": map[string]any{
			"skipRecap":                    true,
			"skipFiller":                   false,
			"allowEpisodeMarking":          false,
			"useImdbIdForCatalogAndSearch": true,
		},
		"sfw": true,
		"tmdb": map[string]any{
			"scrapeImdb":          true,
			"forceLatinCastNames": false,
		},
		"search": map[string]any{
			"enabled":             true,
			"providers":           []string{"tmdb", "tvdb"},
			"ai_enabled":          opts.GeminiAPIKey != "",
			"searchOrder":         []string{"movie", "series"},
			"engineEnabled":       false,
			"engineRatingPosters": true,
		},
		"apiKeys": map[string]any{
			"gemini":                 opts.GeminiAPIKey,
			"tmdb":                   opts.TMDBAPIKey,
			"tmdbAccessToken":        opts.TMDBAccessToken,
			"tvdb":                   opts.TVDBAPIKey,
			"fanart":                 "",
			"rpdb":                   firstNonEmpty(opts.RPDBAPIKey, "t0-free-rpdb"),
			"topPoster":              "",
			"mdblist":                "",
			"traktTokenId":           "",
			"simklTokenId":           "",
			"anilistTokenId":         "",
			"customDescriptionBlurb": "",
		},
		"catalogs": vortexoCatalogPreset(),
	}
}

func vortexoCatalogPreset() []map[string]any {
	base := []struct {
		ID     string
		Name   string
		Type   string
		Source string
	}{
		{"tmdb.top", "Popular Movies", "movie", "tmdb"},
		{"tmdb.trending", "Trending Movies", "movie", "tmdb"},
		{"tmdb.top_rated", "Top Rated Movies", "movie", "tmdb"},
		{"tmdb.top", "Popular TV", "series", "tmdb"},
		{"tmdb.trending", "Trending TV", "series", "tmdb"},
		{"tmdb.top_rated", "Top Rated TV", "series", "tmdb"},
		{"tmdb.discover.movie.genres.action", "Action Movies", "movie", "tmdb"},
		{"tmdb.discover.movie.genres.comedy", "Comedy Movies", "movie", "tmdb"},
		{"tmdb.discover.movie.genres.horror", "Horror Movies", "movie", "tmdb"},
		{"tmdb.discover.movie.genres.science-fiction", "Sci-Fi Movies", "movie", "tmdb"},
		{"tmdb.discover.series.genres.drama", "Drama TV", "series", "tmdb"},
		{"tmdb.discover.series.genres.documentary", "Documentary TV", "series", "tmdb"},
	}
	catalogs := make([]map[string]any, 0, len(base))
	for _, item := range base {
		catalogs = append(catalogs, map[string]any{
			"id":         item.ID,
			"name":       item.Name,
			"type":       item.Type,
			"source":     item.Source,
			"sort":       "default",
			"order":      "asc",
			"enabled":    true,
			"showInHome": true,
		})
	}
	return catalogs
}

func buildAIOStreamsConfig(req aiostreamsSetupRequest) (map[string]any, error) {
	timeout := req.TimeoutMS
	if timeout <= 0 {
		timeout = 5000
	}
	provider := strings.ToLower(strings.TrimSpace(req.DebridProvider))
	debridKey := strings.TrimSpace(req.DebridAPIKey)
	if provider != "" && provider != "none" && debridKey == "" {
		return nil, fmt.Errorf("debrid API key is required when a debrid provider is selected")
	}
	hasDebrid := provider != "" && provider != "none" && debridKey != ""
	hasTMDB := strings.TrimSpace(req.TMDBAPIKey) != "" || strings.TrimSpace(req.TMDBAccessToken) != ""
	includeP2P := req.IncludeP2P || !hasDebrid
	languages := req.Languages
	if len(languages) == 0 {
		languages = []string{"English"}
	}

	presets := []map[string]any{
		{
			"type":       "torrentio",
			"instanceId": "tio",
			"enabled":    true,
			"options": map[string]any{
				"name":                 "Torrentio",
				"timeout":              timeout,
				"resources":            []string{"stream"},
				"providers":            []string{},
				"useMultipleInstances": false,
			},
		},
		{
			"type":       "comet",
			"instanceId": "com",
			"enabled":    true,
			"options": map[string]any{
				"name":        "Comet",
				"timeout":     timeout,
				"resources":   []string{"stream"},
				"includeP2P":  includeP2P,
				"removeTrash": false,
				"mediaTypes":  []string{},
			},
		},
		{
			"type":       "meteor",
			"instanceId": "met",
			"enabled":    true,
			"options": map[string]any{
				"name":                 "Meteor",
				"timeout":              timeout,
				"resources":            []string{"stream"},
				"includeP2P":           includeP2P,
				"removeTrash":          false,
				"useMultipleInstances": false,
				"mediaTypes":           []string{},
			},
		},
	}
	if provider == "torbox" {
		presets = append([]map[string]any{{
			"type":       "torbox-search",
			"instanceId": "tbs",
			"enabled":    true,
			"options": map[string]any{
				"name":                      "TB Search",
				"timeout":                   timeout,
				"sources":                   []string{"torrent"},
				"mediaTypes":                []string{},
				"userSearchEngines":         true,
				"onlyShowUserSearchResults": false,
				"useMultipleInstances":      false,
			},
		}}, presets...)
	}

	services := []map[string]any{}
	if hasDebrid {
		services = append(services, map[string]any{
			"id":      provider,
			"enabled": true,
			"credentials": map[string]any{
				"apiKey": debridKey,
			},
		})
	}

	return map[string]any{
		"addonName":            "Vortexo Sources",
		"services":             services,
		"presets":              presets,
		"formatter":            vortexoStreamFormatter(),
		"preferredQualities":   []string{"BluRay", "BluRay REMUX", "WEB-DL", "WEBRip"},
		"preferredResolutions": []string{"2160p", "1440p", "1080p", "720p"},
		"excludedQualities":    []string{"CAM", "TS", "TC", "SCR"},
		"excludedVisualTags":   []string{"3D"},
		"preferredLanguages":   append(append([]string{}, languages...), "Original", "Dual Audio", "Multi", "Unknown"),
		"requiredLanguages":    []string{},
		"preferredVisualTags":  []string{"HDR+DV", "HDR10+", "HDR10", "DV", "HDR"},
		"preferredAudioTags":   []string{"Atmos", "DD+", "DD"},
		"preferredEncodes":     []string{"AV1", "HEVC", "AVC", "Unknown"},
		"sortCriteria": map[string]any{
			"global": []map[string]string{{"key": "cached", "direction": "desc"}},
			"cached": []map[string]string{
				{"key": "resolution", "direction": "desc"},
				{"key": "quality", "direction": "desc"},
				{"key": "language", "direction": "desc"},
				{"key": "bitrate", "direction": "desc"},
			},
			"uncached": []map[string]string{
				{"key": "resolution", "direction": "desc"},
				{"key": "quality", "direction": "desc"},
				{"key": "seeders", "direction": "desc"},
			},
		},
		"deduplicator": map[string]any{
			"enabled":       true,
			"excludeAddons": []string{},
			"keys":          []string{"filename", "infoHash", "smartDetect"},
			"cached":        "single_result",
			"uncached":      "per_service",
			"p2p":           "single_result",
		},
		"hideErrors": true,
		"preloadStreams": map[string]any{
			"enabled": true,
		},
		"titleMatching": map[string]any{
			"enabled":             hasTMDB,
			"mode":                "exact",
			"similarityThreshold": 1,
			"requestTypes":        []string{"movie", "series", "anime"},
			"addons":              []string{},
		},
		"yearMatching": map[string]any{
			"enabled":      hasTMDB,
			"strict":       true,
			"requestTypes": []string{"movie", "series", "anime"},
			"addons":       []string{},
		},
		"seasonEpisodeMatching": map[string]any{
			"enabled":      true,
			"strict":       true,
			"requestTypes": []string{"series"},
			"addons":       []string{},
		},
		"digitalReleaseFilter": map[string]any{
			"enabled":      hasTMDB,
			"requestTypes": []string{"movie", "series"},
			"tolerance":    10,
			"addons":       []string{},
		},
		"rpdbApiKey":      firstNonEmpty(req.RPDBAPIKey, "t0-free-rpdb"),
		"tmdbApiKey":      req.TMDBAPIKey,
		"tmdbAccessToken": req.TMDBAccessToken,
		"tvdbApiKey":      req.TVDBAPIKey,
		"cacheAndPlay":    map[string]any{"enabled": true, "streamTypes": []string{"usenet"}},
		"trusted":         false,
		"groups":          map[string]any{"enabled": false, "behaviour": "parallel", "addons": []string{}},
	}, nil
}

func vortexoStreamFormatter() map[string]any {
	return map[string]any{
		"id": "custom",
		"definition": map[string]any{
			"name":        `{service.cached::istrue["⚡ "||""]}{stream.resolution::exists["{stream.resolution} "||""]}{stream.quality::exists["{stream.quality} "||""]}{addon.name}`,
			"description": `{stream.filename::exists["{stream.filename}\n"||""]}{stream.size::>0["{stream.size::sbytes}\n"||""]}{stream.visualTags::exists["{stream.visualTags::join(' · ')}\n"||""]}{stream.audioTags::exists["{stream.audioTags::join(' · ')}"||""]}`,
		},
	}
}

func normalizedInstances(primary string, extra []string, defaults []string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		value = strings.TrimRight(value, "/")
		if seen[value] {
			return
		}
		seen[value] = true
		out = append(out, value)
	}
	add(primary)
	for _, value := range extra {
		add(value)
	}
	for _, value := range defaults {
		add(value)
	}
	return out
}

func responseMessage(body []byte) string {
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return ""
	}
	return findMessage(decoded)
}

func findMessage(value any) string {
	switch v := value.(type) {
	case map[string]any:
		for _, key := range []string{"message", "detail", "error"} {
			if found := findMessage(v[key]); found != "" {
				return found
			}
		}
	case []any:
		for _, item := range v {
			if found := findMessage(item); found != "" {
				return found
			}
		}
	case string:
		return v
	}
	return ""
}

func canonicalStremioMeta(meta stremioMeta, requestedID string, fallbackType string) stremioMeta {
	requestedIMDBID := imdbFromID(requestedID)
	metaIMDBID := firstNonEmpty(meta.IMDBID, imdbFromID(meta.ID), requestedIMDBID)
	if metaIMDBID != "" {
		meta.IMDBID = metaIMDBID
		meta.ID = metaIMDBID
	}
	if strings.TrimSpace(meta.Type) == "" {
		if normalized := normalizeStremioType(fallbackType); normalized != "" {
			meta.Type = normalized
		}
	}
	return meta
}

func homeItemFromStremio(meta stremioMeta, fallbackType string) vortexoHomeItem {
	mediaType := normalizeCatalogType(firstNonEmpty(meta.Type, fallbackType))
	title := firstNonEmpty(meta.Name, meta.Title, "Untitled")
	imdbID := firstNonEmpty(meta.IMDBID, imdbFromID(meta.ID))
	tmdbID := intFromAny(meta.TMDBID)
	year := intFromAny(meta.Year)
	if year == 0 {
		year = yearFromText(firstNonEmpty(meta.ReleaseInfo, meta.Released, meta.FirstAired))
	}
	vote := floatFromText(meta.IMDBRating)
	releaseDate := dateFromText(firstNonEmpty(meta.Released, meta.ReleaseInfo))
	firstAirDate := ""
	if mediaType == "tv" {
		firstAirDate = dateFromText(firstNonEmpty(meta.FirstAired, meta.ReleaseInfo))
	}
	id := firstNonEmpty(imdbID, meta.ID, slug(title+"-"+strconv.Itoa(year)))
	guid := ""
	if tmdbID > 0 {
		guid = "tmdb://" + mediaType + "/" + strconv.Itoa(tmdbID)
	} else if imdbID != "" {
		guid = "imdb://" + imdbID
	}
	ratingType := mediaType
	if ratingType == "tv" {
		ratingType = "show"
	}
	return vortexoHomeItem{
		ID:            id,
		RatingKey:     "vortexo:" + ratingType + ":" + id,
		Key:           guid,
		GUID:          guid,
		MediaType:     mediaType,
		TMDBID:        tmdbID,
		IMDBID:        imdbID,
		Title:         title,
		OriginalTitle: firstNonEmpty(meta.OriginalTitle, meta.OriginalName),
		Overview:      meta.Description,
		PosterPath:    meta.Poster,
		BackdropPath:  meta.Background,
		LandscapePath: meta.Background,
		LogoPath:      meta.Logo,
		Year:          year,
		Runtime:       runtimeMinutes(meta.Runtime),
		Genres:        meta.Genres,
		VoteAverage:   vote,
		ReleaseDate:   releaseDate,
		FirstAirDate:  firstAirDate,
		AddedAt:       time.Now().Unix(),
		UpdatedAt:     time.Now().Unix(),
	}
}

func manifestDetailFromStremio(meta stremioMeta, fallbackType string) vortexoManifestDetail {
	item := homeItemFromStremio(meta, fallbackType)
	episodes := manifestEpisodesFromStremio(meta)
	seasonSet := map[int]bool{}
	for _, episode := range episodes {
		seasonSet[episode.SeasonNumber] = true
	}
	detail := vortexoManifestDetail{
		vortexoHomeItem:  item,
		NumberOfSeasons:  len(seasonSet),
		NumberOfEpisodes: len(episodes),
		Metadata: &vortexoManifestMetadata{
			IMDBID:           item.IMDBID,
			ReleaseDate:      item.ReleaseDate,
			FirstAirDate:     item.FirstAirDate,
			OriginalLanguage: item.OriginalLanguage,
			NumberOfSeasons:  len(seasonSet),
			NumberOfEpisodes: len(episodes),
		},
	}
	return detail
}

func manifestEpisodesFromStremio(meta stremioMeta) []vortexoManifestEpisode {
	if len(meta.Videos) == 0 {
		return []vortexoManifestEpisode{}
	}

	now := time.Now().Unix()
	seriesID := firstNonEmpty(meta.IMDBID, imdbFromID(meta.ID), meta.ID, slug(firstNonEmpty(meta.Name, meta.Title)))
	defaultStill := firstNonEmpty(meta.Background, meta.Poster)
	defaultRuntime := runtimeMinutes(meta.Runtime)
	defaultVote := floatFromText(meta.IMDBRating)

	episodes := make([]vortexoManifestEpisode, 0, len(meta.Videos))
	for _, video := range meta.Videos {
		season := intFromAny(video.Season)
		episodeNumber := intFromAny(video.Episode)
		if season == 0 || episodeNumber == 0 {
			idSeason, idEpisode := seasonEpisodeFromVideoID(video.ID)
			if season == 0 {
				season = idSeason
			}
			if episodeNumber == 0 {
				episodeNumber = idEpisode
			}
		}
		if episodeNumber == 0 {
			continue
		}

		id := firstNonEmpty(video.ID, fmt.Sprintf("%s:%d:%d", seriesID, season, episodeNumber))
		runtime := runtimeMinutes(video.Runtime)
		if runtime == 0 {
			runtime = defaultRuntime
		}
		episodes = append(episodes, vortexoManifestEpisode{
			ID:            id,
			TMDBID:        intFromAny(video.TMDBID),
			Title:         firstNonEmpty(video.Title, video.Name, fmt.Sprintf("Episode %d", episodeNumber)),
			Overview:      firstNonEmpty(video.Overview, video.Description),
			StillPath:     firstNonEmpty(video.Thumbnail, video.Poster, defaultStill),
			SeasonNumber:  season,
			EpisodeNumber: episodeNumber,
			Runtime:       runtime,
			AirDate:       dateFromText(firstNonEmpty(video.Released, video.FirstAired)),
			VoteAverage:   defaultVote,
			AddedAt:       now,
			UpdatedAt:     now,
		})
	}

	sort.SliceStable(episodes, func(i, j int) bool {
		if episodes[i].SeasonNumber != episodes[j].SeasonNumber {
			return episodes[i].SeasonNumber < episodes[j].SeasonNumber
		}
		if episodes[i].EpisodeNumber != episodes[j].EpisodeNumber {
			return episodes[i].EpisodeNumber < episodes[j].EpisodeNumber
		}
		return episodes[i].Title < episodes[j].Title
	})
	return episodes
}

func manifestVideosFromStremio(meta stremioMeta) []map[string]any {
	seen := map[string]bool{}
	videos := []map[string]any{}

	add := func(source string, title string, videoType string, official bool) {
		key := youtubeVideoID(source)
		if key == "" || seen[key] {
			return
		}
		seen[key] = true
		name := firstNonEmpty(title, firstNonEmpty(meta.Name, meta.Title)+" Trailer", "Trailer")
		vType := firstNonEmpty(videoType, "Trailer")
		videos = append(videos, map[string]any{
			"id":       key,
			"key":      key,
			"name":     name,
			"site":     "YouTube",
			"type":     vType,
			"official": official,
		})
	}

	for _, trailer := range meta.Trailers {
		add(trailer.Source, "", trailer.Type, true)
	}
	for _, stream := range meta.TrailerStreams {
		add(firstNonEmpty(stream.YTID, stream.YouTubeID, stream.URL), stream.Title, "Trailer", true)
	}

	return videos
}

func xtreamMovieInfoFromStremio(meta stremioMeta) map[string]any {
	item := homeItemFromStremio(meta, "movie")
	info := map[string]any{
		"tmdb_id":       item.TMDBID,
		"imdb_id":       item.IMDBID,
		"genre":         strings.Join(item.Genres, ", "),
		"plot":          item.Overview,
		"rating":        item.VoteAverage,
		"release_date":  firstNonEmpty(item.ReleaseDate, item.FirstAirDate),
		"duration_secs": item.Runtime * 60,
		"movie_image":   item.PosterPath,
		"cover":         item.PosterPath,
		"backdrop_path": stringList(item.BackdropPath),
	}
	movieData := map[string]any{
		"name":                item.Title,
		"added":               item.AddedAt,
		"container_extension": "mp4",
	}
	if item.TMDBID > 0 {
		movieData["stream_id"] = item.TMDBID
	}
	return map[string]any{
		"info":       info,
		"movie_data": movieData,
	}
}

func xtreamSeriesInfoFromStremio(meta stremioMeta) map[string]any {
	item := homeItemFromStremio(meta, "series")
	episodes := manifestEpisodesFromStremio(meta)
	grouped := map[string][]map[string]any{}
	for _, episode := range episodes {
		seasonKey := strconv.Itoa(episode.SeasonNumber)
		grouped[seasonKey] = append(grouped[seasonKey], xtreamEpisodeFromManifest(episode, item))
	}

	info := map[string]any{
		"name":             item.Title,
		"cover":            item.PosterPath,
		"plot":             item.Overview,
		"genre":            strings.Join(item.Genres, ", "),
		"release_date":     firstNonEmpty(item.FirstAirDate, item.ReleaseDate),
		"rating":           item.VoteAverage,
		"backdrop_path":    stringList(item.BackdropPath),
		"episode_run_time": item.Runtime,
	}
	return map[string]any{
		"info":     info,
		"episodes": grouped,
	}
}

func xtreamEpisodeFromManifest(episode vortexoManifestEpisode, show vortexoHomeItem) map[string]any {
	title := firstNonEmpty(episode.Title, fmt.Sprintf("Episode %d", episode.EpisodeNumber))
	info := map[string]any{
		"tmdb_id":       episode.TMDBID,
		"name":          title,
		"air_date":      episode.AirDate,
		"cover_big":     firstNonEmpty(episode.StillPath, show.BackdropPath, show.PosterPath),
		"plot":          episode.Overview,
		"movie_image":   firstNonEmpty(episode.StillPath, show.BackdropPath, show.PosterPath),
		"duration_secs": episode.Runtime * 60,
		"rating":        episode.VoteAverage,
	}
	return map[string]any{
		"id":          episode.ID,
		"episode_num": episode.EpisodeNumber,
		"title":       title,
		"season":      episode.SeasonNumber,
		"added":       episode.AddedAt,
		"info":        info,
	}
}

func stringList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return []string{}
	}
	return []string{value}
}

func vortexoSourceFromStream(stream stremioStream, manifestName string, req vortexoSourcesRequest) (vortexoSource, bool) {
	playURL := firstNonEmpty(stream.URL, stream.ExternalURL)
	if playURL == "" {
		return vortexoSource{}, false
	}
	filename := firstNonEmpty(stream.BehaviorHints.Filename, stream.Title, stream.Name, req.Title)
	quality := extractQuality(filename + " " + stream.Name + " " + stream.Description)
	codec := extractCodec(filename + " " + stream.Name + " " + stream.Description)
	audio := extractAudio(filename + " " + stream.Name + " " + stream.Description)
	dynamicRange := extractDynamicRange(filename + " " + stream.Name + " " + stream.Description)
	sizeGB := float64(stream.BehaviorHints.VideoSize) / (1024 * 1024 * 1024)
	if sizeGB == 0 {
		sizeGB = parseSizeGB(filename + " " + stream.Description + " " + stream.Title)
	}
	tokenData, _ := json.Marshal(playToken{URL: playURL, Headers: stream.BehaviorHints.Headers, Title: filename})
	id := base64.RawURLEncoding.EncodeToString(tokenData)
	labelBits := []string{}
	for _, bit := range []string{quality, dynamicRange, codec, audio} {
		if strings.TrimSpace(bit) != "" {
			labelBits = append(labelBits, bit)
		}
	}
	if len(labelBits) == 0 {
		labelBits = append(labelBits, "Stream")
	}
	source := vortexoSource{
		ID:           id,
		Label:        strings.Join(labelBits, " • "),
		Title:        firstNonEmpty(stream.Name, manifestName),
		Quality:      quality,
		Cached:       looksCached(stream),
		HDR:          dynamicRange != "" && dynamicRange != "SDR",
		DynamicRange: dynamicRange,
		Codec:        codec,
		Audio:        audio,
		Source:       "Vortexo Server",
		SizeGB:       sizeGB,
		FileName:     filename,
		Priority:     0,
		PlayURL:      "/api/v1/vortexo/play/" + id,
	}
	if req.Type == "episode" {
		source.Season = req.Season
		source.Episode = req.Episode
	}
	return source, true
}

func manifestSupportsResource(manifest stremioManifest, wanted string) bool {
	if len(manifest.Resources) == 0 {
		return true
	}
	for _, raw := range manifest.Resources {
		switch value := raw.(type) {
		case string:
			if strings.EqualFold(value, wanted) {
				return true
			}
		case map[string]any:
			if strings.EqualFold(fmt.Sprint(value["name"]), wanted) {
				return true
			}
		}
	}
	return false
}

func manifestSupportsType(manifest stremioManifest, wanted string) bool {
	wanted = normalizeStremioType(wanted)
	if wanted == "" || len(manifest.Types) == 0 {
		return true
	}
	for _, raw := range manifest.Types {
		if normalizeStremioType(raw) == wanted {
			return true
		}
	}
	return false
}

func catalogSupportsSearch(catalog stremioCatalog) bool {
	if len(catalog.Extra) == 0 {
		return true
	}
	for _, extra := range catalog.Extra {
		if strings.EqualFold(strings.TrimSpace(extra.Name), "search") {
			return true
		}
	}
	return false
}

func normalizeManifestURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(raw), "stremio://") {
		raw = "https://" + raw[len("stremio://"):]
	}
	raw = strings.TrimRight(raw, "/")
	if !strings.HasPrefix(strings.ToLower(raw), "http://") && !strings.HasPrefix(strings.ToLower(raw), "https://") {
		raw = "https://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return ""
	}
	parsed.Host = canonicalManifestHost(parsed.Host)
	if !strings.HasSuffix(strings.ToLower(strings.TrimRight(parsed.Path, "/")), "/manifest.json") {
		parsed.Path = strings.TrimRight(parsed.Path, "/") + "/manifest.json"
	}
	return parsed.String()
}

func canonicalManifestHost(host string) string {
	switch strings.ToLower(strings.TrimSpace(host)) {
	case "cinemeta-v3.strem.io":
		return "v3-cinemeta.strem.io"
	default:
		return host
	}
}

func normalizeStremioType(value string) string {
	lower := strings.ToLower(strings.TrimSpace(value))
	switch {
	case lower == "movie", lower == "movies", strings.HasSuffix(lower, ".movie"):
		return "movie"
	case lower == "series", lower == "tv", lower == "show", lower == "shows", strings.HasSuffix(lower, ".series"):
		return "series"
	default:
		return ""
	}
}

func normalizeStremioTypeForVortexo(value string) string {
	switch normalizeVortexoType(value) {
	case "movie":
		return "movie"
	case "episode":
		return "series"
	default:
		return normalizeStremioType(value)
	}
}

func normalizeCatalogType(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "movie", "movies":
		return "movie"
	case "series", "tv", "show", "shows":
		return "tv"
	default:
		return ""
	}
}

func normalizeVortexoType(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "movie":
		return "movie"
	case "episode", "series", "show", "tv":
		return "episode"
	default:
		return ""
	}
}

func splitSubtitlePath(path string) (string, string, bool) {
	raw := strings.Trim(strings.TrimPrefix(path, "/api/v1/vortexo/subtitles/"), "/")
	if raw == "" || raw == path {
		return "", "", false
	}
	parts := strings.Split(raw, "/")
	if len(parts) != 2 {
		return "", "", false
	}
	token, err := url.PathUnescape(parts[0])
	if err != nil {
		token = parts[0]
	}
	language := strings.TrimSuffix(parts[1], ".vtt")
	language = strings.TrimSuffix(language, ".srt")
	language = strings.TrimSpace(strings.ToLower(language))
	if token == "" || language == "" {
		return "", "", false
	}
	return token, language, true
}

func subtitleLanguageAliasSet(language string) map[string]bool {
	normalized := strings.ToLower(strings.TrimSpace(language))
	aliases := []string{normalized}
	switch normalized {
	case "en", "eng", "english":
		aliases = append(aliases, "en", "eng", "english")
	case "hr", "hrv", "scr", "cro", "croatian", "hrvatski":
		aliases = append(aliases, "hr", "hrv", "scr", "cro", "croatian", "hrvatski")
	case "bs", "bos", "bosnian":
		aliases = append(aliases, "bs", "bos", "bosnian")
	case "sr", "srp", "scc", "serbian":
		aliases = append(aliases, "sr", "srp", "scc", "serbian")
	case "sl", "slv", "slovenian":
		aliases = append(aliases, "sl", "slv", "slovenian")
	case "mk", "mkd", "macedonian":
		aliases = append(aliases, "mk", "mkd", "macedonian")
	}
	out := map[string]bool{}
	for _, alias := range aliases {
		alias = strings.ToLower(strings.TrimSpace(alias))
		if alias != "" {
			out[alias] = true
		}
	}
	return out
}

func subtitleMatchesLanguage(subtitle stremioSubtitle, aliases map[string]bool) bool {
	for _, value := range []string{
		subtitle.Lang,
		subtitle.Language,
		subtitle.ID,
		subtitle.Name,
		subtitle.Title,
	} {
		normalized := strings.ToLower(strings.TrimSpace(value))
		if normalized == "" {
			continue
		}
		if aliases[normalized] {
			return true
		}
		fields := strings.FieldsFunc(normalized, func(r rune) bool {
			return r == '-' || r == '_' || r == ' ' || r == '[' || r == ']' || r == '(' || r == ')' || r == '.'
		})
		for _, field := range fields {
			if aliases[strings.TrimSpace(field)] {
				return true
			}
		}
	}
	return false
}

func absoluteURL(base string, rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return ""
	}
	parsed, err := url.Parse(rawURL)
	if err == nil && parsed.IsAbs() {
		return parsed.String()
	}
	baseURL, err := url.Parse(strings.TrimRight(base, "/") + "/")
	if err != nil {
		return rawURL
	}
	relative, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	return baseURL.ResolveReference(relative).String()
}

func webVTTBody(data []byte) []byte {
	data = bytes.TrimPrefix(data, []byte{0xEF, 0xBB, 0xBF})
	trimmed := bytes.TrimSpace(data)
	if bytes.HasPrefix(bytes.ToUpper(trimmed), []byte("WEBVTT")) {
		return trimmed
	}
	text := strings.ReplaceAll(string(trimmed), "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	text = srtTimestampPattern.ReplaceAllString(text, "$1.$2")
	return []byte("WEBVTT\n\n" + strings.TrimSpace(text) + "\n")
}

func splitAPIIDTail(path string, prefix string) (string, string, bool) {
	raw := strings.Trim(strings.TrimPrefix(path, prefix), "/")
	if raw == "" || raw == path {
		return "", "", false
	}
	parts := strings.Split(raw, "/")
	id, err := url.PathUnescape(parts[0])
	if err != nil {
		id = parts[0]
	}
	tail := ""
	if len(parts) > 1 {
		tail = strings.Join(parts[1:], "/")
	}
	return strings.TrimSpace(id), strings.Trim(tail, "/"), true
}

func homeDedupeKey(item vortexoHomeItem) string {
	if item.TMDBID > 0 {
		return item.MediaType + ":tmdb:" + strconv.Itoa(item.TMDBID)
	}
	if item.IMDBID != "" {
		return item.MediaType + ":imdb:" + strings.ToLower(item.IMDBID)
	}
	return item.MediaType + ":" + slug(item.Title+"-"+strconv.Itoa(item.Year))
}

func homeItemMatchesSearch(item vortexoHomeItem, normalizedQuery string) bool {
	normalizedQuery = strings.TrimSpace(strings.ToLower(normalizedQuery))
	if normalizedQuery == "" {
		return true
	}
	haystack := strings.ToLower(strings.Join([]string{
		item.Title,
		item.OriginalTitle,
		item.Overview,
		item.IMDBID,
		strconv.Itoa(item.Year),
		strings.Join(item.Genres, " "),
	}, " "))
	if strings.Contains(haystack, normalizedQuery) {
		return true
	}
	terms := strings.Fields(normalizedQuery)
	if len(terms) == 0 {
		return true
	}
	for _, term := range terms {
		if !strings.Contains(haystack, term) {
			return false
		}
	}
	return true
}

func randomToken() string {
	var data [32]byte
	if _, err := rand.Read(data[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return base64.RawURLEncoding.EncodeToString(data[:])
}

func randomSetupPassword() string {
	const chars = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789"
	var data [20]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "Vtx" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	var out strings.Builder
	out.WriteString("Vtx")
	for _, b := range data {
		out.WriteByte(chars[int(b)%len(chars)])
	}
	return out.String()
}

func respondJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, map[string]any{"error": message, "message": message})
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func boundedInt(raw string, fallback, min, max int) int {
	value, err := strconv.Atoi(raw)
	if err != nil {
		value = fallback
	}
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func wantsJSON(r *http.Request) bool {
	q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("format")))
	if q == "json" || q == "direct" || q == "url" {
		return true
	}
	return strings.Contains(strings.ToLower(r.Header.Get("Accept")), "application/json")
}

func slug(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var out strings.Builder
	lastDash := false
	for _, r := range value {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			out.WriteRune(r)
			lastDash = false
		} else if !lastDash {
			out.WriteRune('-')
			lastDash = true
		}
	}
	return strings.Trim(out.String(), "-")
}

func imdbFromID(value string) string {
	re := regexp.MustCompile(`tt\d{5,}`)
	return re.FindString(value)
}

func youtubeVideoID(value string) string {
	value = strings.TrimSpace(html.UnescapeString(value))
	if value == "" {
		return ""
	}
	if parsed, err := url.Parse(value); err == nil && parsed.Host != "" {
		host := strings.ToLower(parsed.Host)
		if strings.Contains(host, "youtu.be") {
			parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
			if len(parts) > 0 {
				return strings.TrimSpace(parts[0])
			}
			return ""
		}
		if strings.Contains(host, "youtube.com") {
			if id := strings.TrimSpace(parsed.Query().Get("v")); id != "" {
				return id
			}
			parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
			for i, part := range parts {
				if (part == "embed" || part == "shorts") && i+1 < len(parts) {
					return strings.TrimSpace(parts[i+1])
				}
			}
		}
	}
	return value
}

func seasonEpisodeFromVideoID(value string) (int, int) {
	parts := strings.Split(strings.TrimSpace(value), ":")
	if len(parts) < 3 {
		return 0, 0
	}
	season, _ := strconv.Atoi(parts[len(parts)-2])
	episode, _ := strconv.Atoi(parts[len(parts)-1])
	return season, episode
}

func intFromAny(value any) int {
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		n, _ := strconv.Atoi(strings.TrimSpace(v))
		return n
	default:
		return 0
	}
}

func floatFromText(value string) float64 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	f, _ := strconv.ParseFloat(value, 64)
	return f
}

func yearFromText(value string) int {
	re := regexp.MustCompile(`(19|20)\d{2}`)
	n, _ := strconv.Atoi(re.FindString(value))
	return n
}

func dateFromText(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if len(value) >= 10 && value[4:5] == "-" {
		return value[:10]
	}
	if year := yearFromText(value); year > 0 {
		return fmt.Sprintf("%04d-01-01", year)
	}
	return ""
}

func runtimeMinutes(value string) int {
	re := regexp.MustCompile(`\d+`)
	n, _ := strconv.Atoi(re.FindString(value))
	return n
}

func extractQuality(value string) string {
	lower := strings.ToLower(value)
	for _, q := range []string{"2160p", "4k", "1080p", "720p", "576p", "480p"} {
		if strings.Contains(lower, q) {
			if q == "4k" {
				return "2160p"
			}
			return q
		}
	}
	return ""
}

func extractCodec(value string) string {
	lower := strings.ToLower(value)
	for _, c := range []string{"av1", "hevc", "x265", "h265", "h.265", "x264", "h264", "h.264"} {
		if strings.Contains(lower, c) {
			switch c {
			case "x265", "h265", "h.265":
				return "HEVC"
			case "x264", "h264", "h.264":
				return "H264"
			default:
				return strings.ToUpper(c)
			}
		}
	}
	return ""
}

func extractAudio(value string) string {
	lower := strings.ToLower(value)
	for _, a := range []string{"truehd", "atmos", "dts-hd", "eac3", "ddp", "ac3", "aac"} {
		if strings.Contains(lower, a) {
			return strings.ToUpper(a)
		}
	}
	return ""
}

func extractDynamicRange(value string) string {
	lower := strings.ToLower(value)
	switch {
	case strings.Contains(lower, "dolby vision"), strings.Contains(lower, " dovi"), strings.Contains(lower, ".dovi"):
		return "DV"
	case strings.Contains(lower, "hdr10+"):
		return "HDR10+"
	case strings.Contains(lower, "hdr"):
		return "HDR"
	default:
		return ""
	}
}

func parseSizeGB(value string) float64 {
	re := regexp.MustCompile(`(?i)(\d+(?:\.\d+)?)\s*(gb|mb)`)
	match := re.FindStringSubmatch(value)
	if len(match) != 3 {
		return 0
	}
	n, _ := strconv.ParseFloat(match[1], 64)
	if strings.EqualFold(match[2], "mb") {
		return n / 1024
	}
	return n
}

func looksCached(stream stremioStream) bool {
	value := strings.ToLower(stream.Name + " " + stream.Title + " " + stream.Description)
	return strings.Contains(value, "rd+") ||
		strings.Contains(value, "cached") ||
		strings.Contains(value, "instant") ||
		strings.Contains(value, "⚡")
}

const indexHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Vortexo Manifest Server</title>
  <style>
    :root { color-scheme: dark; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #090812; color: #f7f6ff; }
    * { box-sizing: border-box; }
    body { margin: 0; min-height: 100vh; background: #090812; }
    body:before { content: ""; position: fixed; inset: 0; pointer-events: none; background: radial-gradient(circle at 18% 0%, rgba(74,125,255,.24), transparent 30rem), radial-gradient(circle at 88% 12%, rgba(124,83,255,.18), transparent 28rem); }
    button, input, select { font: inherit; }
    button { border: 0; border-radius: 10px; background: #5b8cff; color: white; padding: 12px 16px; font-weight: 800; cursor: pointer; }
    button.secondary { background: rgba(255,255,255,.1); color: #f7f6ff; }
    button.ghost { background: transparent; color: #b8c3ff; border: 1px solid rgba(184,195,255,.22); }
    button:disabled { opacity: .45; cursor: not-allowed; }
    input, select { width: 100%; border: 1px solid rgba(255,255,255,.14); border-radius: 10px; background: rgba(0,0,0,.28); color: white; padding: 12px 13px; font-size: 15px; outline: none; }
    input:focus, select:focus { border-color: rgba(124,156,255,.75); box-shadow: 0 0 0 3px rgba(91,140,255,.16); }
    label { display: block; margin: 14px 0 7px; color: #d9ddff; font-weight: 700; }
    a { color: #9eb8ff; text-decoration: none; }
    code { color: #9cdcfe; }
    .shell { position: relative; display: grid; grid-template-columns: 300px minmax(0, 1fr); min-height: 100vh; }
    .side { border-right: 1px solid rgba(255,255,255,.1); background: rgba(20,16,39,.78); padding: 28px 22px; }
    .brand { display: flex; align-items: center; gap: 12px; margin-bottom: 28px; }
    .brandMark { display: grid; place-items: center; width: 42px; height: 42px; border-radius: 12px; background: #0a7dff; font-weight: 900; }
    h1 { margin: 0; font-size: 24px; letter-spacing: 0; }
    .subtitle, .muted { color: #aeb5d8; line-height: 1.55; }
    .steps { display: grid; gap: 8px; margin: 28px 0; }
    .step { display: flex; align-items: center; gap: 10px; width: 100%; padding: 13px 12px; border-radius: 10px; color: #bdc2e8; background: transparent; text-align: left; border: 1px solid transparent; }
    .step.active { color: white; border-color: rgba(124,156,255,.65); background: rgba(124,156,255,.12); }
    .dot { width: 11px; height: 11px; border: 1px solid rgba(255,255,255,.22); border-radius: 999px; }
    .step.done .dot { background: #70e3a3; border-color: #70e3a3; }
    .fineprint { margin-top: auto; padding-top: 24px; font-size: 13px; color: #8991b6; line-height: 1.5; }
    main { padding: 38px clamp(26px, 5vw, 72px); }
    .topbar { display: flex; align-items: center; justify-content: space-between; gap: 18px; margin-bottom: 24px; }
    .statusPill { border: 1px solid rgba(255,255,255,.12); background: rgba(255,255,255,.07); padding: 9px 12px; border-radius: 999px; color: #b9c1e7; font-size: 14px; }
    .stage { max-width: 1120px; }
    .panel { background: rgba(255,255,255,.075); border: 1px solid rgba(255,255,255,.12); border-radius: 14px; padding: 24px; margin: 16px 0; }
    .hero { padding: 34px; text-align: center; }
    .hero h2 { margin: 0 0 12px; font-size: clamp(30px, 4vw, 48px); letter-spacing: 0; }
    .hero p { max-width: 760px; margin: 10px auto; color: #c4c9e7; line-height: 1.6; }
    .grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; }
    .card { border: 1px solid rgba(255,255,255,.12); border-radius: 12px; padding: 18px; background: rgba(0,0,0,.18); }
    .card h3 { margin: 0 0 7px; }
    .card p { margin: 0; color: #b7bedf; line-height: 1.5; }
    .actions { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 18px; }
    .pane { display: none; }
    .pane.active { display: block; }
    .two { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; }
    .checklist { display: grid; gap: 10px; margin-top: 14px; }
    .check { display: flex; align-items: flex-start; gap: 10px; padding: 12px; border-radius: 10px; background: rgba(255,255,255,.06); color: #d7dcfa; }
    .check input { width: auto; margin-top: 3px; }
    .ok { color: #7ee787; }
    .error { color: #ff8a8a; }
    .row { display: flex; align-items: center; justify-content: space-between; gap: 14px; padding: 14px 0; border-top: 1px solid rgba(255,255,255,.1); }
    .row:first-child { border-top: 0; }
    .url { overflow-wrap: anywhere; color: #aeb8dd; font-size: 13px; }
    .message { min-height: 22px; margin-top: 12px; }
    @media (max-width: 860px) {
      .shell { grid-template-columns: 1fr; }
      .side { position: static; border-right: 0; border-bottom: 1px solid rgba(255,255,255,.1); }
      .steps { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .grid, .two { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
<div class="shell">
  <aside class="side">
    <div class="brand">
      <div class="brandMark">V</div>
      <div>
        <h1>Vortexo Manifest Server</h1>
        <div class="subtitle">Manifest setup wizard</div>
      </div>
    </div>
    <div class="steps">
      <button class="step active" data-step="welcome" onclick="showStep('welcome')"><span class="dot"></span><span>Welcome</span></button>
      <button class="step" data-step="signin" onclick="showStep('signin')"><span class="dot"></span><span>Sign In</span></button>
      <button class="step" data-step="accounts" onclick="showStep('accounts')"><span class="dot"></span><span>Accounts</span></button>
      <button class="step" data-step="catalogs" onclick="showStep('catalogs')"><span class="dot"></span><span>Catalogs</span></button>
      <button class="step" data-step="streams" onclick="showStep('streams')"><span class="dot"></span><span>Streams</span></button>
      <button class="step" data-step="install" onclick="showStep('install')"><span class="dot"></span><span>Install</span></button>
      <button class="step" data-step="finish" onclick="showStep('finish')"><span class="dot"></span><span>Finish</span></button>
    </div>
    <div class="fineprint">
      Vortexo Manifest Server stores only installed manifest URLs. Debrid, TMDB, TVDB, Gemini, and RPDB keys stay inside the upstream addon configurations you create.
    </div>
  </aside>
  <main>
    <div class="topbar">
      <div>
        <div class="subtitle">First-run setup for Vortexo Apple TV</div>
        <h1>Build your manifest-powered server</h1>
      </div>
      <div id="authPill" class="statusPill">Signed out</div>
    </div>
    <section class="stage">
      <div id="welcome" class="pane active">
        <div class="panel hero">
          <h2>Welcome to Vortexo Manifest Server</h2>
          <p>This wizard helps you turn a clean server into a working Vortexo backend. You create or paste AIOStreams and AIOMetadata manifests, then the Apple TV app keeps using the same Vortexo Server settings you already have.</p>
          <div class="grid" style="text-align:left; margin-top:24px;">
            <div class="card">
              <h3>Catalogs</h3>
              <p>AIOMetadata-style manifests become landscape Home rows in Vortexo.</p>
            </div>
            <div class="card">
              <h3>Playback</h3>
              <p>AIOStreams-style manifests become source lookup for movies and episodes.</p>
            </div>
          </div>
          <div class="actions" style="justify-content:center;">
            <button onclick="showStep('signin')">Start Setup</button>
            <button class="secondary" onclick="showStep('install')">I already have manifest URLs</button>
          </div>
        </div>
      </div>

      <div id="signin" class="pane">
        <div class="panel">
          <h2>Sign In</h2>
          <p class="muted">Default Umbrel credentials are <code>vortexo</code> / <code>vortexo</code> unless changed by environment variables.</p>
          <div class="two">
            <div>
              <label>Username</label>
              <input id="username" value="vortexo" autocomplete="username">
            </div>
            <div>
              <label>Password</label>
              <input id="password" type="password" value="vortexo" autocomplete="current-password">
            </div>
          </div>
          <div class="actions">
            <button onclick="login()">Sign In</button>
            <button class="secondary" onclick="loadManifests()">Refresh Session</button>
          </div>
          <div id="loginStatus" class="message muted"></div>
        </div>
      </div>

      <div id="accounts" class="pane">
        <div class="panel">
          <h2>Prepare Accounts and Keys</h2>
          <p class="muted">Enter your own keys here and Vortexo Manifest Server will create AIOMetadata and AIOStreams manifests for you. Keys are sent to the selected upstream addon instance to create its normal manifest configuration; Vortexo Manifest Server stores only the returned manifest URLs.</p>
          <div class="checklist">
            <label class="check"><input type="checkbox" data-check="debrid"><span><strong>Debrid account</strong><br><span class="muted">Real-Debrid, TorBox, Premiumize, AllDebrid, or another provider supported by your AIOStreams instance.</span></span></label>
            <label class="check"><input type="checkbox" data-check="tmdb"><span><strong>TMDB key</strong><br><span class="muted">Helps metadata and catalog quality when your AIOMetadata instance requests it.</span></span></label>
            <label class="check"><input type="checkbox" data-check="tvdb"><span><strong>TVDB key</strong><br><span class="muted">Useful for TV matching and richer series metadata if your instance supports it.</span></span></label>
            <label class="check"><input type="checkbox" data-check="gemini"><span><strong>Gemini key</strong><br><span class="muted">Optional. Some AIOMetadata search or recommendations features may use it.</span></span></label>
            <label class="check"><input type="checkbox" data-check="rpdb"><span><strong>RPDB key</strong><br><span class="muted">Optional poster ratings. Many guides use the free key <code>t0-free-rpdb</code>.</span></span></label>
          </div>
          <div class="panel" style="margin-top:18px;">
            <h3>Generate Perfect Setup</h3>
            <div class="two">
              <div>
                <label>Debrid Provider</label>
                <select id="debridProvider">
                  <option value="none">None / P2P only</option>
                  <option value="realdebrid">Real-Debrid</option>
                  <option value="torbox">TorBox</option>
                  <option value="premiumize">Premiumize</option>
                  <option value="alldebrid">AllDebrid</option>
                  <option value="debridlink">Debrid-Link</option>
                  <option value="easydebrid">EasyDebrid</option>
                </select>
              </div>
              <div>
                <label>Debrid API Key</label>
                <input id="debridApiKey" type="password" placeholder="Required when a provider is selected">
              </div>
              <div>
                <label>AIOStreams Instance</label>
                <select id="aiostreamsInstance">
                  <option value="https://aiostreams.fortheweak.cloud">AIOStreams Fortheweak</option>
                  <option value="https://aiostreamsfortheweebsstable.midnightignite.me">AIOStreams Midnight</option>
                  <option value="https://aiostreams.viren070.me">AIOStreams Viren</option>
                </select>
              </div>
              <div>
                <label>AIOMetadata Instance</label>
                <select id="aiometadataInstance">
                  <option value="https://aiometadata.viren070.me">AIOMetadata Viren</option>
                  <option value="https://aiometadatafortheweebs.midnightignite.me">AIOMetadata Midnight</option>
                </select>
              </div>
              <div>
                <label>TMDB API Key</label>
                <input id="tmdbApiKey" type="password" placeholder="Optional, recommended">
              </div>
              <div>
                <label>TMDB Read Token</label>
                <input id="tmdbAccessToken" type="password" placeholder="Optional">
              </div>
              <div>
                <label>TVDB API Key</label>
                <input id="tvdbApiKey" type="password" placeholder="Optional">
              </div>
              <div>
                <label>Gemini API Key</label>
                <input id="geminiApiKey" type="password" placeholder="Optional">
              </div>
              <div>
                <label>RPDB API Key</label>
                <input id="rpdbApiKey" type="password" placeholder="Optional, defaults to t0-free-rpdb">
              </div>
              <div>
                <label>Preferred Language</label>
                <select id="preferredLanguage">
                  <option value="English">English</option>
                  <option value="Croatian">Croatian</option>
                  <option value="Arabic">Arabic</option>
                  <option value="French">French</option>
                  <option value="German">German</option>
                  <option value="Spanish">Spanish</option>
                </select>
              </div>
            </div>
            <div class="actions">
              <button onclick="generatePerfectSetup()">Generate & Install</button>
              <button class="secondary" onclick="saveChecklist(); showStep('install')">Use Manual Manifest URLs</button>
            </div>
            <div id="generateStatus" class="message muted"></div>
          </div>
          <div class="actions">
            <button onclick="saveChecklist(); showStep('catalogs')">Continue</button>
            <button class="secondary" onclick="saveChecklist()">Save Checklist</button>
          </div>
          <div id="accountStatus" class="message muted"></div>
        </div>
      </div>

      <div id="catalogs" class="pane">
        <div class="panel">
          <h2>Create Catalog Manifest</h2>
          <p class="muted">Open an AIOMetadata instance, import or create your catalog setup, save it, then copy the final manifest URL. This is what Vortexo uses for Home rows.</p>
          <div class="grid">
            <div class="card">
              <h3>AIOMetadata</h3>
              <p>Create a catalog and metadata configuration, then copy the manifest URL after saving.</p>
              <div class="actions">
                <a href="https://aiometadata.viren070.me/" target="_blank" rel="noreferrer"><button>Open Viren Instance</button></a>
                <a href="https://aiometadatafortheweebs.midnightignite.me/" target="_blank" rel="noreferrer"><button class="secondary">Open Midnight Instance</button></a>
              </div>
            </div>
            <div class="card">
              <h3>What to copy</h3>
              <p>Copy the URL ending in <code>/manifest.json</code>. Paste it in the Install step as the catalog manifest.</p>
            </div>
          </div>
          <label>Catalog manifest URL</label>
          <input id="catalogManifestUrl" placeholder="https://aiometadata.example/stremio/.../manifest.json">
          <div class="actions">
            <button onclick="saveDraft(); showStep('streams')">Continue</button>
            <button class="secondary" onclick="saveDraft()">Save Draft</button>
          </div>
        </div>
      </div>

      <div id="streams" class="pane">
        <div class="panel">
          <h2>Create Stream Manifest</h2>
          <p class="muted">Open an AIOStreams instance, configure providers and sorting, save it, then copy the final manifest URL. This is what Vortexo uses for source lookup.</p>
          <div class="grid">
            <div class="card">
              <h3>AIOStreams</h3>
              <p>Configure stream providers, debrid, timeouts, filters, and source formatting.</p>
              <div class="actions">
                <a href="https://aiostreams.viren070.me/" target="_blank" rel="noreferrer"><button>Open Viren Instance</button></a>
                <a href="https://aiostreams.elfhosted.com/" target="_blank" rel="noreferrer"><button class="secondary">Open ElfHosted Instance</button></a>
              </div>
            </div>
            <div class="card">
              <h3>Vortexo behavior</h3>
              <p>The bridge reads stream URLs from the manifest and converts them to Vortexo playback links. If an addon returns only torrent hashes, those are skipped until a debrid-backed URL is returned.</p>
            </div>
          </div>
          <label>Stream manifest URL</label>
          <input id="streamManifestUrl" placeholder="https://aiostreams.example/stremio/.../manifest.json">
          <div class="actions">
            <button onclick="saveDraft(); showStep('install')">Continue</button>
            <button class="secondary" onclick="saveDraft()">Save Draft</button>
          </div>
        </div>
      </div>

      <div id="install" class="pane">
        <div class="panel">
          <h2>Install Into Vortexo Manifest Server</h2>
          <p class="muted">Sign in first, then install one or both manifest URLs. You can come back later and replace them without changing the Apple TV app.</p>
          <div class="two">
            <div>
              <label>Catalog manifest URL</label>
              <input id="installCatalogUrl" placeholder="AIOMetadata manifest URL">
            </div>
            <div>
              <label>Stream manifest URL</label>
              <input id="installStreamUrl" placeholder="AIOStreams manifest URL">
            </div>
          </div>
          <div class="actions">
            <button onclick="installSetup()">Install Setup</button>
            <button class="secondary" onclick="loadManifests()">Refresh Installed</button>
          </div>
          <div id="manifestStatus" class="message muted"></div>
        </div>
        <div class="panel">
          <h2>Installed Manifests</h2>
          <div id="manifestList" class="muted">Sign in to view installed manifests.</div>
        </div>
        <div class="panel">
          <h2>Manual Install</h2>
          <label>Manifest URL</label>
          <input id="manifestUrl" placeholder="https://example.com/your-config/manifest.json">
          <label>Name</label>
          <input id="manifestName" placeholder="AIOStreams or AIOMetadata">
          <div class="actions">
            <button class="secondary" onclick="addManifest()">Install One Manifest</button>
          </div>
        </div>
      </div>

      <div id="finish" class="pane">
        <div class="panel hero">
          <h2>Connect Vortexo Apple TV</h2>
          <p>In Vortexo settings, enable Vortexo Server and connect to this server URL with the same username and password.</p>
          <div class="card" style="text-align:left;">
            <h3>Server URL</h3>
            <div id="serverUrl" class="url"></div>
          </div>
          <div class="actions" style="justify-content:center;">
            <button onclick="copyServerUrl()">Copy Server URL</button>
            <button class="secondary" onclick="showStep('install')">Manage Manifests</button>
          </div>
          <div id="finishStatus" class="message muted"></div>
        </div>
      </div>
    </section>
  </main>
</div>
<script>
let token = localStorage.getItem("vortexoToken") || "";
const stepOrder = ["welcome", "signin", "accounts", "catalogs", "streams", "install", "finish"];
function showStep(id) {
  document.querySelectorAll(".pane").forEach((el) => el.classList.toggle("active", el.id === id));
  document.querySelectorAll(".step").forEach((el) => el.classList.toggle("active", el.dataset.step === id));
  if (id === "install") syncInstallInputs();
  if (id === "finish") updateServerUrl();
}
function markDone(id) {
  const el = document.querySelector(".step[data-step='" + id + "']");
  if (el) el.classList.add("done");
}
async function login() {
  const res = await fetch("/api/v1/auth/login", {method:"POST", headers:{"content-type":"application/json"}, body: JSON.stringify({username: username.value, password: password.value})});
  const data = await res.json();
  if (!res.ok) { loginStatus.textContent = data.message || "Login failed"; loginStatus.className = "error"; return; }
  token = data.token || data.access_token;
  localStorage.setItem("vortexoToken", token);
  authPill.textContent = "Signed in";
  authPill.className = "statusPill ok";
  loginStatus.textContent = "Signed in";
  loginStatus.className = "ok";
  markDone("signin");
  loadManifests();
  showStep("accounts");
}
async function loadManifests() {
  if (!token) return;
  const res = await fetch("/api/v1/bridge/manifests", {headers:{authorization:"Bearer " + token}});
  if (res.status === 401 || res.status === 403) {
    token = "";
    localStorage.removeItem("vortexoToken");
    authPill.textContent = "Signed out";
    manifestList.textContent = "Sign in to view installed manifests.";
    return;
  }
  const data = await res.json();
  manifestList.innerHTML = "";
  const items = data.manifests || [];
  if (items.length === 0) {
    manifestList.textContent = "No manifests installed yet.";
    return;
  }
  items.forEach((item) => {
    const div = document.createElement("div");
    div.className = "row";
    div.innerHTML = "<div><strong>" + escapeHtml(item.name || item.id) + "</strong><div class='url'>" + escapeHtml(item.url) + "</div></div><button class='secondary' onclick='removeManifest(\"" + escapeAttr(item.id) + "\")'>Remove</button>";
    manifestList.appendChild(div);
  });
  markDone("install");
}
async function addManifest() {
  if (!token) { manifestStatus.textContent = "Sign in first."; manifestStatus.className = "error"; showStep("signin"); return; }
  manifestStatus.textContent = "Installing...";
  const res = await fetch("/api/v1/bridge/manifests", {method:"POST", headers:{"content-type":"application/json", authorization:"Bearer " + token}, body: JSON.stringify({name: manifestName.value, url: manifestUrl.value, enabled: true})});
  const data = await res.json();
  if (!res.ok) { manifestStatus.textContent = data.message || "Install failed"; manifestStatus.className = "error"; return; }
  manifestStatus.textContent = "Installed";
  manifestStatus.className = "ok";
  manifestUrl.value = "";
  manifestName.value = "";
  loadManifests();
}
async function installSetup() {
  if (!token) { manifestStatus.textContent = "Sign in first."; manifestStatus.className = "error"; showStep("signin"); return; }
  syncDraftFromInstall();
  const installs = [];
  if (installCatalogUrl.value.trim()) installs.push({name:"AIOMetadata Catalogs", url: installCatalogUrl.value.trim(), enabled:true});
  if (installStreamUrl.value.trim()) installs.push({name:"AIOStreams Sources", url: installStreamUrl.value.trim(), enabled:true});
  if (installs.length === 0) { manifestStatus.textContent = "Paste at least one manifest URL."; manifestStatus.className = "error"; return; }
  manifestStatus.textContent = "Installing " + installs.length + " manifest" + (installs.length === 1 ? "" : "s") + "...";
  manifestStatus.className = "message muted";
  for (const item of installs) {
    const res = await fetch("/api/v1/bridge/manifests", {method:"POST", headers:{"content-type":"application/json", authorization:"Bearer " + token}, body: JSON.stringify(item)});
    const data = await res.json();
    if (!res.ok) { manifestStatus.textContent = data.message || "Install failed"; manifestStatus.className = "error"; return; }
  }
  manifestStatus.textContent = "Setup installed.";
  manifestStatus.className = "ok";
  markDone("catalogs");
  markDone("streams");
  markDone("install");
  await loadManifests();
  showStep("finish");
}
async function generatePerfectSetup() {
  if (!token) { generateStatus.textContent = "Sign in first."; generateStatus.className = "error"; showStep("signin"); return; }
  generateStatus.textContent = "Creating AIOMetadata and AIOStreams manifests...";
  generateStatus.className = "message muted";
  const provider = debridProvider.value || "none";
  const payload = {
    install: true,
    replace_existing: true,
    aiometadata: {
      enabled: true,
      instance: aiometadataInstance.value,
      language: "en-US",
      tmdb_api_key: tmdbApiKey.value.trim(),
      tmdb_access_token: tmdbAccessToken.value.trim(),
      tvdb_api_key: tvdbApiKey.value.trim(),
      gemini_api_key: geminiApiKey.value.trim(),
      rpdb_api_key: rpdbApiKey.value.trim()
    },
    aiostreams: {
      enabled: true,
      instance: aiostreamsInstance.value,
      debrid_provider: provider === "none" ? "" : provider,
      debrid_api_key: debridApiKey.value.trim(),
      tmdb_api_key: tmdbApiKey.value.trim(),
      tmdb_access_token: tmdbAccessToken.value.trim(),
      tvdb_api_key: tvdbApiKey.value.trim(),
      rpdb_api_key: rpdbApiKey.value.trim(),
      languages: [preferredLanguage.value || "English"],
      timeout_ms: 5000,
      include_p2p: provider === "none"
    }
  };
  const res = await fetch("/api/v1/bridge/perfect-setup", {method:"POST", headers:{"content-type":"application/json", authorization:"Bearer " + token}, body: JSON.stringify(payload)});
  const data = await res.json();
  if (!res.ok) {
    generateStatus.textContent = data.message || "Setup failed";
    generateStatus.className = "error";
    return;
  }
  const catalog = (data.generated || []).find((item) => item.kind === "aiometadata");
  const streams = (data.generated || []).find((item) => item.kind === "aiostreams");
  if (catalog) {
    catalogManifestUrl.value = catalog.manifest_url;
    installCatalogUrl.value = catalog.manifest_url;
    localStorage.setItem("vortexoCatalogManifestUrl", catalog.manifest_url);
  }
  if (streams) {
    streamManifestUrl.value = streams.manifest_url;
    installStreamUrl.value = streams.manifest_url;
    localStorage.setItem("vortexoStreamManifestUrl", streams.manifest_url);
  }
  generateStatus.innerHTML = "Generated and installed. Addon password: <code>" + escapeHtml(data.credentials?.password || "") + "</code>";
  generateStatus.className = "message ok";
  markDone("accounts");
  markDone("catalogs");
  markDone("streams");
  markDone("install");
  await loadManifests();
  showStep("finish");
}
async function removeManifest(id) {
  await fetch("/api/v1/bridge/manifests/" + encodeURIComponent(id), {method:"DELETE", headers:{authorization:"Bearer " + token}});
  loadManifests();
}
function saveChecklist() {
  const data = {};
  document.querySelectorAll("[data-check]").forEach((el) => data[el.dataset.check] = el.checked);
  localStorage.setItem("vortexoSetupChecklist", JSON.stringify(data));
  accountStatus.textContent = "Checklist saved";
  accountStatus.className = "message ok";
  markDone("accounts");
}
function restoreChecklist() {
  try {
    const data = JSON.parse(localStorage.getItem("vortexoSetupChecklist") || "{}");
    document.querySelectorAll("[data-check]").forEach((el) => el.checked = !!data[el.dataset.check]);
  } catch {}
}
function saveDraft() {
  localStorage.setItem("vortexoCatalogManifestUrl", catalogManifestUrl.value.trim());
  localStorage.setItem("vortexoStreamManifestUrl", streamManifestUrl.value.trim());
  if (catalogManifestUrl.value.trim()) markDone("catalogs");
  if (streamManifestUrl.value.trim()) markDone("streams");
}
function restoreDraft() {
  catalogManifestUrl.value = localStorage.getItem("vortexoCatalogManifestUrl") || "";
  streamManifestUrl.value = localStorage.getItem("vortexoStreamManifestUrl") || "";
  syncInstallInputs();
}
function syncInstallInputs() {
  installCatalogUrl.value = catalogManifestUrl.value || localStorage.getItem("vortexoCatalogManifestUrl") || "";
  installStreamUrl.value = streamManifestUrl.value || localStorage.getItem("vortexoStreamManifestUrl") || "";
}
function syncDraftFromInstall() {
  catalogManifestUrl.value = installCatalogUrl.value.trim();
  streamManifestUrl.value = installStreamUrl.value.trim();
  saveDraft();
}
function updateServerUrl() {
  serverUrl.textContent = window.location.origin;
}
async function copyServerUrl() {
  try {
    await navigator.clipboard.writeText(window.location.origin);
    finishStatus.textContent = "Copied";
    finishStatus.className = "message ok";
  } catch {
    finishStatus.textContent = window.location.origin;
  }
}
function escapeHtml(value) {
  return String(value || "").replace(/[&<>"']/g, (c) => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#039;"}[c]));
}
function escapeAttr(value) {
  return escapeHtml(value).replace(/\\/g, "\\\\");
}
restoreChecklist();
restoreDraft();
updateServerUrl();
if (token) {
  authPill.textContent = "Signed in";
  authPill.className = "statusPill ok";
  markDone("signin");
  loadManifests();
}
</script>
</body>
</html>`

func _htmlEscape(value string) string {
	return html.EscapeString(value)
}
