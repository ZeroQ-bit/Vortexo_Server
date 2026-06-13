package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
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
	defaultListenAddr               = ":8080"
	defaultUsername                 = "vortexo"
	defaultPassword                 = "vortexo"
	defaultRegistryURL              = "https://stremio-addons.net/api/manifest.json"
	streamingCatalogAddonID         = "com.vortexo.streaming-catalogs"
	streamingCatalogAddonName       = "Streaming Catalogs"
	streamingCatalogAddonPath       = "/addons/streaming-catalogs"
	streamingCatalogCacheTTL        = 6 * time.Hour
	streamingCatalogJustWatchLimit  = 100
	streamingCatalogNewLookbackDays = 30
	streamingCatalogJustWatchURL    = "https://apis.justwatch.com/graphql"
	tmdbAPIBaseURL                  = "https://api.themoviedb.org"
	tmdbKeywordRowsDefaultCount     = 10
	tmdbKeywordRowsMaxCount         = 50
	tmdbKeywordRowMaxItems          = 50
	plexProduct                     = "Vortexo Manifest Server"
	plexVersion                     = "1.0.0"
	plexPlatform                    = "Go"
	plexDevice                      = "Server"
	plexDeviceName                  = "Vortexo Manifest Server"
	watchStateEnrichmentLimit       = 48
	watchStateEnrichmentConcurrency = 6
	watchStateMetadataTimeout       = 6 * time.Second
	watchStateMetadataCacheTTL      = 30 * time.Minute
	plexArtworkSyncInterval         = time.Hour
	plexArtworkInitialDelay         = 15 * time.Second
	plexArtworkFetchDelay           = 2 * time.Second
	plexArtworkStaleAfter           = 30 * 24 * time.Hour
	plexArtworkIncompleteRetryAfter = time.Hour
	plexArtworkSyncLimit            = 500
	plexArtworkSeedCatalogLimit     = 60
	traktWatchSyncInitialDelay      = 45 * time.Second
	traktWatchSyncInterval          = 30 * time.Minute
	youtubePlayerAPIKey             = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"
)

var srtTimestampPattern = regexp.MustCompile(`(\d{2}:\d{2}:\d{2}),(\d{3})`)
var justWatchPosterPattern = regexp.MustCompile(`/poster/([0-9]+)/`)
var traktAtomEpisodePattern = regexp.MustCompile(`/shows/([^/]+)/seasons/([0-9]+)/episodes/([0-9]+)`)

type appState struct {
	mu                       sync.RWMutex
	dataDir                  string
	config                   bridgeConfig
	watchState               watchStateStore
	watchSyncMu              sync.Mutex
	client                   *http.Client
	manifest                 map[string]manifestCacheEntry
	watchMeta                map[string]watchStateMetadataCacheEntry
	streamingCatalogMu       sync.RWMutex
	streamingCatalogs        map[string]streamingCatalogCacheEntry
	tmdbKeywordMu            sync.RWMutex
	tmdbKeywordRows          map[string]tmdbKeywordRowsCacheEntry
	tmdbKeywordIDs           map[string]tmdbKeywordIDCacheEntry
	plexArtworkMu            sync.RWMutex
	plexArtwork              map[string]plexArtworkCacheRecord
	plexArtworkSyncMu        sync.Mutex
	plexArtworkSyncStateMu   sync.RWMutex
	plexArtworkSyncRunning   bool
	plexArtworkLastSync      *plexArtworkSyncStats
	plexArtworkRequestMu     sync.Mutex
	plexArtworkLastRequestAt time.Time
}

type bridgeConfig struct {
	AdminUsername    string                `json:"admin_username"`
	AdminPassword    string                `json:"admin_password"`
	AuthToken        string                `json:"auth_token"`
	AddonRegistryURL string                `json:"addon_registry_url,omitempty"`
	Manifests        []installedManifest   `json:"manifests"`
	Catalogs         []catalogPreference   `json:"catalogs,omitempty"`
	Plex             plexAuthConfig        `json:"plex,omitempty"`
	Watch            watchSyncConfig       `json:"watch,omitempty"`
	TMDBKeywordRows  tmdbKeywordRowsConfig `json:"tmdb_keyword_rows,omitempty"`
}

type plexAuthConfig struct {
	ClientID       string    `json:"client_id,omitempty"`
	AccessToken    string    `json:"access_token,omitempty"`
	UserID         int       `json:"user_id,omitempty"`
	Username       string    `json:"username,omitempty"`
	Email          string    `json:"email,omitempty"`
	Title          string    `json:"title,omitempty"`
	LastSignedInAt time.Time `json:"last_signed_in_at,omitempty"`
}

type plexQueryItem struct {
	Name  string
	Value string
}

type watchSyncConfig struct {
	Trakt traktWatchConfig `json:"trakt,omitempty"`
}

type traktWatchConfig struct {
	ClientID       string    `json:"client_id,omitempty"`
	ClientSecret   string    `json:"client_secret,omitempty"`
	AccessToken    string    `json:"access_token,omitempty"`
	RefreshToken   string    `json:"refresh_token,omitempty"`
	UpNextAtomURL  string    `json:"up_next_atom_url,omitempty"`
	TokenExpiresAt time.Time `json:"token_expires_at,omitempty"`
	LastSyncAt     time.Time `json:"last_sync_at,omitempty"`
}

type tmdbKeywordRowsConfig struct {
	Enabled         bool      `json:"enabled,omitempty"`
	RowCount        int       `json:"row_count,omitempty"`
	TMDBAPIKey      string    `json:"tmdb_api_key,omitempty"`
	TMDBAccessToken string    `json:"tmdb_access_token,omitempty"`
	Language        string    `json:"language,omitempty"`
	Region          string    `json:"region,omitempty"`
	UpdatedAt       time.Time `json:"updated_at,omitempty"`
}

type installedManifest struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	URL       string    `json:"url"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type streamingCatalogSetupRequest struct {
	Install        bool     `json:"install"`
	Providers      []string `json:"providers"`
	Types          []string `json:"types"`
	MergeProviders bool     `json:"merge_providers,omitempty"`
	MergeAll       bool     `json:"merge_all,omitempty"`
	SortBy         string   `json:"sort_by,omitempty"`
	RPDBKey        string   `json:"rpdb_key,omitempty"`
}

type tmdbKeywordRowsRequest struct {
	Enabled          bool   `json:"enabled"`
	RowCount         int    `json:"row_count"`
	TMDBAPIKey       string `json:"tmdb_api_key"`
	TMDBAccessToken  string `json:"tmdb_access_token"`
	Language         string `json:"language"`
	Region           string `json:"region"`
	ClearCredentials bool   `json:"clear_credentials"`
}

type streamingCatalogSetupResponse struct {
	OK          bool              `json:"ok"`
	ManifestURL string            `json:"manifest_url"`
	Manifest    installedManifest `json:"manifest,omitempty"`
}

type streamingCatalogAddonConfig struct {
	Providers      []string `json:"providers"`
	Types          []string `json:"types"`
	MergeProviders bool     `json:"merge_providers,omitempty"`
	MergeAll       bool     `json:"merge_all,omitempty"`
	SortBy         string   `json:"sort_by,omitempty"`
	RPDBKey        string   `json:"rpdb_key,omitempty"`
}

type streamingCatalogProvider struct {
	ID        string
	Name      string
	Types     []string
	Countries map[string]string
	Languages map[string]string
}

type streamingCatalogCacheEntry struct {
	items   []stremioMeta
	expires time.Time
}

type tmdbKeywordRowsCacheEntry struct {
	items   []vortexoHomeItem
	expires time.Time
}

type tmdbKeywordIDCacheEntry struct {
	id      int
	expires time.Time
}

type catalogPreference struct {
	Key         string    `json:"key"`
	ManifestID  string    `json:"manifest_id"`
	CatalogType string    `json:"catalog_type"`
	CatalogID   string    `json:"catalog_id"`
	Name        string    `json:"name,omitempty"`
	Enabled     bool      `json:"enabled"`
	SortOrder   int       `json:"sort_order"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type dashboardManifest struct {
	ID           string                     `json:"id"`
	Name         string                     `json:"name"`
	URL          string                     `json:"url"`
	Enabled      bool                       `json:"enabled"`
	CreatedAt    time.Time                  `json:"created_at"`
	UpdatedAt    time.Time                  `json:"updated_at"`
	Status       string                     `json:"status"`
	Message      string                     `json:"message,omitempty"`
	Description  string                     `json:"description,omitempty"`
	Version      string                     `json:"version,omitempty"`
	Logo         string                     `json:"logo,omitempty"`
	Resources    []string                   `json:"resources"`
	Types        []string                   `json:"types"`
	Capabilities []string                   `json:"capabilities"`
	Catalogs     []dashboardManifestCatalog `json:"catalogs"`
}

type dashboardManifestCatalog struct {
	Key            string   `json:"key"`
	ManifestID     string   `json:"manifest_id"`
	ManifestName   string   `json:"manifest_name"`
	Type           string   `json:"type"`
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	OriginalName   string   `json:"original_name"`
	Enabled        bool     `json:"enabled"`
	SortOrder      int      `json:"sort_order"`
	Search         bool     `json:"search"`
	RequiredExtras []string `json:"required_extras,omitempty"`
	OptionalExtras []string `json:"optional_extras,omitempty"`
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

type watchStateMetadataCacheEntry struct {
	item    watchStateItem
	expires time.Time
}

type stremioManifest struct {
	ID            string           `json:"id"`
	Name          string           `json:"name"`
	Description   string           `json:"description"`
	Version       string           `json:"version"`
	Logo          string           `json:"logo"`
	Resources     []any            `json:"resources"`
	Types         []string         `json:"types"`
	Catalogs      []stremioCatalog `json:"catalogs"`
	AddonCatalogs []stremioCatalog `json:"addonCatalogs"`
	BehaviorHints any              `json:"behaviorHints"`
}

type addonCatalogResponse struct {
	Addons []addonCatalogEntry `json:"addons"`
	Items  []addonCatalogEntry `json:"items"`
	Metas  []addonCatalogEntry `json:"metas"`
}

type addonCatalogEntry struct {
	TransportURL  string          `json:"transportUrl"`
	TransportName string          `json:"transportName"`
	URL           string          `json:"url"`
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	Description   string          `json:"description"`
	Logo          string          `json:"logo"`
	Version       string          `json:"version"`
	Manifest      stremioManifest `json:"manifest"`
}

type dashboardAddon struct {
	ID                    string                     `json:"id"`
	Name                  string                     `json:"name"`
	Description           string                     `json:"description,omitempty"`
	Version               string                     `json:"version,omitempty"`
	Logo                  string                     `json:"logo,omitempty"`
	URL                   string                     `json:"url"`
	ConfigURL             string                     `json:"config_url,omitempty"`
	TransportName         string                     `json:"transport_name,omitempty"`
	Installed             bool                       `json:"installed"`
	Configurable          bool                       `json:"configurable"`
	ConfigurationRequired bool                       `json:"configuration_required"`
	Resources             []string                   `json:"resources"`
	Types                 []string                   `json:"types"`
	Capabilities          []string                   `json:"capabilities"`
	Catalogs              []dashboardManifestCatalog `json:"catalogs"`
}

type manifestCatalogEntry struct {
	Item     installedManifest
	Manifest stremioManifest
	Base     string
	Catalog  stremioCatalog
	Pref     catalogPreference
	Order    int
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

type justWatchPopularTitlesResponse struct {
	Data struct {
		PopularTitles struct {
			Edges []struct {
				Node struct {
					Content struct {
						Title       string `json:"title"`
						PosterURL   string `json:"posterUrl"`
						ReleaseYear int    `json:"originalReleaseYear"`
						ExternalIDs struct {
							IMDBID string `json:"imdbId"`
						} `json:"externalIds"`
						Scoring struct {
							IMDBScore any `json:"imdbScore"`
						} `json:"scoring"`
					} `json:"content"`
				} `json:"node"`
			} `json:"edges"`
		} `json:"popularTitles"`
	} `json:"data"`
}

type justWatchNewTitlesResponse struct {
	Data struct {
		NewTitles struct {
			Edges []struct {
				Node struct {
					TypeName   string                   `json:"__typename"`
					ObjectType string                   `json:"objectType"`
					Content    justWatchNewTitleContent `json:"content"`
					Show       struct {
						ObjectType string                   `json:"objectType"`
						Content    justWatchNewTitleContent `json:"content"`
					} `json:"show"`
				} `json:"node"`
			} `json:"edges"`
		} `json:"newTitles"`
	} `json:"data"`
}

type justWatchNewTitleContent struct {
	Title        string `json:"title"`
	PosterURL    string `json:"posterUrl"`
	ReleaseYear  int    `json:"originalReleaseYear"`
	SeasonNumber int    `json:"seasonNumber"`
	ExternalIDs  struct {
		IMDBID string `json:"imdbId"`
	} `json:"externalIds"`
	Scoring struct {
		IMDBScore any `json:"imdbScore"`
	} `json:"scoring"`
}

var streamingCatalogDefaultProviders = []string{"nfx", "dnp", "amp", "atp", "hbm"}
var streamingCatalogDefaultTypes = []string{"movie", "series"}
var streamingCatalogDefaultSortBy = "TRENDING"
var streamingCatalogMergedID = "merged"
var streamingCatalogLatestReleasesID = "latest-releases"
var streamingCatalogProviderOrder = []string{
	"nfx", "nfk", "dnp", "amp", "atp", "hbm", "pmp", "hlu", "pcp", "cru",
	"jhs", "zee", "vil", "nlz", "sst", "clv", "gop", "hay", "mgl", "cts",
	"cpd", "stz", "dpe", "mbi", "vik", "sgo", "sonyliv", "mp9", "shd", "bbo",
	"act", "itv", "bbc", "al4", "crc", "iqi", "sha",
}
var streamingCatalogProviders = map[string]streamingCatalogProvider{
	"nfx":     {ID: "nfx", Name: "Netflix", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "GB", "series": "GB"}},
	"nfk":     {ID: "nfk", Name: "Netflix Kids", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "US", "series": "US"}},
	"dnp":     {ID: "dnp", Name: "Disney+", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "GB", "series": "GB"}},
	"amp":     {ID: "amp", Name: "Prime Video", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "US", "series": "US"}},
	"atp":     {ID: "atp", Name: "Apple TV+", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "GB", "series": "GB"}},
	"hbm":     {ID: "hbm", Name: "HBO Max", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "NL", "series": "NL"}},
	"pmp":     {ID: "pmp", Name: "Paramount+", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "US", "series": "US"}},
	"hlu":     {ID: "hlu", Name: "Hulu", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "US", "series": "US"}},
	"pcp":     {ID: "pcp", Name: "Peacock", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "US", "series": "US"}},
	"cru":     {ID: "cru", Name: "Crunchyroll", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "US", "series": "US"}},
	"jhs":     {ID: "jhs", Name: "JioHotstar", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "IN", "series": "IN"}, Languages: map[string]string{"movie": "in", "series": "in"}},
	"zee":     {ID: "zee", Name: "Zee5", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "IN", "series": "IN"}, Languages: map[string]string{"movie": "in", "series": "in"}},
	"vil":     {ID: "vil", Name: "Videoland", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "NL", "series": "NL"}, Languages: map[string]string{"movie": "nl", "series": "nl"}},
	"nlz":     {ID: "nlz", Name: "NLZIET", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "NL", "series": "NL"}, Languages: map[string]string{"movie": "nl", "series": "nl"}},
	"sst":     {ID: "sst", Name: "SkyShowtime", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "NL", "series": "NL"}, Languages: map[string]string{"movie": "nl", "series": "nl"}},
	"clv":     {ID: "clv", Name: "Clarovideo", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "BR", "series": "BR"}, Languages: map[string]string{"movie": "br", "series": "br"}},
	"gop":     {ID: "gop", Name: "Globoplay", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "BR", "series": "BR"}, Languages: map[string]string{"movie": "br", "series": "br"}},
	"hay":     {ID: "hay", Name: "Hayu", Types: []string{"series"}, Countries: map[string]string{"series": "GB"}},
	"mgl":     {ID: "mgl", Name: "MagellanTV", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "US", "series": "US"}},
	"cts":     {ID: "cts", Name: "Curiosity Stream", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "US", "series": "US"}},
	"cpd":     {ID: "cpd", Name: "Canal+", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "FR", "series": "FR"}, Languages: map[string]string{"movie": "fr", "series": "fr"}},
	"stz":     {ID: "stz", Name: "Starz", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "US", "series": "US"}},
	"dpe":     {ID: "dpe", Name: "Discovery+", Types: []string{"series"}, Countries: map[string]string{"series": "GB"}},
	"mbi":     {ID: "mbi", Name: "Mubi", Types: []string{"movie"}, Countries: map[string]string{"movie": "US"}},
	"vik":     {ID: "vik", Name: "Rakuten Viki", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "US", "series": "US"}},
	"sgo":     {ID: "sgo", Name: "Sky Go", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "DE", "series": "DE"}, Languages: map[string]string{"movie": "de", "series": "de"}},
	"sonyliv": {ID: "sonyliv", Name: "Sony Liv", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "IN", "series": "IN"}, Languages: map[string]string{"movie": "hi", "series": "hi"}},
	"mp9":     {ID: "mp9", Name: "Movistar+", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "ES", "series": "ES"}, Languages: map[string]string{"movie": "es", "series": "es"}},
	"shd":     {ID: "shd", Name: "Shudder", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "US", "series": "US"}},
	"bbo":     {ID: "bbo", Name: "BritBox", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "US", "series": "US"}},
	"act":     {ID: "act", Name: "Acorn TV", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "US", "series": "US"}},
	"itv":     {ID: "itv", Name: "ITVX", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "GB", "series": "GB"}},
	"bbc":     {ID: "bbc", Name: "BBC iPlayer", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "GB", "series": "GB"}},
	"al4":     {ID: "al4", Name: "Channel 4", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "GB", "series": "GB"}},
	"crc":     {ID: "crc", Name: "Criterion Channel", Types: []string{"movie"}, Countries: map[string]string{"movie": "US"}},
	"iqi":     {ID: "iqi", Name: "iQIYI", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "US", "series": "US"}},
	"sha":     {ID: "sha", Name: "Shahid VIP", Types: []string{"movie", "series"}, Countries: map[string]string{"movie": "US", "series": "US"}},
}

type stremioMeta struct {
	ID             string                 `json:"id"`
	Type           string                 `json:"type"`
	Name           string                 `json:"name"`
	Title          string                 `json:"title"`
	URL            string                 `json:"url"`
	StreamURL      string                 `json:"streamUrl"`
	ExternalURL    string                 `json:"externalUrl"`
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

type vortexoLiveChannel struct {
	ID             string `json:"id"`
	StreamID       int    `json:"stream_id,omitempty"`
	EPGChannelID   string `json:"epg_channel_id,omitempty"`
	CategoryID     string `json:"category_id,omitempty"`
	Name           string `json:"name"`
	Logo           string `json:"logo,omitempty"`
	StreamIcon     string `json:"stream_icon,omitempty"`
	StreamURL      string `json:"stream_url,omitempty"`
	URL            string `json:"url,omitempty"`
	Category       string `json:"category,omitempty"`
	CategoryName   string `json:"category_name,omitempty"`
	Language       string `json:"language,omitempty"`
	Country        string `json:"country,omitempty"`
	IsLive         bool   `json:"is_live"`
	Active         bool   `json:"active"`
	Source         string `json:"source,omitempty"`
	HasEPG         bool   `json:"has_epg"`
	ManifestBase   string `json:"-"`
	ManifestName   string `json:"-"`
	ManifestID     string `json:"-"`
	CatalogType    string `json:"-"`
	CatalogID      string `json:"-"`
	OriginalItemID string `json:"-"`
}

type vortexoLiveTVRow struct {
	ID     string                 `json:"id"`
	Title  string                 `json:"title"`
	Reason string                 `json:"reason,omitempty"`
	Items  []vortexoLiveTVRowItem `json:"items"`
}

type vortexoLiveTVRowItem struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Logo     string `json:"logo,omitempty"`
	Category string `json:"category,omitempty"`
	Source   string `json:"source,omitempty"`
	HasEPG   bool   `json:"has_epg"`
}

type xtreamLiveCategory struct {
	CategoryID   string `json:"category_id"`
	CategoryName string `json:"category_name"`
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

type watchStateStore struct {
	UpdatedAt time.Time        `json:"updated_at"`
	Items     []watchStateItem `json:"items"`
}

type watchStateItem struct {
	ID              string    `json:"id"`
	MediaType       string    `json:"media_type"`
	Title           string    `json:"title,omitempty"`
	ParentTitle     string    `json:"parent_title,omitempty"`
	Year            int       `json:"year,omitempty"`
	IMDBID          string    `json:"imdb_id,omitempty"`
	TMDBID          int       `json:"tmdb_id,omitempty"`
	TVDBID          int       `json:"tvdb_id,omitempty"`
	TraktID         int       `json:"trakt_id,omitempty"`
	Season          int       `json:"season,omitempty"`
	Episode         int       `json:"episode,omitempty"`
	Watched         bool      `json:"watched"`
	WatchedAt       time.Time `json:"watched_at,omitempty"`
	ProgressPercent float64   `json:"progress_percent,omitempty"`
	ProgressSeconds int       `json:"progress_seconds,omitempty"`
	DurationSeconds int       `json:"duration_seconds,omitempty"`
	Overview        string    `json:"overview,omitempty"`
	PosterPath      string    `json:"poster_path,omitempty"`
	BackdropPath    string    `json:"backdrop_path,omitempty"`
	LandscapePath   string    `json:"landscape_path,omitempty"`
	LogoPath        string    `json:"logo_path,omitempty"`
	StillPath       string    `json:"still_path,omitempty"`
	ReleaseDate     string    `json:"release_date,omitempty"`
	AirDate         string    `json:"air_date,omitempty"`
	Runtime         int       `json:"runtime,omitempty"`
	Genres          []string  `json:"genres,omitempty"`
	VoteAverage     float64   `json:"vote_average,omitempty"`
	PlayCount       int       `json:"play_count,omitempty"`
	Source          string    `json:"source"`
	SourceUserID    string    `json:"source_user_id,omitempty"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type watchStateMarkRequest struct {
	ID          string `json:"id,omitempty"`
	RatingKey   string `json:"rating_key,omitempty"`
	MediaType   string `json:"media_type,omitempty"`
	Title       string `json:"title,omitempty"`
	ParentTitle string `json:"parent_title,omitempty"`
	Year        int    `json:"year,omitempty"`
	IMDBID      string `json:"imdb_id,omitempty"`
	TMDBID      int    `json:"tmdb_id,omitempty"`
	TVDBID      int    `json:"tvdb_id,omitempty"`
	TraktID     int    `json:"trakt_id,omitempty"`
	Season      int    `json:"season,omitempty"`
	Episode     int    `json:"episode,omitempty"`
	Watched     *bool  `json:"watched,omitempty"`
}

type plexArtwork struct {
	CoverArt   []string `json:"coverArt"`
	Landscape  []string `json:"landscape"`
	Background []string `json:"background"`
	ClearLogo  []string `json:"clearLogo"`
	Thumbnail  []string `json:"thumbnail"`
}

type plexArtworkEntry struct {
	Version    int         `json:"version"`
	MediaType  string      `json:"mediaType"`
	TMDBID     int         `json:"tmdbId"`
	IMDBID     string      `json:"imdbId,omitempty"`
	Title      string      `json:"title,omitempty"`
	Year       int         `json:"year,omitempty"`
	SourcePage string      `json:"sourcePage,omitempty"`
	UpdatedAt  time.Time   `json:"updatedAt"`
	Artwork    plexArtwork `json:"artwork"`
}

type plexArtworkCacheRecord struct {
	plexArtworkEntry
	Status    string    `json:"status"`
	Error     string    `json:"error,omitempty"`
	FetchedAt time.Time `json:"fetchedAt"`
}

type plexArtworkCacheFile struct {
	UpdatedAt time.Time                `json:"updatedAt"`
	Items     []plexArtworkCacheRecord `json:"items"`
}

type plexArtworkSeedItem struct {
	MediaType string
	TMDBID    int
	IMDBID    string
	Title     string
	Year      int
}

type plexDiscoverImage struct {
	Alt  string `json:"alt,omitempty"`
	Type string `json:"type,omitempty"`
	URL  string `json:"url,omitempty"`
}

type plexDiscoverImageSet struct {
	CoverArt            string `json:"coverArt,omitempty"`
	Landscape           string `json:"landscape,omitempty"`
	Wide                string `json:"wide,omitempty"`
	Tile                string `json:"tile,omitempty"`
	Snapshot            string `json:"snapshot,omitempty"`
	Background          string `json:"background,omitempty"`
	BackgroundLandscape string `json:"backgroundLandscape,omitempty"`
	Art                 string `json:"art,omitempty"`
	Hero                string `json:"hero,omitempty"`
	ClearLogo           string `json:"clearLogo,omitempty"`
	Logo                string `json:"logo,omitempty"`
	Thumbnail           string `json:"thumbnail,omitempty"`
	Thumb               string `json:"thumb,omitempty"`
	Poster              string `json:"poster,omitempty"`
	CoverPoster         string `json:"coverPoster,omitempty"`
}

func (s *plexDiscoverImageSet) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil
	}

	s.CoverArt = firstNonEmpty(s.CoverArt, plexDiscoverImageURLFromRaw(raw["coverArt"]))
	s.Landscape = firstNonEmpty(s.Landscape, plexDiscoverImageURLFromRaw(raw["landscape"]))
	s.Wide = firstNonEmpty(s.Wide, plexDiscoverImageURLFromRaw(raw["wide"]))
	s.Tile = firstNonEmpty(s.Tile, plexDiscoverImageURLFromRaw(raw["tile"]))
	s.Snapshot = firstNonEmpty(s.Snapshot, plexDiscoverImageURLFromRaw(raw["snapshot"]))
	s.Background = firstNonEmpty(s.Background, plexDiscoverImageURLFromRaw(raw["background"]))
	s.BackgroundLandscape = firstNonEmpty(s.BackgroundLandscape, plexDiscoverImageURLFromRaw(raw["backgroundLandscape"]))
	s.Art = firstNonEmpty(s.Art, plexDiscoverImageURLFromRaw(raw["art"]))
	s.Hero = firstNonEmpty(s.Hero, plexDiscoverImageURLFromRaw(raw["hero"]))
	s.ClearLogo = firstNonEmpty(s.ClearLogo, plexDiscoverImageURLFromRaw(raw["clearLogo"]))
	s.Logo = firstNonEmpty(s.Logo, plexDiscoverImageURLFromRaw(raw["logo"]))
	s.Thumbnail = firstNonEmpty(s.Thumbnail, plexDiscoverImageURLFromRaw(raw["thumbnail"]))
	s.Thumb = firstNonEmpty(s.Thumb, plexDiscoverImageURLFromRaw(raw["thumb"]))
	s.Poster = firstNonEmpty(s.Poster, plexDiscoverImageURLFromRaw(raw["poster"]))
	s.CoverPoster = firstNonEmpty(s.CoverPoster, plexDiscoverImageURLFromRaw(raw["coverPoster"]))
	return nil
}

func plexDiscoverImageURLFromRaw(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var value string
	if err := json.Unmarshal(raw, &value); err == nil {
		return strings.TrimSpace(value)
	}
	var values []json.RawMessage
	if err := json.Unmarshal(raw, &values); err == nil {
		for _, item := range values {
			if decoded := plexDiscoverImageURLFromRaw(item); decoded != "" {
				return decoded
			}
		}
		return ""
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err == nil {
		for _, key := range []string{"url", "src", "image"} {
			if decoded := plexDiscoverImageURLFromRaw(object[key]); decoded != "" {
				return decoded
			}
		}
	}
	return ""
}

type plexDiscoverGuid struct {
	ID string `json:"id,omitempty"`
}

type plexDiscoverFloat float64

func (value *plexDiscoverFloat) UnmarshalJSON(data []byte) error {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" || trimmed == "null" || trimmed == `""` || trimmed == "[]" {
		*value = 0
		return nil
	}
	if strings.HasPrefix(trimmed, "[") || strings.HasPrefix(trimmed, "{") {
		*value = 0
		return nil
	}
	if parsed, err := strconv.ParseFloat(strings.Trim(trimmed, `"`), 64); err == nil {
		*value = plexDiscoverFloat(parsed)
		return nil
	}
	*value = 0
	return nil
}

type plexDiscoverMetadata struct {
	RatingKey             string               `json:"ratingKey,omitempty"`
	Key                   string               `json:"key,omitempty"`
	GUID                  string               `json:"guid,omitempty"`
	PrimaryGUID           string               `json:"primaryGuid,omitempty"`
	Type                  string               `json:"type,omitempty"`
	Title                 string               `json:"title,omitempty"`
	OriginalTitle         string               `json:"originalTitle,omitempty"`
	Summary               string               `json:"summary,omitempty"`
	Year                  int                  `json:"year,omitempty"`
	Index                 int                  `json:"index,omitempty"`
	ParentIndex           int                  `json:"parentIndex,omitempty"`
	LeafCount             int                  `json:"leafCount,omitempty"`
	ChildCount            int                  `json:"childCount,omitempty"`
	Duration              int                  `json:"duration,omitempty"`
	Rating                plexDiscoverFloat    `json:"rating,omitempty"`
	AudienceRating        plexDiscoverFloat    `json:"audienceRating,omitempty"`
	OriginallyAvailableAt string               `json:"originallyAvailableAt,omitempty"`
	PublicPagesURL        string               `json:"publicPagesURL,omitempty"`
	Slug                  string               `json:"slug,omitempty"`
	Thumb                 string               `json:"thumb,omitempty"`
	Art                   string               `json:"art,omitempty"`
	Banner                string               `json:"banner,omitempty"`
	Image                 []plexDiscoverImage  `json:"Image,omitempty"`
	Images                plexDiscoverImageSet `json:"images,omitempty"`
	Guid                  []plexDiscoverGuid   `json:"Guid,omitempty"`
	Guids                 []plexDiscoverGuid   `json:"guids,omitempty"`
}

type plexDiscoverSearchResponse struct {
	MediaContainer struct {
		SearchResults []struct {
			SearchResult []struct {
				Metadata *plexDiscoverMetadata `json:"Metadata"`
			} `json:"SearchResult"`
		} `json:"SearchResults"`
	} `json:"MediaContainer"`
}

type plexDiscoverMetadataResponse struct {
	MediaContainer struct {
		Offset    int                    `json:"offset"`
		Size      int                    `json:"size"`
		TotalSize int                    `json:"totalSize"`
		Metadata  []plexDiscoverMetadata `json:"Metadata"`
		Directory []plexDiscoverMetadata `json:"Directory"`
	} `json:"MediaContainer"`
}

type plexPin struct {
	ID        int    `json:"id"`
	Code      string `json:"code"`
	AuthToken string `json:"authToken"`
	ExpiresIn int    `json:"expiresIn"`
	ExpiresAt string `json:"expiresAt"`
	QR        string `json:"qr"`
}

type plexUser struct {
	ID       int    `json:"id"`
	UUID     string `json:"uuid"`
	Username string `json:"username"`
	Email    string `json:"email"`
	Title    string `json:"title"`
}

type plexArtworkSyncStats struct {
	Limit       int       `json:"limit"`
	Attempted   int       `json:"attempted"`
	OK          int       `json:"ok"`
	Miss        int       `json:"miss"`
	Skipped     int       `json:"skipped"`
	Failed      int       `json:"failed"`
	Stopped     string    `json:"stopped,omitempty"`
	StartedAt   time.Time `json:"startedAt"`
	CompletedAt time.Time `json:"completedAt"`
}

type plexArtworkDashboardStats struct {
	Total          int                   `json:"total"`
	OK             int                   `json:"ok"`
	CleanLandscape int                   `json:"clean_landscape"`
	BackdropOnly   int                   `json:"backdrop_only"`
	WithLogo       int                   `json:"with_logo"`
	Miss           int                   `json:"miss"`
	Error          int                   `json:"error"`
	SignedDiscover int                   `json:"signed_discover"`
	PublicFallback int                   `json:"public_fallback"`
	Running        bool                  `json:"running"`
	HasPlexToken   bool                  `json:"has_plex_token"`
	LastFetchedAt  time.Time             `json:"last_fetched_at,omitempty"`
	LastUpdatedAt  time.Time             `json:"last_updated_at,omitempty"`
	LastSync       *plexArtworkSyncStats `json:"last_sync,omitempty"`
}

func (a plexArtwork) isEmpty() bool {
	return len(a.CoverArt) == 0 &&
		len(a.Landscape) == 0 &&
		len(a.Background) == 0 &&
		len(a.ClearLogo) == 0 &&
		len(a.Thumbnail) == 0
}

type watchSettingsRequest struct {
	TraktClientID      string `json:"trakt_client_id"`
	TraktClientSecret  string `json:"trakt_client_secret"`
	TraktAccessToken   string `json:"trakt_access_token"`
	TraktRefreshToken  string `json:"trakt_refresh_token"`
	TraktUpNextAtomURL string `json:"trakt_up_next_atom_url"`
	ClearTraktTokens   bool   `json:"clear_trakt_tokens"`
}

type traktDeviceCodeRequest struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}

type traktDeviceTokenRequest struct {
	DeviceCode string `json:"device_code"`
}

type plexSettingsRequest struct {
	AccessToken string `json:"access_token"`
	ClearToken  bool   `json:"clear_token"`
}

type plexPinTokenRequest struct {
	PinID int `json:"pin_id"`
}

type playToken struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
	Title   string            `json:"title,omitempty"`
}

type youtubePlayerResponse struct {
	StreamingData struct {
		HLSURL          string          `json:"hlsManifestUrl"`
		Formats         []youtubeFormat `json:"formats"`
		AdaptiveFormats []youtubeFormat `json:"adaptiveFormats"`
	} `json:"streamingData"`
	PlayabilityStatus struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	} `json:"playabilityStatus"`
}

type youtubeFormat struct {
	URL           string `json:"url"`
	MimeType      string `json:"mimeType"`
	QualityLabel  string `json:"qualityLabel"`
	Height        int    `json:"height"`
	Bitrate       int    `json:"bitrate"`
	AudioQuality  string `json:"audioQuality"`
	AudioChannels int    `json:"audioChannels"`
}

type traktDeviceCodeResponse struct {
	DeviceCode      string `json:"device_code"`
	UserCode        string `json:"user_code"`
	VerificationURL string `json:"verification_url"`
	ExpiresIn       int    `json:"expires_in"`
	Interval        int    `json:"interval"`
}

type traktTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
	Scope        string `json:"scope"`
	CreatedAt    int64  `json:"created_at"`
}

type traktIDs struct {
	Trakt int    `json:"trakt"`
	Slug  string `json:"slug"`
	IMDB  string `json:"imdb"`
	TMDB  int    `json:"tmdb"`
	TVDB  int    `json:"tvdb"`
}

type traktMovie struct {
	Title string   `json:"title"`
	Year  int      `json:"year"`
	IDs   traktIDs `json:"ids"`
}

type traktShow struct {
	Title string   `json:"title"`
	Year  int      `json:"year"`
	IDs   traktIDs `json:"ids"`
}

type traktEpisode struct {
	Season     int       `json:"season"`
	Number     int       `json:"number"`
	Title      string    `json:"title"`
	IDs        traktIDs  `json:"ids"`
	FirstAired time.Time `json:"first_aired"`
}

type traktWatchedMovie struct {
	Plays         int        `json:"plays"`
	LastWatchedAt time.Time  `json:"last_watched_at"`
	Movie         traktMovie `json:"movie"`
}

type traktWatchedShow struct {
	Plays         int           `json:"plays"`
	LastWatchedAt time.Time     `json:"last_watched_at"`
	Show          traktShow     `json:"show"`
	Seasons       []traktSeason `json:"seasons"`
}

type traktSeason struct {
	Number   int                   `json:"number"`
	Episodes []traktEpisodeWatched `json:"episodes"`
}

type traktEpisodeWatched struct {
	Number        int       `json:"number"`
	Plays         int       `json:"plays"`
	LastWatchedAt time.Time `json:"last_watched_at"`
}

type traktPlaybackMovie struct {
	Progress float64    `json:"progress"`
	PausedAt time.Time  `json:"paused_at"`
	Movie    traktMovie `json:"movie"`
}

type traktPlaybackEpisode struct {
	Progress float64      `json:"progress"`
	PausedAt time.Time    `json:"paused_at"`
	Show     traktShow    `json:"show"`
	Episode  traktEpisode `json:"episode"`
}

type traktShowProgress struct {
	Aired         int           `json:"aired"`
	Completed     int           `json:"completed"`
	LastWatchedAt time.Time     `json:"last_watched_at"`
	NextEpisode   *traktEpisode `json:"next_episode"`
}

type traktUpNextAtomFeed struct {
	Entries []traktUpNextAtomEntry `xml:"entry"`
}

type traktUpNextAtomEntry struct {
	Title string                `xml:"title"`
	Links []traktUpNextAtomLink `xml:"link"`
}

type traktUpNextAtomLink struct {
	Href string `xml:"href,attr"`
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
		dataDir:           dataDir,
		client:            &http.Client{Timeout: 20 * time.Second},
		manifest:          map[string]manifestCacheEntry{},
		watchMeta:         map[string]watchStateMetadataCacheEntry{},
		streamingCatalogs: map[string]streamingCatalogCacheEntry{},
		tmdbKeywordRows:   map[string]tmdbKeywordRowsCacheEntry{},
		tmdbKeywordIDs:    map[string]tmdbKeywordIDCacheEntry{},
		plexArtwork:       map[string]plexArtworkCacheRecord{},
	}
	if err := state.load(); err != nil {
		return err
	}
	if err := state.loadWatchState(); err != nil {
		return err
	}
	if err := state.loadPlexArtworkCache(); err != nil {
		return err
	}

	mux := http.NewServeMux()
	state.registerRoutes(mux)
	go state.runPlexArtworkSyncWorker(context.Background())
	go state.runTraktWatchSyncWorker(context.Background())

	addr := firstNonEmpty(os.Getenv("VORTEXO_LISTEN_ADDR"), os.Getenv("PORT"), defaultListenAddr)
	if !strings.HasPrefix(addr, ":") && !strings.Contains(addr, ":") {
		addr = ":" + addr
	}
	log.Printf("Vortexo Add-on Server listening on %s", addr)
	return http.ListenAndServe(addr, state.withCORS(mux))
}

func (s *appState) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/v1/health", s.handleHealth)
	mux.HandleFunc("/api/v1/auth/status", s.handleAuthStatus)
	mux.HandleFunc("/api/v1/auth/login", s.handleLogin)
	mux.HandleFunc("/api/v1/auth/verify", s.requireAuth(s.handleVerify))
	mux.HandleFunc("/api/v1/settings", s.handleSettings)
	mux.HandleFunc("/api/v1/bridge/dashboard", s.requireAuth(s.handleBridgeDashboard))
	mux.HandleFunc("/api/v1/bridge/addon-registry", s.requireAuth(s.handleAddonRegistry))
	mux.HandleFunc("/api/v1/bridge/catalogs", s.requireAuth(s.handleCatalogPreferences))
	mux.HandleFunc("/api/v1/bridge/streaming-catalogs", s.requireAuth(s.handleStreamingCatalogsSetup))
	mux.HandleFunc("/api/v1/bridge/tmdb-keyword-rows", s.requireAuth(s.handleTMDBKeywordRowsSetup))
	mux.HandleFunc("/api/v1/bridge/perfect-setup", s.requireAuth(s.handlePerfectSetup))
	mux.HandleFunc("/api/v1/bridge/manifests", s.requireAuth(s.handleManifests))
	mux.HandleFunc("/api/v1/bridge/manifests/", s.requireAuth(s.handleManifestByID))
	mux.HandleFunc("/api/v1/bridge/watch/settings", s.requireAuth(s.handleWatchSettings))
	mux.HandleFunc("/api/v1/bridge/watch/history", s.requireAuth(s.handleClearWatchHistory))
	mux.HandleFunc("/api/v1/bridge/watch/trakt/device-code", s.requireAuth(s.handleTraktDeviceCode))
	mux.HandleFunc("/api/v1/bridge/watch/trakt/device-token", s.requireAuth(s.handleTraktDeviceToken))
	mux.HandleFunc("/api/v1/bridge/watch/trakt/sync", s.requireAuth(s.handleTraktWatchSync))
	mux.HandleFunc("/api/v1/bridge/plex/settings", s.requireAuth(s.handlePlexSettings))
	mux.HandleFunc("/api/v1/bridge/plex/pin", s.requireAuth(s.handlePlexPIN))
	mux.HandleFunc("/api/v1/bridge/plex/pin/token", s.requireAuth(s.handlePlexPINToken))
	mux.HandleFunc("/api/v1/movies", s.handleMovies)
	mux.HandleFunc("/api/v1/movies/", s.handleMovieByID)
	mux.HandleFunc("/api/v1/series", s.handleSeries)
	mux.HandleFunc("/api/v1/series/", s.handleSeriesByID)
	mux.HandleFunc("/api/v1/search", s.handleSearch)
	mux.HandleFunc("/api/v1/stats", s.handleStats)
	mux.HandleFunc("/api/v1/channels", s.handleChannels)
	mux.HandleFunc("/api/v1/channels/", s.handleChannels)
	mux.HandleFunc("/api/v1/discover/trending", s.handleDiscoverList)
	mux.HandleFunc("/api/v1/discover/popular", s.handleDiscoverList)
	mux.HandleFunc("/api/v1/tmdb/", s.handleTMDBByID)
	mux.HandleFunc("/api/v1/trailers/", s.handleTrailerPlay)
	mux.HandleFunc("/api/v1/media/", s.handleTrailerPlay)
	mux.HandleFunc("/api/v1/artwork/refresh", s.requireAuth(s.handlePlexArtworkRefresh))
	mux.HandleFunc("/api/v1/artwork/status", s.requireAuth(s.handlePlexArtworkStatus))
	mux.HandleFunc("/api/v1/artwork/", s.handlePlexArtworkByID)
	mux.HandleFunc("/api/v1/vortexo/capabilities", s.handleCapabilities)
	mux.HandleFunc("/api/v1/vortexo/home", s.handleVortexoHome)
	mux.HandleFunc("/api/v1/vortexo/live-tv/rows", s.handleVortexoLiveTVRows)
	if publicWatchStateEnabled() {
		log.Printf("Watch-state endpoint exposed without server auth")
		mux.HandleFunc("/api/v1/vortexo/watch-state", s.handleVortexoWatchState)
	} else {
		mux.HandleFunc("/api/v1/vortexo/watch-state", s.requireAuth(s.handleVortexoWatchState))
	}
	mux.HandleFunc("/api/v1/vortexo/watch-state/mark", s.requireAuth(s.handleVortexoWatchStateMark))
	mux.HandleFunc("/api/v1/vortexo/sources", s.handleVortexoSources)
	mux.HandleFunc("/api/v1/vortexo/play/", s.handleVortexoPlay)
	mux.HandleFunc("/api/v1/vortexo/subtitles/", s.handleVortexoSubtitles)
	mux.HandleFunc(streamingCatalogAddonPath+"/", s.handleStreamingCatalogsAddon)
	mux.HandleFunc("/player_api.php", s.handleXtreamPlayerAPI)
	mux.HandleFunc("/xmltv.php", s.handleXMLTV)
	mux.HandleFunc("/live/", s.handleLivePlayback)
}

func publicWatchStateEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("VORTEXO_PUBLIC_WATCH_STATE"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
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
	if bytes.Contains(data, []byte(`"plex_public_rows"`)) {
		changed = true
	}
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
	if s.config.AddonRegistryURL == "" {
		s.config.AddonRegistryURL = defaultRegistryURL
		changed = true
	}
	if s.config.Plex.ClientID == "" {
		s.config.Plex.ClientID = randomToken()
		changed = true
	}
	if token := strings.TrimSpace(firstNonEmpty(os.Getenv("VORTEXO_PLEX_TOKEN"), os.Getenv("PLEX_TOKEN"))); token != "" && token != s.config.Plex.AccessToken {
		s.config.Plex.AccessToken = token
		s.config.Plex.LastSignedInAt = time.Now().UTC()
		changed = true
	}
	if s.config.Manifests == nil {
		s.config.Manifests = []installedManifest{}
		changed = true
	}
	if s.config.Catalogs == nil {
		s.config.Catalogs = []catalogPreference{}
		changed = true
	}
	normalizedKeywords := normalizeTMDBKeywordRowsConfig(s.config.TMDBKeywordRows)
	if normalizedKeywords != s.config.TMDBKeywordRows {
		s.config.TMDBKeywordRows = normalizedKeywords
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

func (s *appState) loadWatchState() error {
	path := filepath.Join(s.dataDir, "watch_state.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		s.watchState = watchStateStore{UpdatedAt: time.Now().UTC(), Items: []watchStateItem{}}
		return nil
	}
	if err != nil {
		return err
	}
	var state watchStateStore
	if len(data) > 0 {
		if err := json.Unmarshal(data, &state); err != nil {
			return err
		}
	}
	if state.Items == nil {
		state.Items = []watchStateItem{}
	}
	filtered := state.Items[:0]
	changed := false
	for _, item := range state.Items {
		if strings.Contains(strings.ToLower(item.Source), "plex") {
			changed = true
			continue
		}
		filtered = append(filtered, item)
	}
	if changed {
		state.Items = filtered
	}
	s.watchState = state
	if changed {
		return s.saveWatchStateLocked()
	}
	return nil
}

func (s *appState) saveWatchStateLocked() error {
	s.watchState.UpdatedAt = time.Now().UTC()
	if s.watchState.Items == nil {
		s.watchState.Items = []watchStateItem{}
	}
	data, err := json.MarshalIndent(s.watchState, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(s.dataDir, "watch_state.json"), data, 0o600)
}

func (s *appState) loadPlexArtworkCache() error {
	path := filepath.Join(s.dataDir, "plex_artwork_cache.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		s.plexArtworkMu.Lock()
		s.plexArtwork = map[string]plexArtworkCacheRecord{}
		s.plexArtworkMu.Unlock()
		return nil
	}
	if err != nil {
		return err
	}

	var file plexArtworkCacheFile
	if len(data) > 0 {
		if err := json.Unmarshal(data, &file); err != nil {
			return err
		}
	}

	records := map[string]plexArtworkCacheRecord{}
	for _, record := range file.Items {
		key := plexArtworkKey(record.MediaType, record.TMDBID, record.IMDBID, record.Title, record.Year)
		if key == "" {
			continue
		}
		record.MediaType = normalizePlexArtworkMediaType(record.MediaType)
		record.Artwork = dedupePlexArtwork(record.Artwork)
		records[key] = record
	}

	s.plexArtworkMu.Lock()
	s.plexArtwork = records
	s.plexArtworkMu.Unlock()
	return nil
}

func (s *appState) savePlexArtworkCacheSnapshot(records []plexArtworkCacheRecord) error {
	sort.SliceStable(records, func(i, j int) bool {
		left := plexArtworkKey(records[i].MediaType, records[i].TMDBID, records[i].IMDBID, records[i].Title, records[i].Year)
		right := plexArtworkKey(records[j].MediaType, records[j].TMDBID, records[j].IMDBID, records[j].Title, records[j].Year)
		return left < right
	})
	file := plexArtworkCacheFile{
		UpdatedAt: time.Now().UTC(),
		Items:     records,
	}
	data, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(s.dataDir, "plex_artwork_cache.json"), data, 0o600)
}

func (s *appState) plexArtworkDashboardStats() plexArtworkDashboardStats {
	s.plexArtworkMu.RLock()
	records := make([]plexArtworkCacheRecord, 0, len(s.plexArtwork))
	for _, record := range s.plexArtwork {
		records = append(records, record)
	}
	s.plexArtworkMu.RUnlock()

	stats := plexArtworkDashboardStats{
		Total:        len(records),
		Running:      s.isPlexArtworkSyncRunning(),
		HasPlexToken: s.plexAccountToken() != "",
		LastSync:     s.lastPlexArtworkSyncStats(),
	}
	for _, record := range records {
		switch strings.ToLower(strings.TrimSpace(record.Status)) {
		case "ok":
			stats.OK++
			if plexArtworkHasCleanLandscape(record.Artwork) {
				stats.CleanLandscape++
			} else if len(record.Artwork.Background) > 0 {
				stats.BackdropOnly++
			}
			if len(record.Artwork.ClearLogo) > 0 {
				stats.WithLogo++
			}
			if isSignedPlexArtworkSource(record.SourcePage) {
				stats.SignedDiscover++
			} else if strings.TrimSpace(record.SourcePage) != "" {
				stats.PublicFallback++
			}
		case "miss":
			stats.Miss++
		case "error":
			stats.Error++
		}
		if record.FetchedAt.After(stats.LastFetchedAt) {
			stats.LastFetchedAt = record.FetchedAt
		}
		if record.UpdatedAt.After(stats.LastUpdatedAt) {
			stats.LastUpdatedAt = record.UpdatedAt
		}
	}
	return stats
}

func isSignedPlexArtworkSource(source string) bool {
	normalized := strings.ToLower(strings.TrimSpace(source))
	return strings.HasPrefix(normalized, "plex-discover:") || strings.Contains(normalized, "discover.provider.plex.tv")
}

func (s *appState) isPlexArtworkSyncRunning() bool {
	s.plexArtworkSyncStateMu.RLock()
	defer s.plexArtworkSyncStateMu.RUnlock()
	return s.plexArtworkSyncRunning
}

func (s *appState) setPlexArtworkSyncRunning(running bool) {
	s.plexArtworkSyncStateMu.Lock()
	s.plexArtworkSyncRunning = running
	s.plexArtworkSyncStateMu.Unlock()
}

func (s *appState) rememberPlexArtworkSyncStats(stats *plexArtworkSyncStats) {
	if stats == nil {
		return
	}
	clone := *stats
	s.plexArtworkSyncStateMu.Lock()
	s.plexArtworkLastSync = &clone
	s.plexArtworkSyncStateMu.Unlock()
}

func (s *appState) lastPlexArtworkSyncStats() *plexArtworkSyncStats {
	s.plexArtworkSyncStateMu.RLock()
	defer s.plexArtworkSyncStateMu.RUnlock()
	if s.plexArtworkLastSync == nil {
		return nil
	}
	clone := *s.plexArtworkLastSync
	return &clone
}

func (s *appState) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
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
	if strings.HasPrefix(r.URL.Path, "/api/") {
		http.NotFound(w, r)
		return
	}
	if s.serveWebApp(w, r) {
		return
	}
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, indexHTML)
}

func (s *appState) serveWebApp(w http.ResponseWriter, r *http.Request) bool {
	dist := firstNonEmpty(os.Getenv("VORTEXO_WEB_DIST"), "web/dist")
	indexPath := filepath.Join(dist, "index.html")
	if _, err := os.Stat(indexPath); err != nil {
		return false
	}

	if r.URL.Path != "/" {
		name := strings.TrimPrefix(filepath.Clean(r.URL.Path), "/")
		if name != "." && !strings.HasPrefix(name, "..") {
			path := filepath.Join(dist, name)
			if info, err := os.Stat(path); err == nil && !info.IsDir() {
				http.ServeFile(w, r, path)
				return true
			}
		}
	}
	http.ServeFile(w, r, indexPath)
	return true
}

func (s *appState) handleHealth(w http.ResponseWriter, _ *http.Request) {
	respondJSON(w, http.StatusOK, map[string]any{"ok": true, "name": "Vortexo Add-on Server"})
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

func (s *appState) handleWatchSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.mu.RLock()
		watch := s.config.Watch
		count := len(s.watchState.Items)
		updatedAt := s.watchState.UpdatedAt
		s.mu.RUnlock()
		respondJSON(w, http.StatusOK, map[string]any{
			"trakt": map[string]any{
				"client_id":         watch.Trakt.ClientID,
				"has_client_secret": watch.Trakt.ClientSecret != "",
				"has_access_token":  watch.Trakt.AccessToken != "",
				"has_refresh_token": watch.Trakt.RefreshToken != "",
				"up_next_atom_url":  watch.Trakt.UpNextAtomURL,
				"token_expires_at":  watch.Trakt.TokenExpiresAt,
				"last_sync_at":      watch.Trakt.LastSyncAt,
			},
			"watch_state": map[string]any{
				"count":      count,
				"updated_at": updatedAt,
			},
		})
	case http.MethodPost:
		var req watchSettingsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		s.mu.Lock()
		if strings.TrimSpace(req.TraktClientID) != "" {
			s.config.Watch.Trakt.ClientID = strings.TrimSpace(req.TraktClientID)
		}
		if strings.TrimSpace(req.TraktClientSecret) != "" {
			s.config.Watch.Trakt.ClientSecret = strings.TrimSpace(req.TraktClientSecret)
		}
		s.config.Watch.Trakt.UpNextAtomURL = strings.TrimSpace(req.TraktUpNextAtomURL)
		if req.ClearTraktTokens {
			s.config.Watch.Trakt.AccessToken = ""
			s.config.Watch.Trakt.RefreshToken = ""
			s.config.Watch.Trakt.TokenExpiresAt = time.Time{}
		} else {
			if strings.TrimSpace(req.TraktAccessToken) != "" {
				s.config.Watch.Trakt.AccessToken = strings.TrimSpace(req.TraktAccessToken)
			}
			if strings.TrimSpace(req.TraktRefreshToken) != "" {
				s.config.Watch.Trakt.RefreshToken = strings.TrimSpace(req.TraktRefreshToken)
			}
		}
		err := s.saveLocked()
		s.mu.Unlock()
		if err != nil {
			respondError(w, http.StatusInternalServerError, "failed to save watch settings")
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"ok": true})
	default:
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *appState) handleClearWatchHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	s.mu.Lock()
	removed := len(s.watchState.Items)
	s.watchState.Items = []watchStateItem{}
	s.watchMeta = map[string]watchStateMetadataCacheEntry{}
	err := s.saveWatchStateLocked()
	updatedAt := s.watchState.UpdatedAt
	s.mu.Unlock()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "failed to clear watch history")
		return
	}

	respondJSON(w, http.StatusOK, map[string]any{
		"ok":         true,
		"removed":    removed,
		"total":      0,
		"updated_at": updatedAt,
	})
}

func (s *appState) handlePlexSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.mu.RLock()
		plex := s.config.Plex
		s.mu.RUnlock()
		respondJSON(w, http.StatusOK, map[string]any{
			"plex": plexSettingsResponse(plex),
		})
	case http.MethodPost:
		var req plexSettingsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		accessToken := strings.TrimSpace(req.AccessToken)
		var user *plexUser
		if !req.ClearToken && accessToken != "" {
			fetched, err := s.fetchPlexUser(r.Context(), accessToken)
			if err != nil {
				respondError(w, http.StatusBadGateway, "Plex token validation failed: "+err.Error())
				return
			}
			user = fetched
		}

		s.mu.Lock()
		if req.ClearToken {
			s.config.Plex.AccessToken = ""
			s.config.Plex.UserID = 0
			s.config.Plex.Username = ""
			s.config.Plex.Email = ""
			s.config.Plex.Title = ""
			s.config.Plex.LastSignedInAt = time.Time{}
		} else if accessToken != "" {
			s.config.Plex.AccessToken = accessToken
			s.applyPlexUserLocked(user)
			s.config.Plex.LastSignedInAt = time.Now().UTC()
		}
		plex := s.config.Plex
		err := s.saveLocked()
		s.mu.Unlock()
		if err != nil {
			respondError(w, http.StatusInternalServerError, "failed to save Plex settings")
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"ok": true, "plex": plexSettingsResponse(plex)})
	default:
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *appState) handlePlexPIN(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	pin, err := s.createPlexPIN(r.Context())
	if err != nil {
		respondError(w, http.StatusBadGateway, err.Error())
		return
	}
	clientID := s.plexClientID()
	authURL := plexAuthURL(clientID, pin.Code)
	respondJSON(w, http.StatusOK, map[string]any{
		"id":                pin.ID,
		"code":              pin.Code,
		"expires_in":        pin.ExpiresIn,
		"expires_at":        pin.ExpiresAt,
		"qr":                pin.QR,
		"verification_url":  "https://plex.tv/link",
		"authorization_url": authURL,
	})
}

func (s *appState) handlePlexPINToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req plexPinTokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.PinID <= 0 {
		respondError(w, http.StatusBadRequest, "pin_id is required")
		return
	}

	pin, err := s.getPlexPIN(r.Context(), req.PinID)
	if err != nil {
		respondError(w, http.StatusBadGateway, err.Error())
		return
	}
	token := strings.TrimSpace(pin.AuthToken)
	if token == "" {
		respondJSON(w, http.StatusOK, map[string]any{
			"ok":            true,
			"authenticated": false,
			"pin": map[string]any{
				"id":         pin.ID,
				"code":       pin.Code,
				"expires_in": pin.ExpiresIn,
				"expires_at": pin.ExpiresAt,
			},
		})
		return
	}

	user, err := s.fetchPlexUser(r.Context(), token)
	if err != nil {
		respondError(w, http.StatusBadGateway, "Plex token validation failed: "+err.Error())
		return
	}

	s.mu.Lock()
	s.config.Plex.AccessToken = token
	s.applyPlexUserLocked(user)
	s.config.Plex.LastSignedInAt = time.Now().UTC()
	plex := s.config.Plex
	err = s.saveLocked()
	s.mu.Unlock()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "failed to save Plex token")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"ok":            true,
		"authenticated": true,
		"plex":          plexSettingsResponse(plex),
	})
}

func (s *appState) handleTraktDeviceCode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req traktDeviceCodeRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	if strings.TrimSpace(req.ClientID) != "" || strings.TrimSpace(req.ClientSecret) != "" {
		s.mu.Lock()
		if strings.TrimSpace(req.ClientID) != "" {
			s.config.Watch.Trakt.ClientID = strings.TrimSpace(req.ClientID)
		}
		if strings.TrimSpace(req.ClientSecret) != "" {
			s.config.Watch.Trakt.ClientSecret = strings.TrimSpace(req.ClientSecret)
		}
		err := s.saveLocked()
		s.mu.Unlock()
		if err != nil {
			respondError(w, http.StatusInternalServerError, "failed to save Trakt settings")
			return
		}
	}
	deviceCode, err := s.createTraktDeviceCode(r.Context())
	if err != nil {
		respondError(w, http.StatusBadGateway, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, deviceCode)
}

func (s *appState) handleTraktDeviceToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req traktDeviceTokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	token, err := s.pollTraktDeviceToken(r.Context(), req.DeviceCode)
	if err != nil {
		respondError(w, http.StatusBadGateway, err.Error())
		return
	}
	go s.syncTraktWatchStateIfConfigured(context.Background(), "device-login")
	respondJSON(w, http.StatusOK, token)
}

func (s *appState) handleTraktWatchSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	items, err := s.syncTraktWatchStateExclusive(r.Context(), "manual")
	if err != nil {
		respondError(w, http.StatusBadGateway, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"imported": len(items),
		"total":    s.watchStateCount(),
	})
}

func (s *appState) handleVortexoWatchState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	limit := watchStateQueryInt(r, "limit", 0)
	if limit <= 0 {
		limit = watchStateQueryInt(r, "item_limit", 0)
	}
	s.mu.RLock()
	state := s.watchState
	if state.Items == nil {
		state.Items = []watchStateItem{}
	} else {
		state.Items = append([]watchStateItem(nil), state.Items...)
	}
	s.mu.RUnlock()
	state.Items = filterUnairedWatchStateItems(state.Items, false)
	if limit > 0 && len(state.Items) > limit {
		state.Items = limitedWatchStateItems(state.Items, limit)
	}
	state.Items = s.enrichWatchStateWithManifestMetadata(r.Context(), state.Items)
	state.Items = filterUnairedWatchStateItems(state.Items, true)
	for i := range state.Items {
		s.applyCachedPlexArtworkToWatchStateItem(&state.Items[i])
	}
	respondJSON(w, http.StatusOK, state)
}

func watchStateQueryInt(r *http.Request, key string, fallback int) int {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

func limitedWatchStateItems(items []watchStateItem, limit int) []watchStateItem {
	if limit <= 0 || len(items) <= limit {
		return items
	}
	sortedItems := append([]watchStateItem(nil), items...)
	sort.SliceStable(sortedItems, func(i, j int) bool {
		return sortedItems[i].UpdatedAt.After(sortedItems[j].UpdatedAt)
	})

	limited := make([]watchStateItem, 0, limit)
	addMatching := func(include func(watchStateItem) bool) {
		for _, item := range sortedItems {
			if len(limited) >= limit {
				return
			}
			if include(item) {
				limited = append(limited, item)
			}
		}
	}

	addMatching(func(item watchStateItem) bool {
		return strings.Contains(strings.ToLower(item.Source), "trakt-up-next")
	})
	addMatching(func(item watchStateItem) bool {
		return !strings.Contains(strings.ToLower(item.Source), "trakt-up-next") &&
			(!item.Watched || item.ProgressPercent > 0 || item.ProgressSeconds > 0)
	})
	addMatching(func(item watchStateItem) bool {
		return item.Watched && item.ProgressPercent <= 0 && item.ProgressSeconds <= 0
	})
	return limited
}

func filterUnairedWatchStateItems(items []watchStateItem, strictUpNextDate bool) []watchStateItem {
	if len(items) == 0 {
		return items
	}
	now := time.Now().UTC()
	filtered := make([]watchStateItem, 0, len(items))
	for _, item := range items {
		if watchStateItemShouldDisplay(item, now, strictUpNextDate) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func watchStateItemShouldDisplay(item watchStateItem, now time.Time, strictUpNextDate bool) bool {
	if strings.ToLower(strings.TrimSpace(item.MediaType)) != "episode" {
		return true
	}

	source := strings.ToLower(strings.TrimSpace(item.Source))
	isUpNext := strings.Contains(source, "trakt-up-next")
	known, aired := watchStateDateHasAired(firstNonEmpty(item.AirDate, item.ReleaseDate), now)
	if known {
		return aired
	}
	if strictUpNextDate && isUpNext {
		return false
	}
	return true
}

func watchStateDateHasAired(rawValue string, now time.Time) (bool, bool) {
	rawValue = strings.TrimSpace(rawValue)
	if rawValue == "" {
		return false, false
	}
	if parsed, err := time.Parse(time.RFC3339, rawValue); err == nil {
		return true, !parsed.After(now.Add(6*time.Hour))
	}
	if len(rawValue) > len("2006-01-02") {
		rawValue = rawValue[:len("2006-01-02")]
	}
	parsed, err := time.Parse("2006-01-02", rawValue)
	if err != nil {
		return false, false
	}
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	return true, !parsed.After(today)
}

func (s *appState) handleVortexoWatchStateMark(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPatch {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req watchStateMarkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Watched == nil {
		respondError(w, http.StatusBadRequest, "watched is required")
		return
	}

	item := watchStateItemFromMarkRequest(req)
	key := watchStateKey(item)
	if key == "" {
		respondError(w, http.StatusBadRequest, "watch-state identity is incomplete")
		return
	}

	now := time.Now().UTC()
	s.mu.Lock()
	item.ID = key
	item.UpdatedAt = now
	item.Source = mergeSourceLabel(item.Source, "vortexo-manual")

	if idx := s.watchStateIndexLocked(key, req); idx >= 0 {
		item = mergeWatchStateItem(s.watchState.Items[idx], item)
		item = applyWatchStateMark(item, *req.Watched, now)
		s.watchState.Items[idx] = item
	} else {
		item = applyWatchStateMark(item, *req.Watched, now)
		s.watchState.Items = append(s.watchState.Items, item)
	}

	sort.SliceStable(s.watchState.Items, func(i, j int) bool {
		return s.watchState.Items[i].UpdatedAt.After(s.watchState.Items[j].UpdatedAt)
	})
	if s.watchMeta != nil {
		delete(s.watchMeta, key)
	}
	err := s.saveWatchStateLocked()
	s.mu.Unlock()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "failed to save watch state")
		return
	}
	go s.pushWatchStateMarkToTraktAndRefresh(item)

	respondJSON(w, http.StatusOK, map[string]any{
		"ok":   true,
		"item": item,
	})
}

func (s *appState) enrichWatchStateWithManifestMetadata(ctx context.Context, items []watchStateItem) []watchStateItem {
	if len(items) == 0 {
		return items
	}

	enriched := append([]watchStateItem(nil), items...)
	limit := minInt(len(enriched), watchStateEnrichmentLimit)
	sem := make(chan struct{}, watchStateEnrichmentConcurrency)
	var wg sync.WaitGroup

	for i := 0; i < limit; i++ {
		if !watchStateCanUseManifestMetadata(enriched[i]) {
			continue
		}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return enriched
		}

		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			defer func() { <-sem }()
			enriched[index] = s.enrichWatchStateItemWithManifestMetadata(ctx, enriched[index])
		}(i)
	}

	wg.Wait()
	return enriched
}

func watchStateCanUseManifestMetadata(item watchStateItem) bool {
	mediaType := strings.ToLower(strings.TrimSpace(item.MediaType))
	if mediaType != "movie" && mediaType != "episode" {
		return false
	}
	return len(watchStateManifestIDs(item)) > 0
}

func (s *appState) enrichWatchStateItemWithManifestMetadata(ctx context.Context, item watchStateItem) watchStateItem {
	key := watchStateKey(item)
	if key == "" {
		return item
	}

	now := time.Now()
	s.mu.RLock()
	if cached, ok := s.watchMeta[key]; ok && now.Before(cached.expires) {
		s.mu.RUnlock()
		return mergeWatchStateAddonMetadata(item, cached.item)
	}
	s.mu.RUnlock()

	itemCtx, cancel := context.WithTimeout(ctx, watchStateMetadataTimeout)
	defer cancel()

	enriched, ok := s.lookupWatchStateManifestMetadata(itemCtx, item)
	if !ok {
		return item
	}

	s.mu.Lock()
	if s.watchMeta == nil {
		s.watchMeta = map[string]watchStateMetadataCacheEntry{}
	}
	s.watchMeta[key] = watchStateMetadataCacheEntry{
		item:    enriched,
		expires: now.Add(watchStateMetadataCacheTTL),
	}
	s.mu.Unlock()

	return enriched
}

func (s *appState) lookupWatchStateManifestMetadata(ctx context.Context, item watchStateItem) (watchStateItem, bool) {
	mediaType := strings.ToLower(strings.TrimSpace(item.MediaType))
	manifestType := mediaType
	if manifestType == "episode" {
		manifestType = "series"
	}

	for _, id := range watchStateManifestIDs(item) {
		meta, err := s.findManifestMeta(ctx, manifestType, id)
		if err != nil {
			continue
		}
		enriched := applyWatchStateManifestMetadata(item, meta)
		if watchStateHasManifestMetadata(enriched) {
			return enriched, true
		}
	}
	return item, false
}

func applyWatchStateManifestMetadata(item watchStateItem, meta stremioMeta) watchStateItem {
	mediaType := strings.ToLower(strings.TrimSpace(item.MediaType))
	fallbackType := mediaType
	if fallbackType == "episode" {
		fallbackType = "series"
	}

	homeItem := homeItemFromStremio(meta, fallbackType)
	if mediaType == "movie" {
		item.Title = firstNonEmpty(homeItem.Title, item.Title)
	} else {
		item.ParentTitle = firstNonEmpty(item.ParentTitle, homeItem.Title)
	}
	item.Year = firstNonZero(item.Year, homeItem.Year)
	item.IMDBID = firstNonEmpty(item.IMDBID, homeItem.IMDBID)
	item.TMDBID = firstNonZero(item.TMDBID, homeItem.TMDBID)
	item.Overview = firstNonEmpty(item.Overview, homeItem.Overview)
	item.PosterPath = firstNonEmpty(item.PosterPath, homeItem.PosterPath)
	item.BackdropPath = firstNonEmpty(item.BackdropPath, homeItem.BackdropPath)
	item.LandscapePath = firstNonEmpty(item.LandscapePath, homeItem.LandscapePath)
	item.LogoPath = firstNonEmpty(item.LogoPath, homeItem.LogoPath)
	item.ReleaseDate = firstNonEmpty(item.ReleaseDate, homeItem.ReleaseDate, homeItem.FirstAirDate)
	item.Runtime = firstNonZero(item.Runtime, homeItem.Runtime)
	if len(item.Genres) == 0 {
		item.Genres = uniqueNonEmptyStrings(homeItem.Genres)
	}
	if item.VoteAverage == 0 {
		item.VoteAverage = homeItem.VoteAverage
	}

	if mediaType == "episode" {
		if video, ok := matchingStremioEpisodeVideo(meta, item.Season, item.Episode); ok {
			item.Title = firstNonEmpty(video.Title, video.Name, item.Title)
			item.Overview = firstNonEmpty(video.Overview, video.Description, item.Overview)
			item.StillPath = firstNonEmpty(item.StillPath, video.Thumbnail, video.Poster)
			item.LandscapePath = firstNonEmpty(item.LandscapePath, homeItem.LandscapePath)
			item.AirDate = firstNonEmpty(item.AirDate, dateFromText(firstNonEmpty(video.Released, video.FirstAired)))
			item.Runtime = firstNonZero(runtimeMinutes(video.Runtime), item.Runtime)
		}
		item.AirDate = firstNonEmpty(item.AirDate, item.ReleaseDate)
	}

	if item.DurationSeconds == 0 && item.Runtime > 0 {
		item.DurationSeconds = item.Runtime * 60
	}
	return item
}

func matchingStremioEpisodeVideo(meta stremioMeta, season int, episode int) (stremioVideo, bool) {
	if season <= 0 || episode <= 0 {
		return stremioVideo{}, false
	}
	for _, video := range meta.Videos {
		videoSeason := intFromAny(video.Season)
		videoEpisode := intFromAny(video.Episode)
		if videoSeason == 0 || videoEpisode == 0 {
			idSeason, idEpisode := seasonEpisodeFromVideoID(video.ID)
			if videoSeason == 0 {
				videoSeason = idSeason
			}
			if videoEpisode == 0 {
				videoEpisode = idEpisode
			}
		}
		if videoSeason == season && videoEpisode == episode {
			return video, true
		}
	}
	return stremioVideo{}, false
}

func watchStateManifestIDs(item watchStateItem) []string {
	var ids []string
	if item.IMDBID != "" {
		ids = append(ids, item.IMDBID)
	}
	if item.TMDBID > 0 {
		tmdbID := strconv.Itoa(item.TMDBID)
		ids = append(ids, "tmdb:"+tmdbID, tmdbID)
	}
	if item.TVDBID > 0 {
		ids = append(ids, "tvdb:"+strconv.Itoa(item.TVDBID))
	}
	if id := imdbFromID(item.ID); id != "" {
		ids = append(ids, id)
	}
	return uniqueNonEmptyStrings(ids)
}

func watchStateHasManifestMetadata(item watchStateItem) bool {
	return firstNonEmpty(
		item.Overview,
		item.PosterPath,
		item.BackdropPath,
		item.LandscapePath,
		item.LogoPath,
		item.StillPath,
	) != ""
}

func mergeWatchStateAddonMetadata(base watchStateItem, metadata watchStateItem) watchStateItem {
	base.Title = firstNonEmpty(metadata.Title, base.Title)
	base.ParentTitle = firstNonEmpty(base.ParentTitle, metadata.ParentTitle)
	base.Year = firstNonZero(base.Year, metadata.Year)
	base.IMDBID = firstNonEmpty(base.IMDBID, metadata.IMDBID)
	base.TMDBID = firstNonZero(base.TMDBID, metadata.TMDBID)
	base.TVDBID = firstNonZero(base.TVDBID, metadata.TVDBID)
	base.Overview = firstNonEmpty(metadata.Overview, base.Overview)
	base.PosterPath = firstNonEmpty(metadata.PosterPath, base.PosterPath)
	base.BackdropPath = firstNonEmpty(metadata.BackdropPath, base.BackdropPath)
	base.LandscapePath = firstNonEmpty(metadata.LandscapePath, base.LandscapePath)
	base.LogoPath = firstNonEmpty(metadata.LogoPath, base.LogoPath)
	base.StillPath = firstNonEmpty(metadata.StillPath, base.StillPath)
	base.ReleaseDate = firstNonEmpty(metadata.ReleaseDate, base.ReleaseDate)
	base.AirDate = firstNonEmpty(metadata.AirDate, base.AirDate)
	base.Runtime = firstNonZero(base.Runtime, metadata.Runtime)
	if len(base.Genres) == 0 {
		base.Genres = uniqueNonEmptyStrings(metadata.Genres)
	}
	if base.VoteAverage == 0 {
		base.VoteAverage = metadata.VoteAverage
	}
	if base.DurationSeconds == 0 && base.Runtime > 0 {
		base.DurationSeconds = base.Runtime * 60
	}
	return base
}

func (s *appState) handleCapabilities(w http.ResponseWriter, _ *http.Request) {
	respondJSON(w, http.StatusOK, map[string]any{
		"name":                 "Vortexo Add-on Server",
		"home":                 true,
		"source_api":           true,
		"playback":             true,
		"metadata":             true,
		"seasons":              true,
		"episodes":             true,
		"live_tv":              true,
		"watch_history":        true,
		"manifest_bridge":      true,
		"requires_app_changes": false,
		"types":                []string{"movie", "show", "season", "episode", "live_tv", "watch_history"},
	})
}

func (s *appState) handleBridgeDashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	s.mu.RLock()
	installed := append([]installedManifest(nil), s.config.Manifests...)
	prefs := catalogPreferenceMapLocked(s.config.Catalogs)
	registryURL := firstNonEmpty(s.config.AddonRegistryURL, defaultRegistryURL)
	watch := s.config.Watch
	tmdbKeywordRows := s.config.TMDBKeywordRows
	watchCount := len(s.watchState.Items)
	watchUpdatedAt := s.watchState.UpdatedAt
	s.mu.RUnlock()

	manifests := make([]dashboardManifest, 0, len(installed))
	allCatalogs := make([]dashboardManifestCatalog, 0)
	catalogOrder := 0
	for _, item := range installed {
		entry := dashboardManifest{
			ID:        item.ID,
			Name:      item.Name,
			URL:       item.URL,
			Enabled:   item.Enabled,
			CreatedAt: item.CreatedAt,
			UpdatedAt: item.UpdatedAt,
			Status:    "disabled",
			Resources: []string{},
			Types:     []string{},
			Catalogs:  []dashboardManifestCatalog{},
		}
		if !item.Enabled {
			manifests = append(manifests, entry)
			continue
		}

		manifest, _, err := s.fetchManifest(r.Context(), item.URL, false)
		if err != nil {
			entry.Status = "error"
			entry.Message = err.Error()
			manifests = append(manifests, entry)
			continue
		}

		entry.Status = "ok"
		entry.Name = firstNonEmpty(item.Name, manifest.Name, entry.ID)
		entry.Description = manifest.Description
		entry.Version = manifest.Version
		entry.Logo = manifest.Logo
		entry.Resources = manifestResourceNames(manifest)
		entry.Types = append([]string(nil), manifest.Types...)
		entry.Capabilities = manifestCapabilities(manifest)
		entry.Catalogs = dashboardCatalogs(item, manifest, prefs, catalogOrder)
		catalogOrder += len(manifest.Catalogs)
		allCatalogs = append(allCatalogs, entry.Catalogs...)
		manifests = append(manifests, entry)
	}
	sortDashboardCatalogs(allCatalogs)

	respondJSON(w, http.StatusOK, map[string]any{
		"server": map[string]any{
			"name": "Vortexo Add-on Server",
			"time": time.Now().UTC(),
		},
		"manifests":         manifests,
		"catalogs":          allCatalogs,
		"registry_url":      registryURL,
		"artwork":           s.plexArtworkDashboardStats(),
		"tmdb_keyword_rows": tmdbKeywordRowsDashboardSummary(tmdbKeywordRows, time.Now().UTC()),
		"watch": map[string]any{
			"count":               watchCount,
			"updated_at":          watchUpdatedAt,
			"trakt_connected":     watch.Trakt.AccessToken != "",
			"trakt_last_sync_at":  watch.Trakt.LastSyncAt,
			"trakt_client_config": watch.Trakt.ClientID != "",
		},
	})
}

func (s *appState) handleAddonRegistry(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.mu.RLock()
		registryURL := firstNonEmpty(s.config.AddonRegistryURL, defaultRegistryURL)
		installed := append([]installedManifest(nil), s.config.Manifests...)
		s.mu.RUnlock()

		if override := strings.TrimSpace(r.URL.Query().Get("registry_url")); override != "" {
			registryURL = normalizeManifestURL(override)
		}
		registryURL = normalizeManifestURL(registryURL)
		if registryURL == "" {
			respondError(w, http.StatusBadRequest, "registry URL is invalid")
			return
		}

		query := strings.TrimSpace(r.URL.Query().Get("q"))
		capability := strings.TrimSpace(r.URL.Query().Get("capability"))
		mediaType := strings.TrimSpace(r.URL.Query().Get("type"))
		limit := boundedInt(r.URL.Query().Get("limit"), 80, 1, 250)

		manifest, base, err := s.fetchManifest(r.Context(), registryURL, false)
		if err != nil {
			respondError(w, http.StatusBadGateway, "add-on registry failed: "+err.Error())
			return
		}

		catalogs := manifest.AddonCatalogs
		if len(catalogs) == 0 {
			catalogs = manifest.Catalogs
		}
		installedURLs := installedManifestURLSet(installed)
		addons := make([]dashboardAddon, 0, limit)
		seen := map[string]bool{}
		for _, catalog := range catalogs {
			if len(addons) >= limit {
				break
			}
			entries, err := s.fetchAddonCatalog(r.Context(), base, catalog, limit*2)
			if err != nil {
				log.Printf("add-on registry catalog %s/%s failed: %v", catalog.Type, catalog.ID, err)
				continue
			}
			for _, entry := range entries {
				addon := dashboardAddonFromEntry(entry, installedURLs)
				if addon.URL == "" || addon.Name == "" {
					continue
				}
				key := strings.ToLower(addon.URL)
				if seen[key] {
					continue
				}
				if !addonMatchesFilters(addon, query, capability, mediaType) {
					continue
				}
				seen[key] = true
				addons = append(addons, addon)
				if len(addons) >= limit {
					break
				}
			}
		}
		sort.SliceStable(addons, func(i, j int) bool {
			if addons[i].Installed != addons[j].Installed {
				return addons[i].Installed
			}
			return strings.ToLower(addons[i].Name) < strings.ToLower(addons[j].Name)
		})
		respondJSON(w, http.StatusOK, map[string]any{
			"registry_url": registryURL,
			"addons":       addons,
		})
	case http.MethodPost:
		var req struct {
			RegistryURL string `json:"registry_url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		registryURL := normalizeManifestURL(firstNonEmpty(req.RegistryURL, defaultRegistryURL))
		if registryURL == "" {
			respondError(w, http.StatusBadRequest, "registry URL is invalid")
			return
		}
		if _, _, err := s.fetchManifest(r.Context(), registryURL, true); err != nil {
			respondError(w, http.StatusBadGateway, "registry validation failed: "+err.Error())
			return
		}
		s.mu.Lock()
		s.config.AddonRegistryURL = registryURL
		err := s.saveLocked()
		s.mu.Unlock()
		if err != nil {
			respondError(w, http.StatusInternalServerError, "failed to save registry URL")
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"ok": true, "registry_url": registryURL})
	default:
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *appState) handleCatalogPreferences(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		respondJSON(w, http.StatusOK, map[string]any{"catalogs": s.dashboardCatalogs(r.Context())})
	case http.MethodPost:
		var req struct {
			Catalogs []catalogPreference `json:"catalogs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		now := time.Now().UTC()
		next := make([]catalogPreference, 0, len(req.Catalogs))
		seen := map[string]bool{}
		for _, item := range req.Catalogs {
			key := strings.TrimSpace(item.Key)
			if key == "" {
				key = catalogKey(item.ManifestID, item.CatalogType, item.CatalogID)
			}
			if key == "" || seen[key] {
				continue
			}
			manifestID, catalogType, catalogID := splitCatalogKey(key)
			item.Key = key
			item.ManifestID = firstNonEmpty(strings.TrimSpace(item.ManifestID), manifestID)
			item.CatalogType = firstNonEmpty(strings.TrimSpace(item.CatalogType), catalogType)
			item.CatalogID = firstNonEmpty(strings.TrimSpace(item.CatalogID), catalogID)
			item.Name = strings.TrimSpace(item.Name)
			item.UpdatedAt = now
			seen[key] = true
			next = append(next, item)
		}
		sort.SliceStable(next, func(i, j int) bool {
			if next[i].SortOrder != next[j].SortOrder {
				return next[i].SortOrder < next[j].SortOrder
			}
			return next[i].Key < next[j].Key
		})
		s.mu.Lock()
		s.config.Catalogs = next
		err := s.saveLocked()
		s.mu.Unlock()
		if err != nil {
			respondError(w, http.StatusInternalServerError, "failed to save catalogs")
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"ok": true, "catalogs": s.dashboardCatalogs(r.Context())})
	default:
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *appState) handleTMDBKeywordRowsSetup(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.mu.RLock()
		config := s.config.TMDBKeywordRows
		s.mu.RUnlock()
		respondJSON(w, http.StatusOK, tmdbKeywordRowsDashboardSummary(config, time.Now().UTC()))
	case http.MethodPost:
		var req tmdbKeywordRowsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		s.mu.Lock()
		next := s.config.TMDBKeywordRows
		next.Enabled = req.Enabled
		next.RowCount = req.RowCount
		next.Language = firstNonEmpty(req.Language, next.Language)
		next.Region = firstNonEmpty(req.Region, next.Region)
		if req.ClearCredentials {
			next.TMDBAPIKey = ""
			next.TMDBAccessToken = ""
		}
		if strings.TrimSpace(req.TMDBAPIKey) != "" {
			next.TMDBAPIKey = strings.TrimSpace(req.TMDBAPIKey)
		}
		if strings.TrimSpace(req.TMDBAccessToken) != "" {
			next.TMDBAccessToken = strings.TrimSpace(req.TMDBAccessToken)
		}
		next.UpdatedAt = time.Now().UTC()
		next = normalizeTMDBKeywordRowsConfig(next)
		if req.Enabled && !tmdbKeywordRowsHasCredentials(next) {
			s.mu.Unlock()
			respondError(w, http.StatusBadRequest, "TMDB API key or read access token is required")
			return
		}
		s.config.TMDBKeywordRows = next
		err := s.saveLocked()
		s.mu.Unlock()
		if err != nil {
			respondError(w, http.StatusInternalServerError, "failed to save TMDB keyword rows")
			return
		}

		s.clearTMDBKeywordRowsCache()
		respondJSON(w, http.StatusOK, tmdbKeywordRowsDashboardSummary(next, time.Now().UTC()))
	default:
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *appState) handleStreamingCatalogsSetup(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		respondJSON(w, http.StatusOK, map[string]any{
			"providers":         streamingCatalogDashboardProviders(),
			"default_providers": streamingCatalogDefaultProviders,
			"default_types":     streamingCatalogDefaultTypes,
			"default_sort_by":   streamingCatalogDefaultSortBy,
			"sort_options":      streamingCatalogSortOptions(),
		})
	case http.MethodPost:
		var req streamingCatalogSetupRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		config := normalizeStreamingCatalogConfig(streamingCatalogAddonConfig{
			Providers:      req.Providers,
			Types:          req.Types,
			MergeProviders: req.MergeProviders,
			MergeAll:       req.MergeAll,
			SortBy:         req.SortBy,
			RPDBKey:        req.RPDBKey,
		})
		manifestURL := streamingCatalogManifestURL(requestPublicBaseURL(r), config)
		response := streamingCatalogSetupResponse{
			OK:          true,
			ManifestURL: manifestURL,
		}
		if req.Install {
			manifest, err := s.installManifest(r.Context(), installedManifest{
				ID:      "vortexo-streaming-catalogs",
				Name:    streamingCatalogAddonName,
				URL:     manifestURL,
				Enabled: true,
			})
			if err != nil {
				respondError(w, http.StatusBadGateway, err.Error())
				return
			}
			response.Manifest = manifest
		}
		respondJSON(w, http.StatusOK, response)
	default:
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *appState) handleStreamingCatalogsAddon(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	tail := strings.Trim(strings.TrimPrefix(r.URL.Path, streamingCatalogAddonPath), "/")
	if tail == "" || tail == "manifest.json" {
		s.handleStreamingCatalogsManifest(w, defaultStreamingCatalogConfig())
		return
	}

	parts := strings.Split(tail, "/")
	if len(parts) == 2 && parts[1] == "manifest.json" {
		config, err := decodeStreamingCatalogConfig(parts[0])
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid streaming catalogs configuration")
			return
		}
		s.handleStreamingCatalogsManifest(w, config)
		return
	}

	if len(parts) >= 4 && parts[1] == "catalog" {
		config, err := decodeStreamingCatalogConfig(parts[0])
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid streaming catalogs configuration")
			return
		}
		catalogType := normalizeStreamingCatalogCatalogType(parts[2])
		catalogID := strings.TrimSuffix(parts[3], ".json")
		if catalogType == "" || catalogID == "" {
			respondError(w, http.StatusBadRequest, "invalid streaming catalog request")
			return
		}
		s.handleStreamingCatalogsCatalog(w, r, config, catalogType, catalogID)
		return
	}

	respondError(w, http.StatusNotFound, "streaming catalogs route not found")
}

func (s *appState) handleStreamingCatalogsManifest(w http.ResponseWriter, config streamingCatalogAddonConfig) {
	respondJSON(w, http.StatusOK, streamingCatalogManifest(config))
}

func (s *appState) handleStreamingCatalogsCatalog(w http.ResponseWriter, r *http.Request, config streamingCatalogAddonConfig, catalogType string, providerID string) {
	if !streamingCatalogConfigIncludes(config, catalogType, providerID) {
		respondJSON(w, http.StatusOK, map[string]any{"metas": []stremioMeta{}})
		return
	}
	items, err := s.streamingCatalogCatalogMetas(r.Context(), config, catalogType, providerID)
	if err != nil {
		log.Printf("streaming catalog %s/%s failed: %v", catalogType, providerID, err)
		respondJSON(w, http.StatusOK, map[string]any{"metas": []stremioMeta{}})
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{"metas": applyStreamingCatalogRPDB(config.RPDBKey, items)})
}

func streamingCatalogDashboardProviders() []map[string]any {
	providers := make([]map[string]any, 0, len(streamingCatalogProviderOrder))
	for _, id := range streamingCatalogProviderOrder {
		provider, ok := streamingCatalogProviders[id]
		if !ok {
			continue
		}
		providers = append(providers, map[string]any{
			"id":    provider.ID,
			"name":  provider.Name,
			"types": provider.Types,
		})
	}
	return providers
}

func streamingCatalogSortOptions() []map[string]string {
	return []map[string]string{
		{"id": "TRENDING", "name": "Trending"},
		{"id": "POPULAR", "name": "Popular"},
		{"id": "NEW", "name": "New"},
	}
}

func streamingCatalogManifest(config streamingCatalogAddonConfig) stremioManifest {
	config = normalizeStreamingCatalogConfig(config)
	rows := streamingCatalogRows(config)
	return stremioManifest{
		ID:          streamingCatalogAddonID,
		Name:        streamingCatalogAddonName,
		Description: "Trending movie and series rows from Netflix, Disney+, HBO Max, Prime Video, Apple TV+, and more.",
		Version:     "1.0.0",
		Logo:        "https://play-lh.googleusercontent.com/TBRwjS_qfJCSj1m7zZB93FnpJM5fSpMA_wUlFDLxWAb45T9RmwBvQd5cWR5viJJOhkI",
		Resources:   []any{"catalog"},
		Types:       streamingCatalogManifestTypes(config, rows),
		Catalogs:    rows,
		BehaviorHints: map[string]any{
			"configurable": false,
		},
	}
}

func streamingCatalogRows(config streamingCatalogAddonConfig) []stremioCatalog {
	config = normalizeStreamingCatalogConfig(config)
	if config.MergeAll {
		enabledTypes := streamingCatalogEnabledTypes(config)
		if len(enabledTypes) == 0 {
			return []stremioCatalog{}
		}
		return []stremioCatalog{{
			ID:   streamingCatalogLatestReleasesID,
			Type: streamingCatalogMergedProviderRowType(enabledTypes),
			Name: "Latest Releases",
		}}
	}

	if config.MergeProviders {
		rows := make([]stremioCatalog, 0, len(config.Providers))
		for _, providerID := range config.Providers {
			provider, ok := streamingCatalogProviders[providerID]
			if !ok {
				continue
			}
			enabledTypes := streamingCatalogProviderEnabledTypes(config, provider)
			if len(enabledTypes) == 0 {
				continue
			}
			rows = append(rows, stremioCatalog{
				ID:   provider.ID,
				Type: streamingCatalogMergedProviderRowType(enabledTypes),
				Name: provider.Name,
			})
		}
		return rows
	}

	typeSet := map[string]bool{}
	for _, catalogType := range config.Types {
		typeSet[catalogType] = true
	}
	rows := make([]stremioCatalog, 0, len(config.Providers)*len(config.Types))
	for _, providerID := range config.Providers {
		provider, ok := streamingCatalogProviders[providerID]
		if !ok {
			continue
		}
		for _, catalogType := range config.Types {
			if !typeSet[catalogType] || !streamingProviderSupportsType(provider, catalogType) {
				continue
			}
			rows = append(rows, stremioCatalog{
				ID:   provider.ID,
				Type: catalogType,
				Name: provider.Name,
			})
		}
	}
	return rows
}

func streamingCatalogManifestTypes(config streamingCatalogAddonConfig, rows []stremioCatalog) []string {
	seen := map[string]bool{}
	types := make([]string, 0, len(config.Types)+len(rows))
	for _, catalogType := range config.Types {
		if catalogType == "" || seen[catalogType] {
			continue
		}
		seen[catalogType] = true
		types = append(types, catalogType)
	}
	for _, row := range rows {
		catalogType := strings.TrimSpace(row.Type)
		if catalogType == "" || seen[catalogType] {
			continue
		}
		seen[catalogType] = true
		types = append(types, catalogType)
	}
	return types
}

func streamingCatalogConfigIncludes(config streamingCatalogAddonConfig, catalogType string, providerID string) bool {
	config = normalizeStreamingCatalogConfig(config)
	catalogType = normalizeStreamingCatalogCatalogType(catalogType)
	providerID = canonicalStreamingProviderID(providerID)
	if providerID == streamingCatalogLatestReleasesID {
		return config.MergeAll && streamingCatalogMatchesMergedRowType(catalogType, streamingCatalogEnabledTypes(config))
	}
	if providerID == streamingCatalogMergedID {
		return config.MergeProviders && streamingCatalogHasProviderForType(config, catalogType)
	}
	if config.MergeAll {
		return false
	}
	provider, ok := streamingCatalogProviders[providerID]
	if !ok {
		return false
	}
	providerEnabled := false
	for _, id := range config.Providers {
		if id == providerID {
			providerEnabled = true
			break
		}
	}
	if !providerEnabled {
		return false
	}
	if config.MergeProviders {
		enabledTypes := streamingCatalogProviderEnabledTypes(config, provider)
		return streamingCatalogMatchesMergedRowType(catalogType, enabledTypes)
	}
	if !streamingProviderSupportsType(provider, catalogType) {
		return false
	}
	for _, enabledType := range config.Types {
		if enabledType == catalogType {
			return true
		}
	}
	return false
}

func streamingCatalogEnabledTypes(config streamingCatalogAddonConfig) []string {
	enabled := make([]string, 0, len(config.Types))
	for _, catalogType := range config.Types {
		if streamingCatalogHasProviderForType(config, catalogType) {
			enabled = append(enabled, catalogType)
		}
	}
	return enabled
}

func streamingCatalogProviderEnabledTypes(config streamingCatalogAddonConfig, provider streamingCatalogProvider) []string {
	enabled := make([]string, 0, len(config.Types))
	for _, catalogType := range config.Types {
		if streamingProviderSupportsType(provider, catalogType) {
			enabled = append(enabled, catalogType)
		}
	}
	return enabled
}

func streamingCatalogMergedProviderRowType(types []string) string {
	if len(types) > 1 {
		for _, catalogType := range types {
			if catalogType == "movie" {
				return "movie"
			}
		}
		return types[0]
	}
	if len(types) == 1 {
		return types[0]
	}
	return ""
}

func streamingCatalogMatchesMergedRowType(catalogType string, enabledTypes []string) bool {
	return catalogType == streamingCatalogMergedProviderRowType(enabledTypes) || catalogType == "mixed"
}

func streamingCatalogHasProviderForType(config streamingCatalogAddonConfig, catalogType string) bool {
	catalogType = normalizeStreamingCatalogCatalogType(catalogType)
	if catalogType == "" {
		return false
	}
	if catalogType == "mixed" {
		for _, providerID := range config.Providers {
			provider, ok := streamingCatalogProviders[providerID]
			if ok && len(streamingCatalogProviderEnabledTypes(config, provider)) > 1 {
				return true
			}
		}
		return false
	}
	typeEnabled := false
	for _, enabledType := range config.Types {
		if enabledType == catalogType {
			typeEnabled = true
			break
		}
	}
	if !typeEnabled {
		return false
	}
	for _, providerID := range config.Providers {
		provider, ok := streamingCatalogProviders[providerID]
		if ok && streamingProviderSupportsType(provider, catalogType) {
			return true
		}
	}
	return false
}

func normalizeStreamingCatalogConfig(config streamingCatalogAddonConfig) streamingCatalogAddonConfig {
	config.Providers = normalizeStreamingCatalogProviders(config.Providers)
	config.Types = normalizeStreamingCatalogTypes(config.Types)
	if config.MergeAll {
		config.MergeProviders = false
	}
	config.SortBy = normalizeStreamingCatalogSortBy(config.SortBy)
	config.RPDBKey = strings.TrimSpace(config.RPDBKey)
	return config
}

func defaultStreamingCatalogConfig() streamingCatalogAddonConfig {
	return normalizeStreamingCatalogConfig(streamingCatalogAddonConfig{
		Providers: append([]string(nil), streamingCatalogDefaultProviders...),
		Types:     append([]string(nil), streamingCatalogDefaultTypes...),
		SortBy:    streamingCatalogDefaultSortBy,
	})
}

func normalizeStreamingCatalogProviders(providerIDs []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(providerIDs))
	for _, raw := range providerIDs {
		id := canonicalStreamingProviderID(raw)
		if id == "" || seen[id] {
			continue
		}
		if _, ok := streamingCatalogProviders[id]; !ok {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	if len(out) > 0 {
		return out
	}
	return append([]string(nil), streamingCatalogDefaultProviders...)
}

func normalizeStreamingCatalogTypes(types []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(types))
	for _, raw := range types {
		catalogType := normalizeStremioType(raw)
		if catalogType == "" || seen[catalogType] {
			continue
		}
		seen[catalogType] = true
		out = append(out, catalogType)
	}
	if len(out) > 0 {
		return out
	}
	return append([]string(nil), streamingCatalogDefaultTypes...)
}

func canonicalStreamingProviderID(providerID string) string {
	switch strings.ToLower(strings.TrimSpace(providerID)) {
	case "pct":
		return "pcp"
	case "fmn":
		return "cru"
	case "hst":
		return "jhs"
	case streamingCatalogLatestReleasesID:
		return streamingCatalogLatestReleasesID
	default:
		return strings.ToLower(strings.TrimSpace(providerID))
	}
}

func normalizeStreamingCatalogSortBy(sortBy string) string {
	switch strings.ToUpper(strings.TrimSpace(sortBy)) {
	case "NEW":
		return "NEW"
	case "POPULAR":
		return "POPULAR"
	case "TRENDING", "":
		return "TRENDING"
	default:
		return streamingCatalogDefaultSortBy
	}
}

func normalizeStreamingCatalogCatalogType(value string) string {
	lower := strings.ToLower(strings.TrimSpace(value))
	switch lower {
	case "mixed", "all":
		return "mixed"
	default:
		return normalizeStremioType(value)
	}
}

func streamingProviderSupportsType(provider streamingCatalogProvider, catalogType string) bool {
	for _, supported := range provider.Types {
		if supported == catalogType {
			return true
		}
	}
	return false
}

func streamingCatalogManifestURL(baseURL string, config streamingCatalogAddonConfig) string {
	config = normalizeStreamingCatalogConfig(config)
	data, _ := json.Marshal(config)
	encoded := base64.RawURLEncoding.EncodeToString(data)
	return strings.TrimRight(baseURL, "/") + streamingCatalogAddonPath + "/" + encoded + "/manifest.json"
}

func decodeStreamingCatalogConfig(encoded string) (streamingCatalogAddonConfig, error) {
	encoded = strings.TrimSpace(encoded)
	if encoded == "" {
		return defaultStreamingCatalogConfig(), nil
	}
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return streamingCatalogAddonConfig{}, err
	}
	var config streamingCatalogAddonConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return streamingCatalogAddonConfig{}, err
	}
	return normalizeStreamingCatalogConfig(config), nil
}

func requestPublicBaseURL(r *http.Request) string {
	proto := firstForwardedValue(r.Header.Get("X-Forwarded-Proto"))
	if proto == "" {
		if r.TLS != nil {
			proto = "https"
		} else {
			proto = "http"
		}
	}
	host := firstForwardedValue(r.Header.Get("X-Forwarded-Host"))
	if host == "" {
		host = r.Host
	}
	return proto + "://" + host
}

func firstForwardedValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if idx := strings.Index(value, ","); idx >= 0 {
		value = value[:idx]
	}
	return strings.TrimSpace(value)
}

func (s *appState) streamingCatalogCatalogMetas(ctx context.Context, config streamingCatalogAddonConfig, catalogType string, catalogID string) ([]stremioMeta, error) {
	config = normalizeStreamingCatalogConfig(config)
	catalogID = canonicalStreamingProviderID(catalogID)
	if catalogID == streamingCatalogLatestReleasesID {
		return s.mergedAllStreamingCatalogMetas(ctx, config)
	}
	if catalogID == streamingCatalogMergedID {
		return s.mergedStreamingCatalogMetas(ctx, config, catalogType)
	}
	if config.MergeProviders {
		return s.mergedStreamingProviderMetas(ctx, config, catalogID)
	}
	return s.streamingCatalogMetas(ctx, catalogType, catalogID, config.SortBy)
}

type streamingCatalogItemGroup struct {
	id    string
	items []stremioMeta
}

func (s *appState) mergedStreamingProviderMetas(ctx context.Context, config streamingCatalogAddonConfig, providerID string) ([]stremioMeta, error) {
	provider, ok := streamingCatalogProviders[providerID]
	if !ok {
		return nil, fmt.Errorf("unsupported streaming provider %s", providerID)
	}
	groups := make([]streamingCatalogItemGroup, 0, len(config.Types))
	for _, catalogType := range streamingCatalogProviderEnabledTypes(config, provider) {
		items, err := s.streamingCatalogMetas(ctx, catalogType, providerID, config.SortBy)
		if err != nil {
			log.Printf("streaming catalog %s/%s merge source failed: %v", catalogType, providerID, err)
			continue
		}
		if len(items) == 0 {
			continue
		}
		groups = append(groups, streamingCatalogItemGroup{id: catalogType, items: items})
	}
	if len(groups) == 0 {
		return []stremioMeta{}, nil
	}
	return mergeStreamingCatalogItemGroups(groups, config.SortBy), nil
}

func (s *appState) mergedAllStreamingCatalogMetas(ctx context.Context, config streamingCatalogAddonConfig) ([]stremioMeta, error) {
	groups := make([]streamingCatalogItemGroup, 0, len(config.Providers)*len(config.Types))
	for _, providerID := range config.Providers {
		provider, ok := streamingCatalogProviders[providerID]
		if !ok {
			continue
		}
		for _, catalogType := range streamingCatalogProviderEnabledTypes(config, provider) {
			items, err := s.streamingCatalogMetas(ctx, catalogType, providerID, config.SortBy)
			if err != nil {
				log.Printf("streaming catalog %s/%s merge-all source failed: %v", catalogType, providerID, err)
				continue
			}
			if len(items) == 0 {
				continue
			}
			groups = append(groups, streamingCatalogItemGroup{id: providerID + ":" + catalogType, items: items})
		}
	}
	if len(groups) == 0 {
		return []stremioMeta{}, nil
	}
	return mergeStreamingCatalogItemGroups(groups, config.SortBy), nil
}

func (s *appState) mergedStreamingCatalogMetas(ctx context.Context, config streamingCatalogAddonConfig, catalogType string) ([]stremioMeta, error) {
	groups := make([]streamingCatalogItemGroup, 0, len(config.Providers))
	for _, providerID := range config.Providers {
		provider, ok := streamingCatalogProviders[providerID]
		if !ok || !streamingProviderSupportsType(provider, catalogType) {
			continue
		}
		items, err := s.streamingCatalogMetas(ctx, catalogType, providerID, config.SortBy)
		if err != nil {
			log.Printf("streaming catalog %s/%s merge source failed: %v", catalogType, providerID, err)
			continue
		}
		if len(items) == 0 {
			continue
		}
		groups = append(groups, streamingCatalogItemGroup{id: providerID, items: items})
	}

	if len(groups) == 0 {
		return []stremioMeta{}, nil
	}
	return mergeStreamingCatalogItemGroups(groups, config.SortBy), nil
}

func mergeStreamingCatalogItemGroups(groups []streamingCatalogItemGroup, sortBy string) []stremioMeta {
	seen := map[string]bool{}
	if normalizeStreamingCatalogSortBy(sortBy) == "NEW" {
		merged := make([]stremioMeta, 0, streamingCatalogJustWatchLimit)
		for _, group := range groups {
			for _, item := range group.items {
				key := imdbFromID(firstNonEmpty(item.IMDBID, item.ID))
				if key == "" {
					key = strings.ToLower(strings.TrimSpace(item.Type + ":" + item.Name))
				}
				if key == "" || seen[key] {
					continue
				}
				seen[key] = true
				merged = append(merged, item)
			}
		}
		sortStreamingCatalogMetasByNewDate(merged)
		if len(merged) > streamingCatalogJustWatchLimit {
			merged = merged[:streamingCatalogJustWatchLimit]
		}
		return merged
	}

	maxItems := 0
	for _, group := range groups {
		if len(group.items) > maxItems {
			maxItems = len(group.items)
		}
	}
	merged := make([]stremioMeta, 0, streamingCatalogJustWatchLimit)
	for index := 0; index < maxItems && len(merged) < streamingCatalogJustWatchLimit; index++ {
		for _, group := range groups {
			if index >= len(group.items) {
				continue
			}
			item := group.items[index]
			key := imdbFromID(firstNonEmpty(item.IMDBID, item.ID))
			if key == "" {
				key = strings.ToLower(strings.TrimSpace(item.Type + ":" + item.Name))
			}
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			merged = append(merged, item)
			if len(merged) >= streamingCatalogJustWatchLimit {
				break
			}
		}
	}
	return merged
}

func (s *appState) streamingCatalogMetas(ctx context.Context, catalogType string, providerID string, sortBy string) ([]stremioMeta, error) {
	providerID = canonicalStreamingProviderID(providerID)
	sortBy = normalizeStreamingCatalogSortBy(sortBy)
	provider, ok := streamingCatalogProviders[providerID]
	if !ok || !streamingProviderSupportsType(provider, catalogType) {
		return nil, fmt.Errorf("unsupported streaming catalog %s/%s", catalogType, providerID)
	}
	country := firstNonEmpty(provider.Countries[catalogType], "US")
	language := firstNonEmpty(provider.Languages[catalogType], "en")
	cacheKey := strings.Join([]string{catalogType, providerID, country, language, sortBy}, ":")
	now := time.Now()

	s.streamingCatalogMu.RLock()
	cached, ok := s.streamingCatalogs[cacheKey]
	s.streamingCatalogMu.RUnlock()
	if ok && now.Before(cached.expires) {
		return append([]stremioMeta(nil), cached.items...), nil
	}

	items, err := s.fetchJustWatchStreamingCatalog(ctx, provider, catalogType, country, language, sortBy)
	if err != nil {
		return nil, err
	}
	s.streamingCatalogMu.Lock()
	s.streamingCatalogs[cacheKey] = streamingCatalogCacheEntry{
		items:   append([]stremioMeta(nil), items...),
		expires: now.Add(streamingCatalogCacheTTL),
	}
	s.streamingCatalogMu.Unlock()
	return items, nil
}

func (s *appState) fetchJustWatchStreamingCatalog(ctx context.Context, provider streamingCatalogProvider, catalogType string, country string, language string, sortBy string) ([]stremioMeta, error) {
	sortBy = normalizeStreamingCatalogSortBy(sortBy)
	if sortBy == "NEW" {
		return s.fetchJustWatchNewStreamingCatalog(ctx, provider, catalogType, country, language)
	}

	objectType := "MOVIE"
	if catalogType == "series" {
		objectType = "SHOW"
	}
	payload := map[string]any{
		"operationName": "GetPopularTitles",
		"variables": map[string]any{
			"popularTitlesSortBy": sortBy,
			"first":               streamingCatalogJustWatchLimit,
			"platform":            "WEB",
			"sortRandomSeed":      0,
			"popularAfterCursor":  "",
			"offset":              nil,
			"popularTitlesFilter": map[string]any{
				"ageCertifications":          []string{},
				"excludeGenres":              []string{},
				"excludeProductionCountries": []string{},
				"genres":                     []string{},
				"objectTypes":                []string{objectType},
				"productionCountries":        []string{},
				"packages":                   []string{provider.ID},
				"excludeIrrelevantTitles":    false,
				"presentationTypes":          []string{},
				"monetizationTypes":          []string{},
			},
			"language": language,
			"country":  country,
		},
		"query": streamingCatalogJustWatchQuery,
	}

	var response justWatchPopularTitlesResponse
	if err := s.postJSON(ctx, streamingCatalogJustWatchURL, payload, &response); err != nil {
		return nil, err
	}

	seen := map[string]bool{}
	items := make([]stremioMeta, 0, len(response.Data.PopularTitles.Edges))
	for _, edge := range response.Data.PopularTitles.Edges {
		content := edge.Node.Content
		imdbID := imdbFromID(content.ExternalIDs.IMDBID)
		title := strings.TrimSpace(content.Title)
		if imdbID == "" || title == "" || seen[imdbID] {
			continue
		}
		seen[imdbID] = true
		items = append(items, stremioMeta{
			ID:          imdbID,
			Type:        catalogType,
			Name:        title,
			Title:       title,
			Year:        content.ReleaseYear,
			ReleaseInfo: streamingCatalogReleaseInfo(content.ReleaseYear),
			IMDBID:      imdbID,
			Poster:      justWatchStreamingPosterURL(content.PosterURL, imdbID),
			PosterShape: "poster",
			IMDBRating:  justWatchScoreText(content.Scoring.IMDBScore),
		})
	}
	return items, nil
}

func (s *appState) fetchJustWatchNewStreamingCatalog(ctx context.Context, provider streamingCatalogProvider, catalogType string, country string, language string) ([]stremioMeta, error) {
	seen := map[string]bool{}
	items := make([]stremioMeta, 0, streamingCatalogJustWatchLimit)
	var firstErr error

	for day := 0; day < streamingCatalogNewLookbackDays && len(items) < streamingCatalogJustWatchLimit; day++ {
		date := time.Now().UTC().AddDate(0, 0, -day).Format("2006-01-02")
		dayItems, err := s.fetchJustWatchNewStreamingCatalogDate(ctx, provider, catalogType, country, language, date)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			log.Printf("streaming catalog new titles %s/%s %s failed: %v", catalogType, provider.ID, date, err)
			continue
		}
		for _, item := range dayItems {
			key := imdbFromID(firstNonEmpty(item.IMDBID, item.ID))
			if key == "" {
				key = strings.ToLower(strings.TrimSpace(item.Type + ":" + item.Name))
			}
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			items = append(items, item)
			if len(items) >= streamingCatalogJustWatchLimit {
				break
			}
		}
	}

	if len(items) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return items, nil
}

func (s *appState) fetchJustWatchNewStreamingCatalogDate(ctx context.Context, provider streamingCatalogProvider, catalogType string, country string, language string, date string) ([]stremioMeta, error) {
	objectType := "MOVIE"
	if catalogType == "series" {
		objectType = "SHOW_SEASON"
	}
	payload := map[string]any{
		"operationName": "GetNewTitles",
		"variables": map[string]any{
			"country":    country,
			"date":       date,
			"first":      streamingCatalogJustWatchLimit,
			"language":   language,
			"priceDrops": false,
			"pageType":   "NEW",
			"bucketType": "TODAY",
			"filter": map[string]any{
				"ageCertifications":          []string{},
				"excludeGenres":              []string{},
				"excludeProductionCountries": []string{},
				"genres":                     []string{},
				"objectTypes":                []string{objectType},
				"productionCountries":        []string{},
				"packages":                   []string{provider.ID},
				"excludeIrrelevantTitles":    false,
				"presentationTypes":          []string{},
				"monetizationTypes":          []string{},
			},
		},
		"query": streamingCatalogJustWatchNewTitlesQuery,
	}

	var response justWatchNewTitlesResponse
	if err := s.postJSON(ctx, streamingCatalogJustWatchURL, payload, &response); err != nil {
		return nil, err
	}

	items := make([]stremioMeta, 0, len(response.Data.NewTitles.Edges))
	for _, edge := range response.Data.NewTitles.Edges {
		content := edge.Node.Content
		if catalogType == "series" {
			content = edge.Node.Show.Content
		}
		item := streamingCatalogMetaFromNewContent(content, catalogType, date)
		if item.ID == "" {
			continue
		}
		items = append(items, item)
	}
	return items, nil
}

func streamingCatalogMetaFromNewContent(content justWatchNewTitleContent, catalogType string, date string) stremioMeta {
	imdbID := imdbFromID(content.ExternalIDs.IMDBID)
	title := strings.TrimSpace(content.Title)
	if imdbID == "" || title == "" {
		return stremioMeta{}
	}
	return stremioMeta{
		ID:          imdbID,
		Type:        catalogType,
		Name:        title,
		Title:       title,
		Year:        content.ReleaseYear,
		ReleaseInfo: streamingCatalogReleaseInfo(content.ReleaseYear),
		Released:    date,
		IMDBID:      imdbID,
		Poster:      justWatchStreamingPosterURL(content.PosterURL, imdbID),
		PosterShape: "poster",
		IMDBRating:  justWatchScoreText(content.Scoring.IMDBScore),
	}
}

func streamingCatalogReleaseInfo(year int) string {
	if year <= 0 {
		return ""
	}
	return strconv.Itoa(year)
}

func sortStreamingCatalogMetasByNewDate(items []stremioMeta) {
	sort.SliceStable(items, func(i, j int) bool {
		leftDate := strings.TrimSpace(items[i].Released)
		rightDate := strings.TrimSpace(items[j].Released)
		if leftDate != rightDate {
			return leftDate > rightDate
		}
		left := intFromAny(items[i].Year)
		right := intFromAny(items[j].Year)
		if left != right {
			return left > right
		}
		return strings.ToLower(items[i].Name) < strings.ToLower(items[j].Name)
	})
}

func justWatchStreamingPosterURL(raw string, imdbID string) string {
	if match := justWatchPosterPattern.FindStringSubmatch(raw); len(match) == 2 {
		return "https://images.justwatch.com/poster/" + match[1] + "/s332/img"
	}
	if imdbID != "" {
		return "https://live.metahub.space/poster/medium/" + imdbID + "/img"
	}
	return ""
}

func justWatchScoreText(value any) string {
	switch score := value.(type) {
	case float64:
		if score > 0 {
			return strconv.FormatFloat(score, 'f', 1, 64)
		}
	case string:
		return strings.TrimSpace(score)
	}
	return ""
}

func applyStreamingCatalogRPDB(rpdbKey string, items []stremioMeta) []stremioMeta {
	rpdbKey = strings.TrimSpace(rpdbKey)
	if rpdbKey == "" {
		return append([]stremioMeta{}, items...)
	}
	out := make([]stremioMeta, 0, len(items))
	for _, item := range items {
		clone := item
		if imdbID := imdbFromID(firstNonEmpty(clone.IMDBID, clone.ID)); imdbID != "" {
			clone.Poster = "https://api.ratingposterdb.com/" + url.PathEscape(rpdbKey) + "/imdb/poster-default/" + imdbID + ".jpg"
		}
		out = append(out, clone)
	}
	return out
}

func (s *appState) getLocalStreamingCatalogJSON(ctx context.Context, rawURL string, target any) (bool, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil || !strings.HasPrefix(parsed.Path, streamingCatalogAddonPath+"/") {
		return false, nil
	}
	value, err := s.localStreamingCatalogValue(ctx, parsed.Path)
	if err != nil {
		return true, err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return true, err
	}
	return true, json.Unmarshal(data, target)
}

func (s *appState) localStreamingCatalogValue(ctx context.Context, path string) (any, error) {
	tail := strings.Trim(strings.TrimPrefix(path, streamingCatalogAddonPath), "/")
	if tail == "" || tail == "manifest.json" {
		return streamingCatalogManifest(defaultStreamingCatalogConfig()), nil
	}
	parts := strings.Split(tail, "/")
	if len(parts) == 2 && parts[1] == "manifest.json" {
		config, err := decodeStreamingCatalogConfig(parts[0])
		if err != nil {
			return nil, err
		}
		return streamingCatalogManifest(config), nil
	}
	if len(parts) >= 4 && parts[1] == "catalog" {
		config, err := decodeStreamingCatalogConfig(parts[0])
		if err != nil {
			return nil, err
		}
		catalogType := normalizeStreamingCatalogCatalogType(parts[2])
		providerID := strings.TrimSuffix(parts[3], ".json")
		if catalogType == "" || providerID == "" {
			return nil, fmt.Errorf("invalid streaming catalog path")
		}
		if !streamingCatalogConfigIncludes(config, catalogType, providerID) {
			return map[string]any{"metas": []stremioMeta{}}, nil
		}
		items, err := s.streamingCatalogCatalogMetas(ctx, config, catalogType, providerID)
		if err != nil {
			return nil, err
		}
		return map[string]any{"metas": applyStreamingCatalogRPDB(config.RPDBKey, items)}, nil
	}
	return nil, fmt.Errorf("streaming catalogs route not found")
}

type tmdbKeywordSearchResponse struct {
	Results []struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	} `json:"results"`
}

type tmdbDiscoverResponse struct {
	Results    []tmdbDiscoverResult `json:"results"`
	TotalPages int                  `json:"total_pages"`
}

type tmdbDiscoverResult struct {
	ID               int     `json:"id"`
	Title            string  `json:"title"`
	Name             string  `json:"name"`
	OriginalTitle    string  `json:"original_title"`
	OriginalName     string  `json:"original_name"`
	Overview         string  `json:"overview"`
	PosterPath       string  `json:"poster_path"`
	BackdropPath     string  `json:"backdrop_path"`
	ReleaseDate      string  `json:"release_date"`
	FirstAirDate     string  `json:"first_air_date"`
	OriginalLanguage string  `json:"original_language"`
	GenreIDs         []int   `json:"genre_ids"`
	VoteAverage      float64 `json:"vote_average"`
}

type tmdbGenre struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type tmdbExternalIDsResponse struct {
	IMDBID string `json:"imdb_id"`
}

type tmdbMovieDetailsResponse struct {
	ID               int         `json:"id"`
	Title            string      `json:"title"`
	OriginalTitle    string      `json:"original_title"`
	Overview         string      `json:"overview"`
	PosterPath       string      `json:"poster_path"`
	BackdropPath     string      `json:"backdrop_path"`
	ReleaseDate      string      `json:"release_date"`
	Runtime          int         `json:"runtime"`
	Genres           []tmdbGenre `json:"genres"`
	VoteAverage      float64     `json:"vote_average"`
	OriginalLanguage string      `json:"original_language"`
	Status           string      `json:"status"`
	Tagline          string      `json:"tagline"`
}

type tmdbTVDetailsResponse struct {
	ID               int                  `json:"id"`
	Name             string               `json:"name"`
	OriginalName     string               `json:"original_name"`
	Overview         string               `json:"overview"`
	PosterPath       string               `json:"poster_path"`
	BackdropPath     string               `json:"backdrop_path"`
	FirstAirDate     string               `json:"first_air_date"`
	EpisodeRunTime   []int                `json:"episode_run_time"`
	NumberOfEpisodes int                  `json:"number_of_episodes"`
	NumberOfSeasons  int                  `json:"number_of_seasons"`
	Genres           []tmdbGenre          `json:"genres"`
	VoteAverage      float64              `json:"vote_average"`
	OriginalLanguage string               `json:"original_language"`
	OriginCountry    []string             `json:"origin_country"`
	Status           string               `json:"status"`
	Tagline          string               `json:"tagline"`
	Seasons          []tmdbTVSeasonResult `json:"seasons"`
}

type tmdbTVSeasonResult struct {
	ID           int    `json:"id"`
	Name         string `json:"name"`
	Overview     string `json:"overview"`
	PosterPath   string `json:"poster_path"`
	AirDate      string `json:"air_date"`
	SeasonNumber int    `json:"season_number"`
	EpisodeCount int    `json:"episode_count"`
}

type tmdbTVSeasonDetailsResponse struct {
	ID       int                 `json:"id"`
	Name     string              `json:"name"`
	Overview string              `json:"overview"`
	Season   int                 `json:"season_number"`
	Episodes []tmdbTVEpisodeItem `json:"episodes"`
}

type tmdbTVEpisodeItem struct {
	ID            int     `json:"id"`
	Name          string  `json:"name"`
	Overview      string  `json:"overview"`
	StillPath     string  `json:"still_path"`
	AirDate       string  `json:"air_date"`
	SeasonNumber  int     `json:"season_number"`
	EpisodeNumber int     `json:"episode_number"`
	Runtime       int     `json:"runtime"`
	VoteAverage   float64 `json:"vote_average"`
}

func normalizeTMDBKeywordRowsConfig(config tmdbKeywordRowsConfig) tmdbKeywordRowsConfig {
	config.TMDBAPIKey = strings.TrimSpace(config.TMDBAPIKey)
	config.TMDBAccessToken = strings.TrimSpace(config.TMDBAccessToken)
	config.Language = firstNonEmpty(strings.TrimSpace(config.Language), "en-US")
	config.Region = strings.ToUpper(firstNonEmpty(strings.TrimSpace(config.Region), "US"))
	if config.RowCount <= 0 {
		config.RowCount = tmdbKeywordRowsDefaultCount
	}
	if config.RowCount > tmdbKeywordRowsMaxCount {
		config.RowCount = tmdbKeywordRowsMaxCount
	}
	if !tmdbKeywordRowsHasCredentials(config) {
		config.Enabled = false
	}
	return config
}

func tmdbKeywordRowsHasCredentials(config tmdbKeywordRowsConfig) bool {
	return strings.TrimSpace(config.TMDBAPIKey) != "" || strings.TrimSpace(config.TMDBAccessToken) != ""
}

func tmdbKeywordRowsDashboardSummary(config tmdbKeywordRowsConfig, now time.Time) map[string]any {
	config = normalizeTMDBKeywordRowsConfig(config)
	bucket, next := tmdbKeywordRowsRotation(now)
	return map[string]any{
		"enabled":           config.Enabled,
		"row_count":         config.RowCount,
		"has_api_key":       strings.TrimSpace(config.TMDBAPIKey) != "",
		"has_access_token":  strings.TrimSpace(config.TMDBAccessToken) != "",
		"language":          config.Language,
		"region":            config.Region,
		"current_bucket":    bucket,
		"next_rotation_at":  next,
		"current_phrases":   tmdbKeywordSelectedPhrases(bucket, config.RowCount),
		"max_row_count":     tmdbKeywordRowsMaxCount,
		"default_row_count": tmdbKeywordRowsDefaultCount,
	}
}

func (s *appState) tmdbKeywordHomeRows(ctx context.Context, itemLimit int, rowLimit int, now time.Time) ([]vortexoHomeRow, time.Time) {
	s.mu.RLock()
	config := s.config.TMDBKeywordRows
	s.mu.RUnlock()
	config = normalizeTMDBKeywordRowsConfig(config)
	bucket, nextRotation := tmdbKeywordRowsRotation(now)
	if !config.Enabled || !tmdbKeywordRowsHasCredentials(config) || rowLimit <= 0 {
		return []vortexoHomeRow{}, time.Time{}
	}

	count := minInt(config.RowCount, rowLimit)
	phrases := tmdbKeywordSelectedPhrases(bucket, count)
	rows := make([]vortexoHomeRow, 0, len(phrases))
	for _, phrase := range phrases {
		items, err := s.tmdbKeywordRowItems(ctx, config, phrase, itemLimit, bucket, nextRotation)
		if err != nil {
			log.Printf("TMDB keyword row %q failed: %v", phrase, err)
			continue
		}
		if len(items) == 0 {
			continue
		}
		rows = append(rows, vortexoHomeRow{
			ID:           "tmdb-keyword-" + slug(phrase) + "-" + strconv.FormatInt(bucket.Unix()/3600, 10),
			Title:        tmdbKeywordDisplayName(phrase),
			Reason:       "Hourly TMDB keyword row",
			RefreshAfter: nextRotation,
			Items:        items,
		})
	}
	return rows, nextRotation
}

func (s *appState) tmdbKeywordRowItems(ctx context.Context, config tmdbKeywordRowsConfig, phrase string, itemLimit int, bucket time.Time, expires time.Time) ([]vortexoHomeItem, error) {
	itemLimit = boundedInt(strconv.Itoa(itemLimit), 30, 6, tmdbKeywordRowMaxItems)
	cacheKey := strings.Join([]string{
		strconv.FormatInt(bucket.Unix()/3600, 10),
		config.Language,
		config.Region,
		strconv.Itoa(itemLimit),
		strings.ToLower(strings.TrimSpace(phrase)),
	}, ":")

	s.tmdbKeywordMu.RLock()
	cached, ok := s.tmdbKeywordRows[cacheKey]
	s.tmdbKeywordMu.RUnlock()
	if ok && time.Now().UTC().Before(cached.expires) {
		return append([]vortexoHomeItem(nil), cached.items...), nil
	}

	keywordID, err := s.resolveTMDBKeywordID(ctx, config, phrase)
	if err != nil {
		return nil, err
	}
	movies, movieErr := s.fetchTMDBKeywordDiscoverItems(ctx, config, "movie", keywordID, itemLimit)
	shows, showErr := s.fetchTMDBKeywordDiscoverItems(ctx, config, "tv", keywordID, itemLimit)
	if movieErr != nil {
		log.Printf("TMDB keyword movie discover %q failed: %v", phrase, movieErr)
	}
	if showErr != nil {
		log.Printf("TMDB keyword TV discover %q failed: %v", phrase, showErr)
	}
	if len(movies) == 0 && len(shows) == 0 {
		if movieErr != nil {
			return nil, movieErr
		}
		if showErr != nil {
			return nil, showErr
		}
	}

	combined := append(movies, shows...)
	seen := map[string]bool{}
	items := make([]vortexoHomeItem, 0, minInt(itemLimit, len(combined)))
	sort.SliceStable(combined, func(i, j int) bool {
		leftKey := homeDedupeKey(combined[i])
		rightKey := homeDedupeKey(combined[j])
		leftHash := stableHash64(strconv.FormatInt(bucket.Unix()/3600, 10) + ":" + phrase + ":" + leftKey)
		rightHash := stableHash64(strconv.FormatInt(bucket.Unix()/3600, 10) + ":" + phrase + ":" + rightKey)
		if leftHash != rightHash {
			return leftHash < rightHash
		}
		return strings.ToLower(combined[i].Title) < strings.ToLower(combined[j].Title)
	})
	for _, item := range combined {
		key := homeDedupeKey(item)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		items = append(items, item)
		if len(items) >= itemLimit {
			break
		}
	}

	s.tmdbKeywordMu.Lock()
	s.tmdbKeywordRows[cacheKey] = tmdbKeywordRowsCacheEntry{
		items:   append([]vortexoHomeItem(nil), items...),
		expires: expires,
	}
	s.tmdbKeywordMu.Unlock()
	return items, nil
}

func (s *appState) resolveTMDBKeywordID(ctx context.Context, config tmdbKeywordRowsConfig, phrase string) (int, error) {
	phrase = strings.TrimSpace(phrase)
	if phrase == "" {
		return 0, fmt.Errorf("empty TMDB keyword phrase")
	}
	cacheKey := strings.ToLower(phrase)
	now := time.Now().UTC()
	s.tmdbKeywordMu.RLock()
	cached, ok := s.tmdbKeywordIDs[cacheKey]
	s.tmdbKeywordMu.RUnlock()
	if ok && now.Before(cached.expires) {
		return cached.id, nil
	}

	var response tmdbKeywordSearchResponse
	if err := s.tmdbGetJSON(ctx, config, "/3/search/keyword", []plexQueryItem{
		{Name: "query", Value: phrase},
	}, &response); err != nil {
		return 0, err
	}

	var keywordID int
	normalizedPhrase := strings.ToLower(phrase)
	for _, result := range response.Results {
		if strings.ToLower(strings.TrimSpace(result.Name)) == normalizedPhrase {
			keywordID = result.ID
			break
		}
	}
	if keywordID == 0 && len(response.Results) > 0 {
		keywordID = response.Results[0].ID
	}
	if keywordID <= 0 {
		return 0, fmt.Errorf("TMDB keyword %q not found", phrase)
	}

	s.tmdbKeywordMu.Lock()
	s.tmdbKeywordIDs[cacheKey] = tmdbKeywordIDCacheEntry{
		id:      keywordID,
		expires: now.Add(24 * time.Hour),
	}
	s.tmdbKeywordMu.Unlock()
	return keywordID, nil
}

func (s *appState) fetchTMDBKeywordDiscoverItems(ctx context.Context, config tmdbKeywordRowsConfig, endpointType string, keywordID int, limit int) ([]vortexoHomeItem, error) {
	limit = boundedInt(strconv.Itoa(limit), 30, 6, tmdbKeywordRowMaxItems)
	endpointType = strings.ToLower(strings.TrimSpace(endpointType))
	if endpointType != "movie" && endpointType != "tv" {
		return nil, fmt.Errorf("unsupported TMDB discover endpoint %q", endpointType)
	}

	items := make([]vortexoHomeItem, 0, limit)
	totalPages := 1
	for page := 1; len(items) < limit && page <= totalPages && page <= 3; page++ {
		var response tmdbDiscoverResponse
		err := s.tmdbGetJSON(ctx, config, "/3/discover/"+endpointType, []plexQueryItem{
			{Name: "include_adult", Value: "false"},
			{Name: "sort_by", Value: "popularity.desc"},
			{Name: "with_keywords", Value: strconv.Itoa(keywordID)},
			{Name: "page", Value: strconv.Itoa(page)},
		}, &response)
		if err != nil {
			return items, err
		}
		totalPages = maxInt(response.TotalPages, 1)
		for _, result := range response.Results {
			item := tmdbKeywordHomeItem(result, endpointType)
			if item.TMDBID <= 0 || strings.TrimSpace(item.Title) == "" {
				continue
			}
			items = append(items, item)
			if len(items) >= limit {
				break
			}
		}
	}
	return items, nil
}

func (s *appState) tmdbGetJSON(ctx context.Context, config tmdbKeywordRowsConfig, path string, queryItems []plexQueryItem, target any) error {
	config = normalizeTMDBKeywordRowsConfig(config)
	if !tmdbKeywordRowsHasCredentials(config) {
		return fmt.Errorf("TMDB API key or read access token is required")
	}
	components, err := url.Parse(tmdbAPIBaseURL)
	if err != nil {
		return err
	}
	components.Path = path
	query := components.Query()
	if strings.TrimSpace(config.TMDBAccessToken) == "" {
		query.Set("api_key", config.TMDBAPIKey)
	}
	if strings.TrimSpace(config.Language) != "" {
		query.Set("language", config.Language)
	}
	if strings.TrimSpace(config.Region) != "" {
		query.Set("region", config.Region)
	}
	for _, item := range queryItems {
		if strings.TrimSpace(item.Name) != "" {
			query.Set(item.Name, item.Value)
		}
	}
	components.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, components.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "VortexoManifestServer/1.0")
	if strings.TrimSpace(config.TMDBAccessToken) != "" {
		req.Header.Set("Authorization", "Bearer "+config.TMDBAccessToken)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("TMDB %s failed: HTTP %d %s", path, resp.StatusCode, responseMessage(data))
	}
	return json.Unmarshal(data, target)
}

func (s *appState) tmdbKeywordConfig() tmdbKeywordRowsConfig {
	s.mu.RLock()
	config := s.config.TMDBKeywordRows
	s.mu.RUnlock()
	return normalizeTMDBKeywordRowsConfig(config)
}

func (s *appState) tmdbMovieDetailsFromAPI(ctx context.Context, tmdbID int) (map[string]any, error) {
	if tmdbID <= 0 {
		return nil, fmt.Errorf("missing tmdb id")
	}
	var details tmdbMovieDetailsResponse
	if err := s.tmdbGetJSON(ctx, s.tmdbKeywordConfig(), fmt.Sprintf("/3/movie/%d", tmdbID), nil, &details); err != nil {
		return nil, err
	}
	return tmdbMovieDetailsMap(details), nil
}

func (s *appState) tmdbTVDetailsFromAPI(ctx context.Context, tmdbID int) (map[string]any, error) {
	details, err := s.tmdbTVRawDetails(ctx, tmdbID)
	if err != nil {
		return nil, err
	}
	return tmdbTVDetailsMap(details), nil
}

func (s *appState) tmdbTVSeasonsFromAPI(ctx context.Context, tmdbID int) ([]map[string]any, error) {
	details, err := s.tmdbTVRawDetails(ctx, tmdbID)
	if err != nil {
		return nil, err
	}
	seasons := details.Seasons
	if len(seasons) == 0 && details.NumberOfSeasons > 0 {
		seasons = make([]tmdbTVSeasonResult, 0, details.NumberOfSeasons)
		for seasonNumber := 1; seasonNumber <= details.NumberOfSeasons; seasonNumber++ {
			seasons = append(seasons, tmdbTVSeasonResult{SeasonNumber: seasonNumber})
		}
	}
	out := make([]map[string]any, 0, len(seasons))
	for _, season := range seasons {
		if season.SeasonNumber < 0 {
			continue
		}
		out = append(out, tmdbTVSeasonMap(season, details.Overview))
	}
	sort.SliceStable(out, func(i, j int) bool {
		return intFromAny(out[i]["season_number"]) < intFromAny(out[j]["season_number"])
	})
	return out, nil
}

func (s *appState) tmdbTVEpisodesFromAPI(ctx context.Context, tmdbID int) ([]map[string]any, error) {
	details, err := s.tmdbTVRawDetails(ctx, tmdbID)
	if err != nil {
		return nil, err
	}
	seasons := details.Seasons
	if len(seasons) == 0 && details.NumberOfSeasons > 0 {
		seasons = make([]tmdbTVSeasonResult, 0, details.NumberOfSeasons)
		for seasonNumber := 1; seasonNumber <= details.NumberOfSeasons; seasonNumber++ {
			seasons = append(seasons, tmdbTVSeasonResult{SeasonNumber: seasonNumber})
		}
	}

	config := s.tmdbKeywordConfig()
	episodes := make([]map[string]any, 0, details.NumberOfEpisodes)
	var lastErr error
	for _, season := range seasons {
		if season.SeasonNumber < 0 {
			continue
		}
		var seasonDetails tmdbTVSeasonDetailsResponse
		path := fmt.Sprintf("/3/tv/%d/season/%d", tmdbID, season.SeasonNumber)
		if err := s.tmdbGetJSON(ctx, config, path, nil, &seasonDetails); err != nil {
			lastErr = err
			log.Printf("TMDB episodes failed for tv/%d season %d: %v", tmdbID, season.SeasonNumber, err)
			continue
		}
		for _, episode := range seasonDetails.Episodes {
			if episode.SeasonNumber == 0 {
				episode.SeasonNumber = season.SeasonNumber
			}
			episodes = append(episodes, tmdbTVEpisodeMap(episode))
		}
	}
	if len(episodes) == 0 && lastErr != nil {
		return nil, lastErr
	}
	sort.SliceStable(episodes, func(i, j int) bool {
		leftSeason := intFromAny(episodes[i]["season_number"])
		rightSeason := intFromAny(episodes[j]["season_number"])
		if leftSeason != rightSeason {
			return leftSeason < rightSeason
		}
		return intFromAny(episodes[i]["episode_number"]) < intFromAny(episodes[j]["episode_number"])
	})
	return episodes, nil
}

func (s *appState) tmdbTVRawDetails(ctx context.Context, tmdbID int) (tmdbTVDetailsResponse, error) {
	if tmdbID <= 0 {
		return tmdbTVDetailsResponse{}, fmt.Errorf("missing tmdb id")
	}
	var details tmdbTVDetailsResponse
	if err := s.tmdbGetJSON(ctx, s.tmdbKeywordConfig(), fmt.Sprintf("/3/tv/%d", tmdbID), nil, &details); err != nil {
		return tmdbTVDetailsResponse{}, err
	}
	return details, nil
}

func (s *appState) tmdbIMDbID(ctx context.Context, vortexoType string, tmdbID int) (string, error) {
	if tmdbID <= 0 {
		return "", fmt.Errorf("missing tmdb id")
	}
	endpointType := "movie"
	if normalizeVortexoType(vortexoType) == "episode" {
		endpointType = "tv"
	}
	var response tmdbExternalIDsResponse
	if err := s.tmdbGetJSON(ctx, s.tmdbKeywordConfig(), fmt.Sprintf("/3/%s/%d/external_ids", endpointType, tmdbID), nil, &response); err != nil {
		return "", err
	}
	imdbID := imdbFromID(response.IMDBID)
	if imdbID == "" {
		return "", fmt.Errorf("TMDB %s/%d has no imdb id", endpointType, tmdbID)
	}
	return imdbID, nil
}

func tmdbMovieDetailsMap(details tmdbMovieDetailsResponse) map[string]any {
	return map[string]any{
		"id":                details.ID,
		"title":             details.Title,
		"overview":          details.Overview,
		"poster_path":       tmdbImageURL(details.PosterPath, "w500"),
		"backdrop_path":     tmdbImageURL(details.BackdropPath, "w1280"),
		"poster_paths":      stringList(tmdbImageURL(details.PosterPath, "w500")),
		"backdrop_paths":    stringList(tmdbImageURL(details.BackdropPath, "w1280")),
		"landscape_paths":   stringList(tmdbImageURL(details.BackdropPath, "w780")),
		"genres":            tmdbGenreNames(details.Genres),
		"vote_average":      details.VoteAverage,
		"runtime":           details.Runtime,
		"release_date":      details.ReleaseDate,
		"original_language": details.OriginalLanguage,
		"status":            details.Status,
		"tagline":           details.Tagline,
	}
}

func tmdbTVDetailsMap(details tmdbTVDetailsResponse) map[string]any {
	return map[string]any{
		"id":                 details.ID,
		"name":               details.Name,
		"overview":           details.Overview,
		"poster_path":        tmdbImageURL(details.PosterPath, "w500"),
		"backdrop_path":      tmdbImageURL(details.BackdropPath, "w1280"),
		"poster_paths":       stringList(tmdbImageURL(details.PosterPath, "w500")),
		"backdrop_paths":     stringList(tmdbImageURL(details.BackdropPath, "w1280")),
		"landscape_paths":    stringList(tmdbImageURL(details.BackdropPath, "w780")),
		"genres":             tmdbGenreNames(details.Genres),
		"vote_average":       details.VoteAverage,
		"runtime":            firstInt(details.EpisodeRunTime),
		"first_air_date":     details.FirstAirDate,
		"number_of_seasons":  details.NumberOfSeasons,
		"number_of_episodes": details.NumberOfEpisodes,
		"original_language":  details.OriginalLanguage,
		"origin_country":     details.OriginCountry,
		"status":             details.Status,
		"tagline":            details.Tagline,
	}
}

func tmdbTVSeasonMap(season tmdbTVSeasonResult, fallbackOverview string) map[string]any {
	seasonNumber := season.SeasonNumber
	name := firstNonEmpty(season.Name, fmt.Sprintf("Season %d", seasonNumber))
	if seasonNumber == 0 {
		name = firstNonEmpty(season.Name, "Specials")
	}
	return map[string]any{
		"id":            firstNonZero(season.ID, seasonNumber),
		"season_number": seasonNumber,
		"name":          name,
		"overview":      firstNonEmpty(season.Overview, fallbackOverview),
		"poster_path":   tmdbImageURL(season.PosterPath, "w500"),
		"air_date":      season.AirDate,
		"episode_count": season.EpisodeCount,
	}
}

func tmdbTVEpisodeMap(episode tmdbTVEpisodeItem) map[string]any {
	return map[string]any{
		"id":             strconv.Itoa(episode.ID),
		"tmdb_id":        episode.ID,
		"title":          firstNonEmpty(episode.Name, fmt.Sprintf("Episode %d", episode.EpisodeNumber)),
		"overview":       episode.Overview,
		"still_path":     tmdbImageURL(episode.StillPath, "w780"),
		"season_number":  episode.SeasonNumber,
		"episode_number": episode.EpisodeNumber,
		"runtime":        episode.Runtime,
		"air_date":       episode.AirDate,
		"vote_average":   episode.VoteAverage,
	}
}

func tmdbGenreNames(genres []tmdbGenre) []string {
	values := make([]string, 0, len(genres))
	for _, genre := range genres {
		values = append(values, genre.Name)
	}
	return uniqueNonEmptyStrings(values)
}

func firstInt(values []int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func tmdbKeywordHomeItem(result tmdbDiscoverResult, endpointType string) vortexoHomeItem {
	mediaType := "movie"
	date := result.ReleaseDate
	if endpointType == "tv" {
		mediaType = "tv"
		date = result.FirstAirDate
	}
	tmdbID := result.ID
	title := firstNonEmpty(result.Title, result.Name)
	id := "tmdb:" + strconv.Itoa(tmdbID)
	guid := "tmdb://" + mediaType + "/" + strconv.Itoa(tmdbID)
	ratingType := mediaType
	if ratingType == "tv" {
		ratingType = "show"
	}
	return vortexoHomeItem{
		ID:               id,
		RatingKey:        "vortexo:tmdb:" + ratingType + ":" + strconv.Itoa(tmdbID),
		Key:              guid,
		GUID:             guid,
		MediaType:        mediaType,
		TMDBID:           tmdbID,
		Title:            title,
		OriginalTitle:    firstNonEmpty(result.OriginalTitle, result.OriginalName),
		Overview:         result.Overview,
		PosterPath:       tmdbImageURL(result.PosterPath, "w500"),
		BackdropPath:     tmdbImageURL(result.BackdropPath, "w780"),
		LandscapePath:    tmdbImageURL(result.BackdropPath, "w780"),
		OriginalLanguage: result.OriginalLanguage,
		Year:             yearFromText(date),
		VoteAverage:      result.VoteAverage,
		ReleaseDate:      date,
		FirstAirDate:     result.FirstAirDate,
		AddedAt:          time.Now().Unix(),
		UpdatedAt:        time.Now().Unix(),
	}
}

func tmdbImageURL(path string, size string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	size = firstNonEmpty(strings.TrimSpace(size), "w500")
	return "https://image.tmdb.org/t/p/" + size + "/" + strings.TrimLeft(path, "/")
}

func tmdbKeywordRowsRotation(now time.Time) (time.Time, time.Time) {
	bucket := now.UTC().Truncate(time.Hour)
	return bucket, bucket.Add(time.Hour)
}

func tmdbKeywordSelectedPhrases(bucket time.Time, count int) []string {
	count = boundedInt(strconv.Itoa(count), tmdbKeywordRowsDefaultCount, 1, tmdbKeywordRowsMaxCount)
	phrases := append([]string(nil), tmdbKeywordPhrasePool...)
	seed := strconv.FormatInt(bucket.UTC().Unix()/3600, 10)
	sort.SliceStable(phrases, func(i, j int) bool {
		leftHash := stableHash64(seed + ":" + phrases[i])
		rightHash := stableHash64(seed + ":" + phrases[j])
		if leftHash != rightHash {
			return leftHash < rightHash
		}
		return phrases[i] < phrases[j]
	})
	if len(phrases) > count {
		phrases = phrases[:count]
	}
	return phrases
}

func tmdbKeywordDisplayName(phrase string) string {
	parts := strings.Fields(strings.TrimSpace(phrase))
	for i, part := range parts {
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + part[1:]
	}
	return strings.Join(parts, " ")
}

func stableHash64(value string) uint64 {
	var hash uint64 = 1469598103934665603
	for _, b := range []byte(value) {
		hash ^= uint64(b)
		hash *= 1099511628211
	}
	return hash
}

func (s *appState) clearTMDBKeywordRowsCache() {
	s.tmdbKeywordMu.Lock()
	s.tmdbKeywordRows = map[string]tmdbKeywordRowsCacheEntry{}
	s.tmdbKeywordIDs = map[string]tmdbKeywordIDCacheEntry{}
	s.tmdbKeywordMu.Unlock()
}

var tmdbKeywordPhrasePool = []string{
	"mind bending",
	"crime thriller",
	"spy drama",
	"space adventure",
	"survival",
	"post apocalypse",
	"serial killer",
	"time travel",
	"courtroom drama",
	"heist",
	"revenge",
	"martial arts",
	"coming of age",
	"cyberpunk",
	"supernatural mystery",
	"political thriller",
	"dark comedy",
	"gangster",
	"war epic",
	"small town secrets",
	"vigilante",
	"road trip",
	"alien invasion",
	"medical drama",
	"prison break",
	"fantasy quest",
	"period romance",
	"sports underdog",
	"disaster",
	"psychological drama",
	"family saga",
	"monster",
	"tech thriller",
	"conspiracy",
	"noir",
	"detective",
	"holiday chaos",
	"anime action",
	"school drama",
	"zombie outbreak",
	"ocean adventure",
	"western",
	"musical",
	"dystopia",
	"assassin",
	"biopic",
	"treasure hunt",
	"parallel universe",
	"true crime",
	"cozy mystery",
}

const streamingCatalogJustWatchQuery = `query GetPopularTitles(
  $country: Country!
  $popularTitlesFilter: TitleFilter
  $popularAfterCursor: String
  $popularTitlesSortBy: PopularTitlesSorting! = POPULAR
  $first: Int!
  $language: Language!
  $offset: Int = 0
  $sortRandomSeed: Int! = 0
  $profile: PosterProfile
  $format: ImageFormat
) {
  popularTitles(
    country: $country
    filter: $popularTitlesFilter
    offset: $offset
    after: $popularAfterCursor
    sortBy: $popularTitlesSortBy
    first: $first
    sortRandomSeed: $sortRandomSeed
  ) {
    edges {
      node {
        content(country: $country, language: $language) {
          externalIds {
            imdbId
          }
          title
          originalReleaseYear
          scoring {
            imdbScore
          }
          posterUrl(profile: $profile, format: $format)
        }
      }
    }
  }
}`

const streamingCatalogJustWatchNewTitlesQuery = `query GetNewTitles(
  $country: Country!
  $date: Date!
  $filter: TitleFilter
  $first: Int!
  $language: Language!
  $priceDrops: Boolean!
  $pageType: NewPageType!
  $bucketType: NewDateRangeBucket
) {
  newTitles(
    country: $country
    date: $date
    filter: $filter
    first: $first
    priceDrops: $priceDrops
    pageType: $pageType
    bucketType: $bucketType
  ) {
    edges {
      node {
        __typename
        ... on Movie {
          objectType
          content(country: $country, language: $language) {
            title
            originalReleaseYear
            externalIds {
              imdbId
            }
            scoring {
              imdbScore
            }
            posterUrl
          }
        }
        ... on Season {
          objectType
          content(country: $country, language: $language) {
            title
            seasonNumber
            originalReleaseYear
          }
          show {
            objectType
            content(country: $country, language: $language) {
              title
              originalReleaseYear
              externalIds {
                imdbId
              }
              scoring {
                imdbScore
              }
              posterUrl
            }
          }
        }
      }
    }
  }
}`

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
	case http.MethodPut, http.MethodPatch:
		var req struct {
			Name    *string `json:"name"`
			Enabled *bool   `json:"enabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		s.mu.Lock()
		found := false
		var updated installedManifest
		for i := range s.config.Manifests {
			if s.config.Manifests[i].ID != id {
				continue
			}
			found = true
			if req.Name != nil {
				if name := strings.TrimSpace(*req.Name); name != "" {
					s.config.Manifests[i].Name = name
				}
			}
			if req.Enabled != nil {
				s.config.Manifests[i].Enabled = *req.Enabled
			}
			s.config.Manifests[i].UpdatedAt = time.Now().UTC()
			updated = s.config.Manifests[i]
			break
		}
		var err error
		if found {
			err = s.saveLocked()
		}
		s.mu.Unlock()
		if !found {
			respondError(w, http.StatusNotFound, "manifest not found")
			return
		}
		if err != nil {
			respondError(w, http.StatusInternalServerError, "failed to save manifest")
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"manifest": updated})
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
		s.config.Catalogs = removeManifestCatalogPreferences(s.config.Catalogs, id)
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

func (s *appState) handlePlexArtworkRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	limit := plexArtworkSyncLimit
	var req struct {
		Limit int  `json:"limit"`
		Force bool `json:"force"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Limit > 0 {
		limit = minInt(req.Limit, 2000)
	}
	forceRepair := req.Force

	if !s.plexArtworkSyncMu.TryLock() {
		respondError(w, http.StatusConflict, "plex artwork sync is already running")
		return
	}
	s.setPlexArtworkSyncRunning(true)

	go func() {
		defer func() {
			s.setPlexArtworkSyncRunning(false)
			s.plexArtworkSyncMu.Unlock()
		}()
		stats, err := s.syncPlexArtworkCache(context.Background(), limit, forceRepair)
		s.rememberPlexArtworkSyncStats(stats)
		if err != nil {
			log.Printf("[PlexArtwork] manual sync error: %v", err)
			return
		}
		log.Printf("[PlexArtwork] manual sync complete forceRepair=%t ok=%d miss=%d failed=%d stopped=%q", forceRepair, stats.OK, stats.Miss, stats.Failed, stats.Stopped)
	}()

	respondJSON(w, http.StatusAccepted, map[string]any{
		"message": "Plex artwork sync triggered",
		"limit":   limit,
		"force":   forceRepair,
	})
}

func (s *appState) handlePlexArtworkStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{"artwork": s.plexArtworkDashboardStats()})
}

func (s *appState) handlePlexArtworkByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	raw := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/artwork/"), "/")
	parts := strings.Split(raw, "/")
	if len(parts) != 2 && len(parts) != 3 {
		respondError(w, http.StatusNotFound, "plex artwork not found")
		return
	}
	mediaType := normalizePlexArtworkMediaType(parts[0])
	var tmdbID int
	var imdbID string
	if len(parts) == 3 {
		if !strings.EqualFold(parts[1], "imdb") {
			respondError(w, http.StatusNotFound, "plex artwork not found")
			return
		}
		imdbID = imdbFromID(strings.TrimSuffix(parts[2], ".json"))
	} else {
		rawID := strings.TrimSuffix(parts[1], ".json")
		if strings.HasPrefix(strings.ToLower(rawID), "tt") {
			imdbID = imdbFromID(rawID)
		} else {
			var err error
			tmdbID, err = strconv.Atoi(rawID)
			if err != nil {
				respondError(w, http.StatusBadRequest, "invalid artwork id")
				return
			}
		}
	}
	if (tmdbID <= 0 && imdbID == "") || mediaType == "" {
		respondError(w, http.StatusBadRequest, "invalid artwork id")
		return
	}

	if r.URL.Query().Get("refresh") == "true" {
		var item plexArtworkSeedItem
		if tmdbID > 0 {
			item = s.findPlexArtworkSeedItem(r.Context(), mediaType, tmdbID)
		}
		if item.TMDBID <= 0 && item.IMDBID == "" {
			item = plexArtworkSeedItem{MediaType: mediaType, TMDBID: tmdbID, IMDBID: imdbID}
		}
		record, err := s.refreshPlexArtworkSeed(r.Context(), item)
		if err != nil {
			respondError(w, http.StatusBadGateway, err.Error())
			return
		}
		if record == nil || record.Status != "ok" || record.Artwork.isEmpty() {
			respondError(w, http.StatusNotFound, "plex artwork unavailable")
			return
		}
		respondJSON(w, http.StatusOK, record)
		return
	}

	record, ok := s.getCachedPlexArtwork(mediaType, tmdbID, imdbID, "", 0)
	if !ok || record.Status != "ok" || record.Artwork.isEmpty() {
		respondError(w, http.StatusNotFound, "plex artwork not cached")
		return
	}
	respondJSON(w, http.StatusOK, record)
}

func (s *appState) handleEmptyList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{"items": []any{}, "channels": []any{}})
}

func (s *appState) handleChannels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if strings.Contains(r.URL.Path, "/epg/guide") {
		respondJSON(w, http.StatusOK, map[string]any{"channels": []any{}, "items": []any{}})
		return
	}
	limit := boundedInt(r.URL.Query().Get("limit"), 500, 1, 1000)
	channels := s.collectLiveChannels(r.Context(), limit)
	respondJSON(w, http.StatusOK, map[string]any{
		"channels": channels,
		"items":    channels,
		"results":  channels,
	})
}

func (s *appState) handleVortexoLiveTVRows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	rowLimit := boundedInt(r.URL.Query().Get("row_limit"), 8, 1, 12)
	itemLimit := boundedInt(r.URL.Query().Get("item_limit"), 30, 6, 50)
	channels := s.collectLiveChannels(r.Context(), rowLimit*itemLimit)
	rows := liveRowsFromChannels(
		channels,
		commaSet(r.URL.Query().Get("favorite_ids")),
		commaSet(r.URL.Query().Get("recent_ids")),
		rowLimit,
		itemLimit,
	)

	respondJSON(w, http.StatusOK, map[string]any{"rows": rows})
}

func (s *appState) handleXMLTV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=300")
	_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><tv generator-info-name="Vortexo Add-on Server"></tv>`))
}

func (s *appState) handleLivePlayback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	streamID := strings.Trim(strings.TrimPrefix(r.URL.Path, "/live/"), "/")
	parts := strings.Split(streamID, "/")
	if len(parts) > 0 {
		streamID = parts[len(parts)-1]
	}
	streamID = strings.TrimSuffix(streamID, ".m3u8")
	streamID = strings.TrimSuffix(streamID, ".ts")
	streamID = strings.TrimSpace(streamID)
	if streamID == "" {
		respondError(w, http.StatusNotFound, "channel not found")
		return
	}

	channels := s.collectLiveChannels(r.Context(), 1000)
	var match *vortexoLiveChannel
	for i := range channels {
		if strings.EqualFold(channels[i].ID, streamID) || strconv.Itoa(channels[i].StreamID) == streamID {
			match = &channels[i]
			break
		}
	}
	if match == nil {
		respondError(w, http.StatusNotFound, "channel not found")
		return
	}

	playURL, err := s.resolveLiveChannelURL(r.Context(), *match)
	if err != nil || strings.TrimSpace(playURL) == "" {
		if err == nil {
			err = fmt.Errorf("empty stream URL")
		}
		respondError(w, http.StatusBadGateway, "live channel stream unavailable: "+err.Error())
		return
	}

	http.Redirect(w, r, playURL, http.StatusTemporaryRedirect)
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
	defaultRowLimit := 12
	s.mu.RLock()
	keywordConfig := normalizeTMDBKeywordRowsConfig(s.config.TMDBKeywordRows)
	s.mu.RUnlock()
	configuredRowLimit := 0
	if keywordConfig.Enabled {
		configuredRowLimit += keywordConfig.RowCount
	}
	if configuredRowLimit > 0 {
		defaultRowLimit = maxInt(defaultRowLimit, configuredRowLimit)
	}
	rowLimit := boundedInt(r.URL.Query().Get("row_limit"), defaultRowLimit, 1, 60)
	itemLimit := boundedInt(r.URL.Query().Get("item_limit"), 30, 6, 50)

	entries := s.enabledCatalogEntries(r.Context())
	rows := make([]vortexoHomeRow, 0, rowLimit)
	used := map[string]bool{}
	now := time.Now().UTC()
	refreshAfter := now.Add(time.Hour)

	keywordRows, keywordRefreshAfter := s.tmdbKeywordHomeRows(r.Context(), itemLimit, rowLimit, now)
	if !keywordRefreshAfter.IsZero() {
		refreshAfter = minTime(refreshAfter, keywordRefreshAfter)
	}
	for _, row := range keywordRows {
		if len(rows) >= rowLimit {
			break
		}
		rowItems := make([]vortexoHomeItem, 0, len(row.Items))
		for _, item := range row.Items {
			key := homeDedupeKey(item)
			if key == "" || used[key] {
				continue
			}
			used[key] = true
			rowItems = append(rowItems, item)
		}
		if len(rowItems) == 0 {
			continue
		}
		row.Items = rowItems
		rows = append(rows, row)
	}

	for _, entry := range entries {
		if len(rows) >= rowLimit {
			break
		}
		if !manifestSupportsResource(entry.Manifest, "catalog") {
			continue
		}
		mediaType := normalizeCatalogType(entry.Catalog.Type)
		if mediaType == "" {
			continue
		}
		items, err := s.fetchCatalog(r.Context(), entry.Base, entry.Catalog, itemLimit*2)
		if err != nil {
			log.Printf("catalog %s/%s failed: %v", entry.Catalog.Type, entry.Catalog.ID, err)
			continue
		}
		rowItems := make([]vortexoHomeItem, 0, itemLimit)
		for _, meta := range items {
			homeItem := homeItemFromStremio(meta, mediaType)
			s.applyCachedPlexArtworkToHomeItem(&homeItem)
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
		title := firstNonEmpty(entry.Pref.Name, entry.Catalog.Name, entry.Item.Name, entry.Manifest.Name, "Recommended")
		rows = append(rows, vortexoHomeRow{
			ID:           slug(entry.Pref.Key),
			Title:        title,
			Reason:       "Installed manifest catalog",
			RefreshAfter: refreshAfter,
			Items:        rowItems,
		})
	}

	respondJSON(w, http.StatusOK, vortexoHomeFeed{
		GeneratedAt:  now,
		RefreshAfter: refreshAfter,
		Rows:         rows,
	})
}

func (s *appState) collectManifestItems(ctx context.Context, mediaType string, limit int, offset int) []vortexoHomeItem {
	entries := s.enabledCatalogEntries(ctx)
	seen := map[string]bool{}
	collected := make([]vortexoHomeItem, 0, limit)
	skip := offset

	for _, entry := range entries {
		if len(collected) >= limit {
			break
		}
		if !manifestSupportsResource(entry.Manifest, "catalog") {
			continue
		}
		catalogType := normalizeCatalogType(entry.Catalog.Type)
		if catalogType == "" || (catalogType != "mixed" && mediaType != "" && catalogType != mediaType) {
			continue
		}
		items, err := s.fetchCatalog(ctx, entry.Base, entry.Catalog, limit+offset+25)
		if err != nil {
			log.Printf("library catalog %s/%s failed: %v", entry.Catalog.Type, entry.Catalog.ID, err)
			continue
		}
		for _, meta := range items {
			homeItem := homeItemFromStremio(meta, mediaType)
			s.applyCachedPlexArtworkToHomeItem(&homeItem)
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

	return collected
}

func (s *appState) collectLiveChannels(ctx context.Context, limit int) []vortexoLiveChannel {
	if limit <= 0 {
		limit = 500
	}
	entries := s.enabledCatalogEntries(ctx)
	seen := map[string]bool{}
	channels := make([]vortexoLiveChannel, 0, minInt(limit, 100))
	streamID := 1

	for _, entry := range entries {
		if len(channels) >= limit {
			break
		}
		if !manifestSupportsResource(entry.Manifest, "catalog") {
			continue
		}
		if !isLiveCatalog(entry.Manifest, entry.Item, entry.Catalog) {
			continue
		}
		items, err := s.fetchCatalog(ctx, entry.Base, entry.Catalog, limit*2)
		if err != nil {
			log.Printf("live catalog %s/%s failed: %v", entry.Catalog.Type, entry.Catalog.ID, err)
			continue
		}
		for _, meta := range items {
			if len(channels) >= limit {
				break
			}
			channel := liveChannelFromStremio(meta, entry.Item, entry.Manifest, entry.Base, entry.Catalog, streamID)
			if channel.ID == "" || channel.Name == "" {
				continue
			}
			if channel.StreamURL == "" && !manifestSupportsResource(entry.Manifest, "stream") {
				continue
			}
			key := strings.ToLower(channel.ManifestID + ":" + channel.CatalogType + ":" + channel.ID)
			if seen[key] {
				continue
			}
			seen[key] = true
			channels = append(channels, channel)
			streamID++
		}
	}

	return channels
}

func liveChannelFromStremio(
	meta stremioMeta,
	item installedManifest,
	manifest stremioManifest,
	base string,
	catalog stremioCatalog,
	streamID int,
) vortexoLiveChannel {
	name := firstNonEmpty(meta.Name, meta.Title, meta.OriginalName, meta.OriginalTitle)
	id := firstNonEmpty(meta.ID, meta.IMDBID, slug(name))
	category := firstNonEmpty(strings.Join(meta.Genres, ", "), catalog.Name, item.Name, manifest.Name, "Live TV")
	categoryID := slug(firstNonEmpty(catalog.ID, category, "live-tv"))
	source := firstNonEmpty(item.Name, manifest.Name, "Vortexo Server")
	streamURL := absoluteAddonURL(firstNonEmpty(meta.StreamURL, meta.URL, meta.ExternalURL), base)
	logo := absoluteAddonURL(firstNonEmpty(meta.Logo, meta.Poster, meta.Background), base)

	return vortexoLiveChannel{
		ID:             id,
		StreamID:       streamID,
		EPGChannelID:   id,
		CategoryID:     categoryID,
		Name:           firstNonEmpty(name, id, "Channel"),
		Logo:           logo,
		StreamIcon:     logo,
		StreamURL:      streamURL,
		URL:            streamURL,
		Category:       category,
		CategoryName:   category,
		IsLive:         true,
		Active:         true,
		Source:         source,
		HasEPG:         false,
		ManifestBase:   base,
		ManifestName:   manifest.Name,
		ManifestID:     firstNonEmpty(item.ID, manifest.ID, source),
		CatalogType:    catalog.Type,
		CatalogID:      catalog.ID,
		OriginalItemID: id,
	}
}

func (s *appState) resolveLiveChannelURL(ctx context.Context, channel vortexoLiveChannel) (string, error) {
	if channel.StreamURL != "" {
		return channel.StreamURL, nil
	}
	if channel.ManifestBase == "" || channel.OriginalItemID == "" {
		return "", fmt.Errorf("missing live channel stream metadata")
	}
	return s.fetchLiveStreamURL(ctx, channel.ManifestBase, channel.CatalogType, channel.OriginalItemID)
}

func (s *appState) fetchLiveStreamURL(ctx context.Context, base string, catalogType string, id string) (string, error) {
	var lastErr error
	for _, streamType := range liveStreamTypes(catalogType) {
		u := fmt.Sprintf("%s/stream/%s/%s.json", strings.TrimRight(base, "/"), url.PathEscape(streamType), url.PathEscape(id))
		var response stremioStreamResponse
		if err := s.getJSON(ctx, u, &response); err != nil {
			lastErr = err
			continue
		}
		for _, stream := range response.Streams {
			playURL := absoluteAddonURL(firstNonEmpty(stream.URL, stream.ExternalURL), base)
			if playURL != "" {
				return playURL, nil
			}
		}
	}
	if lastErr != nil {
		return "", lastErr
	}
	return "", fmt.Errorf("empty stream response")
}

func liveRowsFromChannels(
	channels []vortexoLiveChannel,
	favoriteIDs map[string]bool,
	recentIDs map[string]bool,
	rowLimit int,
	itemLimit int,
) []vortexoLiveTVRow {
	rows := make([]vortexoLiveTVRow, 0, rowLimit)
	if len(channels) == 0 || rowLimit <= 0 {
		return rows
	}

	addRow := func(id string, title string, reason string, items []vortexoLiveChannel) {
		if len(rows) >= rowLimit || len(items) == 0 {
			return
		}
		if len(items) > itemLimit {
			items = items[:itemLimit]
		}
		rowItems := make([]vortexoLiveTVRowItem, 0, len(items))
		for _, channel := range items {
			rowItems = append(rowItems, liveRowItemFromChannel(channel))
		}
		rows = append(rows, vortexoLiveTVRow{
			ID:     id,
			Title:  title,
			Reason: reason,
			Items:  rowItems,
		})
	}

	if len(favoriteIDs) > 0 {
		favorites := make([]vortexoLiveChannel, 0, len(favoriteIDs))
		for _, channel := range channels {
			if favoriteIDs[channel.ID] {
				favorites = append(favorites, channel)
			}
		}
		addRow("favorites", "Favorite Channels", "Saved on Apple TV", favorites)
	}

	if len(recentIDs) > 0 {
		recent := make([]vortexoLiveChannel, 0, len(recentIDs))
		for _, channel := range channels {
			if recentIDs[channel.ID] {
				recent = append(recent, channel)
			}
		}
		addRow("recent", "Recently Watched", "From this Vortexo Server", recent)
	}

	addRow("all", "All Channels", fmt.Sprintf("%d channels", len(channels)), channels)

	grouped := map[string][]vortexoLiveChannel{}
	order := []string{}
	for _, channel := range channels {
		category := firstNonEmpty(channel.Category, channel.Source, "Live TV")
		if _, ok := grouped[category]; !ok {
			order = append(order, category)
		}
		grouped[category] = append(grouped[category], channel)
	}
	sort.Strings(order)
	for _, category := range order {
		addRow("category-"+slug(category), category, "Installed live manifest", grouped[category])
	}

	return rows
}

func liveRowItemFromChannel(channel vortexoLiveChannel) vortexoLiveTVRowItem {
	return vortexoLiveTVRowItem{
		ID:       channel.ID,
		Name:     channel.Name,
		Logo:     channel.Logo,
		Category: channel.Category,
		Source:   channel.Source,
		HasEPG:   channel.HasEPG,
	}
}

func liveCategoriesFromChannels(channels []vortexoLiveChannel) []xtreamLiveCategory {
	seen := map[string]bool{}
	categories := make([]xtreamLiveCategory, 0)
	for _, channel := range channels {
		id := firstNonEmpty(channel.CategoryID, slug(channel.Category), "live-tv")
		name := firstNonEmpty(channel.Category, channel.CategoryName, "Live TV")
		key := strings.ToLower(id)
		if seen[key] {
			continue
		}
		seen[key] = true
		categories = append(categories, xtreamLiveCategory{
			CategoryID:   id,
			CategoryName: name,
		})
	}
	sort.SliceStable(categories, func(i, j int) bool {
		return categories[i].CategoryName < categories[j].CategoryName
	})
	return categories
}

func (s *appState) searchManifestItems(ctx context.Context, query string, mediaType string, limit int) []vortexoHomeItem {
	entries := s.enabledCatalogEntries(ctx)
	seen := map[string]bool{}
	collected := make([]vortexoHomeItem, 0, limit)
	normalizedQuery := strings.ToLower(strings.TrimSpace(query))

	for _, entry := range entries {
		if len(collected) >= limit {
			break
		}
		if !manifestSupportsResource(entry.Manifest, "catalog") {
			continue
		}
		catalogType := normalizeCatalogType(entry.Catalog.Type)
		if catalogType == "" || (catalogType != "mixed" && mediaType != "" && catalogType != mediaType) {
			continue
		}
		if !catalogSupportsSearch(entry.Catalog) {
			continue
		}
		items, err := s.fetchCatalogSearch(ctx, entry.Base, entry.Catalog, query, limit*2)
		if err != nil {
			log.Printf("search catalog %s/%s failed: %v", entry.Catalog.Type, entry.Catalog.ID, err)
			continue
		}
		for _, meta := range items {
			homeItem := homeItemFromStremio(meta, catalogType)
			s.applyCachedPlexArtworkToHomeItem(&homeItem)
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
	detail := manifestDetailFromStremio(meta, "movie")
	s.applyCachedPlexArtworkToHomeItem(&detail.vortexoHomeItem)
	respondJSON(w, http.StatusOK, map[string]any{"movie": detail})
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
		detail := manifestDetailFromStremio(meta, "series")
		s.applyCachedPlexArtworkToHomeItem(&detail.vortexoHomeItem)
		respondJSON(w, http.StatusOK, map[string]any{"series": detail})
	case "episodes":
		respondJSON(w, http.StatusOK, map[string]any{"episodes": manifestEpisodesFromStremio(meta)})
	default:
		respondError(w, http.StatusNotFound, "series endpoint not found")
	}
}

func (s *appState) handleTMDBByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	const prefix = "/api/v1/tmdb/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		respondError(w, http.StatusNotFound, "tmdb endpoint not found")
		return
	}

	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, prefix), "/"), "/")
	if len(parts) < 2 {
		respondError(w, http.StatusNotFound, "tmdb metadata not found")
		return
	}

	routeMediaType, stremioType, ok := normalizeTMDBRouteMediaType(parts[0])
	if !ok {
		respondError(w, http.StatusBadRequest, "unsupported tmdb media type")
		return
	}

	rawID, err := url.PathUnescape(parts[1])
	if err != nil {
		rawID = parts[1]
	}
	tmdbIDText := strings.TrimPrefix(strings.TrimSpace(rawID), "tmdb:")
	tmdbID, err := strconv.Atoi(tmdbIDText)
	if err != nil || tmdbID <= 0 {
		respondError(w, http.StatusBadRequest, "invalid tmdb id")
		return
	}

	tail := ""
	if len(parts) > 2 {
		tail = strings.Join(parts[2:], "/")
	}

	meta, manifestErr := s.findManifestMetaByTMDBID(r.Context(), stremioType, tmdbID)
	if manifestErr == nil {
		switch tail {
		case "":
			respondJSON(w, http.StatusOK, map[string]any{
				"details": tmdbDetailsFromManifest(meta, routeMediaType, tmdbID),
			})
		case "seasons":
			if routeMediaType != "tv" {
				respondError(w, http.StatusNotFound, "tmdb seasons not found")
				return
			}
			respondJSON(w, http.StatusOK, map[string]any{
				"seasons": tmdbSeasonsFromManifest(meta),
			})
		case "episodes":
			if routeMediaType != "tv" {
				respondError(w, http.StatusNotFound, "tmdb episodes not found")
				return
			}
			respondJSON(w, http.StatusOK, map[string]any{
				"episodes": manifestEpisodesFromStremio(meta),
			})
		default:
			respondError(w, http.StatusNotFound, "tmdb endpoint not found")
		}
		return
	}

	var tmdbAPIErr error
	switch tail {
	case "":
		if routeMediaType == "tv" {
			details, err := s.tmdbTVDetailsFromAPI(r.Context(), tmdbID)
			if err == nil {
				respondJSON(w, http.StatusOK, map[string]any{"details": details})
				return
			}
			tmdbAPIErr = err
		} else {
			details, err := s.tmdbMovieDetailsFromAPI(r.Context(), tmdbID)
			if err == nil {
				respondJSON(w, http.StatusOK, map[string]any{"details": details})
				return
			}
			tmdbAPIErr = err
		}
	case "seasons":
		if routeMediaType != "tv" {
			respondError(w, http.StatusNotFound, "tmdb seasons not found")
			return
		}
		seasons, err := s.tmdbTVSeasonsFromAPI(r.Context(), tmdbID)
		if err == nil {
			respondJSON(w, http.StatusOK, map[string]any{"seasons": seasons})
			return
		}
		tmdbAPIErr = err
	case "episodes":
		if routeMediaType != "tv" {
			respondError(w, http.StatusNotFound, "tmdb episodes not found")
			return
		}
		episodes, err := s.tmdbTVEpisodesFromAPI(r.Context(), tmdbID)
		if err == nil {
			respondJSON(w, http.StatusOK, map[string]any{"episodes": episodes})
			return
		}
		tmdbAPIErr = err
	default:
		respondError(w, http.StatusNotFound, "tmdb endpoint not found")
		return
	}

	if routeMediaType == "tv" {
		switch tail {
		case "":
			details, err := s.tmdbTVDetailsFromPlexDiscover(r.Context(), tmdbID)
			if err != nil {
				respondError(w, http.StatusNotFound, joinedErrorMessage(manifestErr, tmdbAPIErr, err))
				return
			}
			respondJSON(w, http.StatusOK, map[string]any{"details": details})
		case "seasons":
			seasons, err := s.tmdbTVSeasonsFromPlexDiscover(r.Context(), tmdbID)
			if err != nil {
				respondError(w, http.StatusNotFound, joinedErrorMessage(manifestErr, tmdbAPIErr, err))
				return
			}
			respondJSON(w, http.StatusOK, map[string]any{"seasons": seasons})
		case "episodes":
			episodes, err := s.tmdbTVEpisodesFromPlexDiscover(r.Context(), tmdbID)
			if err != nil {
				respondError(w, http.StatusNotFound, joinedErrorMessage(manifestErr, tmdbAPIErr, err))
				return
			}
			respondJSON(w, http.StatusOK, map[string]any{"episodes": episodes})
		default:
			respondError(w, http.StatusNotFound, "tmdb endpoint not found")
		}
		return
	}

	respondError(w, http.StatusNotFound, joinedErrorMessage(manifestErr, tmdbAPIErr))
}

func normalizeTMDBRouteMediaType(value string) (routeMediaType string, stremioType string, ok bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "movie", "movies":
		return "movie", "movie", true
	case "tv", "show", "shows", "series":
		return "tv", "series", true
	default:
		return "", "", false
	}
}

func (s *appState) findManifestMetaByTMDBID(ctx context.Context, mediaType string, tmdbID int) (stremioMeta, error) {
	if tmdbID <= 0 {
		return stremioMeta{}, fmt.Errorf("missing tmdb id")
	}

	ids := []string{
		"tmdb:" + strconv.Itoa(tmdbID),
		strconv.Itoa(tmdbID),
	}
	var lastErr error
	for _, id := range ids {
		meta, err := s.findManifestMeta(ctx, mediaType, id)
		if err == nil {
			return meta, nil
		}
		lastErr = err
	}

	if lastErr != nil {
		return stremioMeta{}, fmt.Errorf("manifest metadata not found for tmdb:%d: %w", tmdbID, lastErr)
	}
	return stremioMeta{}, fmt.Errorf("manifest metadata not found for tmdb:%d", tmdbID)
}

func tmdbDetailsFromManifest(meta stremioMeta, mediaType string, fallbackTMDBID int) map[string]any {
	fallbackType := "movie"
	if mediaType == "tv" {
		fallbackType = "series"
	}

	detail := manifestDetailFromStremio(meta, fallbackType)
	item := detail.vortexoHomeItem
	tmdbID := item.TMDBID
	if tmdbID <= 0 {
		tmdbID = fallbackTMDBID
	}

	details := map[string]any{
		"id":                 tmdbID,
		"overview":           item.Overview,
		"poster_path":        item.PosterPath,
		"backdrop_path":      item.BackdropPath,
		"poster_paths":       stringList(item.PosterPath),
		"backdrop_paths":     stringList(item.BackdropPath),
		"landscape_paths":    stringList(item.LandscapePath),
		"logo_path":          item.LogoPath,
		"logo_paths":         stringList(item.LogoPath),
		"genres":             item.Genres,
		"vote_average":       item.VoteAverage,
		"runtime":            item.Runtime,
		"number_of_seasons":  detail.NumberOfSeasons,
		"number_of_episodes": detail.NumberOfEpisodes,
	}

	if mediaType == "tv" {
		details["name"] = item.Title
		details["first_air_date"] = item.FirstAirDate
	} else {
		details["title"] = item.Title
		details["release_date"] = item.ReleaseDate
	}

	return details
}

func tmdbSeasonsFromManifest(meta stremioMeta) []map[string]any {
	detail := manifestDetailFromStremio(meta, "series")
	episodes := manifestEpisodesFromStremio(meta)
	episodesBySeason := map[int][]vortexoManifestEpisode{}
	for _, episode := range episodes {
		episodesBySeason[episode.SeasonNumber] = append(episodesBySeason[episode.SeasonNumber], episode)
	}

	seasonSet := map[int]bool{}
	for seasonNumber := range episodesBySeason {
		seasonSet[seasonNumber] = true
	}
	if len(seasonSet) == 0 && detail.NumberOfSeasons > 0 {
		for seasonNumber := 1; seasonNumber <= detail.NumberOfSeasons; seasonNumber++ {
			seasonSet[seasonNumber] = true
		}
	}

	seasonNumbers := make([]int, 0, len(seasonSet))
	for seasonNumber := range seasonSet {
		seasonNumbers = append(seasonNumbers, seasonNumber)
	}
	sort.Ints(seasonNumbers)

	seasons := make([]map[string]any, 0, len(seasonNumbers))
	for _, seasonNumber := range seasonNumbers {
		seasonEpisodes := episodesBySeason[seasonNumber]
		airDate := ""
		for _, episode := range seasonEpisodes {
			if episode.AirDate != "" {
				airDate = episode.AirDate
				break
			}
		}

		name := fmt.Sprintf("Season %d", seasonNumber)
		if seasonNumber == 0 {
			name = "Specials"
		}

		seasons = append(seasons, map[string]any{
			"id":            seasonNumber,
			"season_number": seasonNumber,
			"name":          name,
			"overview":      detail.Overview,
			"poster_path":   detail.PosterPath,
			"air_date":      airDate,
			"episode_count": len(seasonEpisodes),
		})
	}

	return seasons
}

func (s *appState) tmdbTVDetailsFromPlexDiscover(ctx context.Context, tmdbID int) (map[string]any, error) {
	meta, token, err := s.plexDiscoverTVMetadataForTMDB(ctx, tmdbID)
	if err != nil {
		return nil, err
	}
	return tmdbDetailsFromPlexDiscover(meta, token, tmdbID), nil
}

func (s *appState) tmdbTVSeasonsFromPlexDiscover(ctx context.Context, tmdbID int) ([]map[string]any, error) {
	meta, token, err := s.plexDiscoverTVMetadataForTMDB(ctx, tmdbID)
	if err != nil {
		return nil, err
	}
	discoverID := plexDiscoverMetadataID(meta)
	if discoverID == "" {
		return nil, fmt.Errorf("Plex Discover show id missing")
	}
	items, err := s.getSignedPlexDiscoverMetadataList(ctx, token, "/library/metadata/"+discoverID+"/children", 1000)
	if err != nil {
		return nil, err
	}
	seasons := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if strings.ToLower(strings.TrimSpace(item.Type)) != "season" {
			continue
		}
		seasons = append(seasons, tmdbSeasonFromPlexDiscover(item, token))
	}
	sort.SliceStable(seasons, func(i, j int) bool {
		return intFromAny(seasons[i]["season_number"]) < intFromAny(seasons[j]["season_number"])
	})
	return seasons, nil
}

func (s *appState) tmdbTVEpisodesFromPlexDiscover(ctx context.Context, tmdbID int) ([]map[string]any, error) {
	meta, token, err := s.plexDiscoverTVMetadataForTMDB(ctx, tmdbID)
	if err != nil {
		return nil, err
	}
	discoverID := plexDiscoverMetadataID(meta)
	if discoverID == "" {
		return nil, fmt.Errorf("Plex Discover show id missing")
	}
	items, err := s.getSignedPlexDiscoverMetadataList(ctx, token, "/library/metadata/"+discoverID+"/grandchildren", 5000)
	if err != nil {
		return nil, err
	}
	episodes := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if strings.ToLower(strings.TrimSpace(item.Type)) != "episode" {
			continue
		}
		episodes = append(episodes, tmdbEpisodeFromPlexDiscover(item, token))
	}
	sort.SliceStable(episodes, func(i, j int) bool {
		leftSeason := intFromAny(episodes[i]["season_number"])
		rightSeason := intFromAny(episodes[j]["season_number"])
		if leftSeason != rightSeason {
			return leftSeason < rightSeason
		}
		return intFromAny(episodes[i]["episode_number"]) < intFromAny(episodes[j]["episode_number"])
	})
	return episodes, nil
}

func (s *appState) plexDiscoverTVMetadataForTMDB(ctx context.Context, tmdbID int) (plexDiscoverMetadata, string, error) {
	token := s.plexAccountToken()
	if token == "" {
		return plexDiscoverMetadata{}, "", fmt.Errorf("Plex account token is not configured")
	}

	seed := s.plexDiscoverSeedForTMDBID("tv", tmdbID)
	if strings.TrimSpace(seed.Title) == "" {
		return plexDiscoverMetadata{}, "", fmt.Errorf("no watch-state title for tmdb:%d", tmdbID)
	}

	results, err := s.searchSignedPlexDiscoverMetadata(ctx, token, seed.Title, 12)
	if err != nil {
		return plexDiscoverMetadata{}, "", err
	}
	match := bestPlexDiscoverSearchMatch(results, seed)
	if match == nil {
		return plexDiscoverMetadata{}, "", fmt.Errorf("Plex Discover metadata not found for tmdb:%d", tmdbID)
	}

	if discoverID := plexDiscoverMetadataID(*match); discoverID != "" {
		if hydrated, err := s.getSignedPlexDiscoverMetadata(ctx, token, discoverID); err == nil && hydrated != nil {
			return *hydrated, token, nil
		}
	}
	return *match, token, nil
}

func (s *appState) plexDiscoverSeedForTMDBID(mediaType string, tmdbID int) plexArtworkSeedItem {
	seed := plexArtworkSeedItem{
		MediaType: normalizePlexArtworkMediaType(mediaType),
		TMDBID:    tmdbID,
	}

	s.mu.RLock()
	for _, item := range s.watchState.Items {
		if item.TMDBID != tmdbID {
			continue
		}
		if seed.Title == "" {
			seed.Title = firstNonEmpty(item.ParentTitle, item.Title)
		}
		if seed.IMDBID == "" {
			seed.IMDBID = item.IMDBID
		}
		if seed.Year == 0 {
			seed.Year = item.Year
		}
		if seed.Title != "" && seed.IMDBID != "" {
			break
		}
	}
	s.mu.RUnlock()

	if record, ok := s.getCachedPlexArtwork(seed.MediaType, tmdbID, seed.IMDBID, seed.Title, seed.Year); ok {
		seed.Title = firstNonEmpty(seed.Title, record.Title)
		seed.IMDBID = firstNonEmpty(seed.IMDBID, record.IMDBID)
		seed.Year = firstNonZero(seed.Year, record.Year)
	}
	return seed
}

func tmdbDetailsFromPlexDiscover(meta plexDiscoverMetadata, token string, fallbackTMDBID int) map[string]any {
	artwork := plexArtworkFromDiscoverMetadata(meta, token)
	tmdbID, _ := plexDiscoverExternalIDs(meta, "tv")
	if tmdbID <= 0 {
		tmdbID = fallbackTMDBID
	}
	return map[string]any{
		"id":                 tmdbID,
		"name":               firstNonEmpty(meta.Title, meta.OriginalTitle),
		"overview":           meta.Summary,
		"poster_path":        firstNonEmpty(firstPlexArtworkURL(artwork.Thumbnail), meta.Thumb),
		"backdrop_path":      firstNonEmpty(firstPlexArtworkURL(artwork.Background), meta.Art),
		"poster_paths":       uniqueNonEmptyStrings([]string{firstPlexArtworkURL(artwork.Thumbnail), meta.Thumb}),
		"backdrop_paths":     uniqueNonEmptyStrings([]string{firstPlexArtworkURL(artwork.Background), meta.Art}),
		"landscape_paths":    uniqueNonEmptyStrings([]string{firstPlexArtworkURL(artwork.Landscape, artwork.CoverArt), meta.Banner}),
		"logo_path":          firstPlexArtworkURL(artwork.ClearLogo),
		"logo_paths":         uniqueNonEmptyStrings([]string{firstPlexArtworkURL(artwork.ClearLogo)}),
		"genres":             []string{},
		"vote_average":       firstNonZeroFloat(float64(meta.AudienceRating), float64(meta.Rating)),
		"first_air_date":     meta.OriginallyAvailableAt,
		"number_of_seasons":  meta.ChildCount,
		"number_of_episodes": meta.LeafCount,
	}
}

func tmdbSeasonFromPlexDiscover(meta plexDiscoverMetadata, token string) map[string]any {
	artwork := plexArtworkFromDiscoverMetadata(meta, token)
	seasonNumber := meta.Index
	return map[string]any{
		"id":            firstNonZero(meta.Index, intFromAny(meta.RatingKey)),
		"season_number": seasonNumber,
		"name":          firstNonEmpty(meta.Title, fmt.Sprintf("Season %d", seasonNumber)),
		"overview":      meta.Summary,
		"poster_path":   firstNonEmpty(firstPlexArtworkURL(artwork.Thumbnail), meta.Thumb),
		"air_date":      meta.OriginallyAvailableAt,
		"episode_count": meta.LeafCount,
	}
}

func tmdbEpisodeFromPlexDiscover(meta plexDiscoverMetadata, token string) map[string]any {
	artwork := plexArtworkFromDiscoverMetadata(meta, token)
	runtimeMinutes := 0
	if meta.Duration > 0 {
		runtimeMinutes = meta.Duration / 60000
	}
	return map[string]any{
		"id":             firstNonEmpty(meta.RatingKey, meta.Key, meta.GUID),
		"title":          firstNonEmpty(meta.Title, fmt.Sprintf("Episode %d", meta.Index)),
		"overview":       meta.Summary,
		"still_path":     firstNonEmpty(meta.Thumb, firstPlexArtworkURL(artwork.Thumbnail), firstPlexArtworkURL(artwork.Landscape, artwork.CoverArt), meta.Art),
		"season_number":  meta.ParentIndex,
		"episode_number": meta.Index,
		"runtime":        runtimeMinutes,
		"air_date":       meta.OriginallyAvailableAt,
		"vote_average":   firstNonZeroFloat(float64(meta.AudienceRating), float64(meta.Rating)),
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
				"server_name": "Vortexo Add-on Server",
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
	case "get_live_streams":
		respondJSON(w, http.StatusOK, s.collectLiveChannels(r.Context(), 1000))
	case "get_live_categories":
		respondJSON(w, http.StatusOK, liveCategoriesFromChannels(s.collectLiveChannels(r.Context(), 1000)))
	case "get_vod_streams", "get_series", "get_vod_categories", "get_series_categories":
		respondJSON(w, http.StatusOK, []any{})
	default:
		respondJSON(w, http.StatusOK, []any{})
	}
}

func (s *appState) watchStateCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.watchState.Items)
}

func (s *appState) upsertWatchStateItems(items []watchStateItem) error {
	if len(items) == 0 {
		return nil
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := map[string]int{}
	for i := range s.watchState.Items {
		key := watchStateKey(s.watchState.Items[i])
		if key != "" {
			existing[key] = i
		}
	}

	for _, item := range items {
		key := watchStateKey(item)
		if key == "" {
			continue
		}
		item.ID = key
		if item.UpdatedAt.IsZero() {
			item.UpdatedAt = now
		}
		if idx, ok := existing[key]; ok {
			s.watchState.Items[idx] = mergeWatchStateItem(s.watchState.Items[idx], item)
			continue
		}
		existing[key] = len(s.watchState.Items)
		s.watchState.Items = append(s.watchState.Items, item)
	}

	sort.SliceStable(s.watchState.Items, func(i, j int) bool {
		return s.watchState.Items[i].UpdatedAt.After(s.watchState.Items[j].UpdatedAt)
	})
	return s.saveWatchStateLocked()
}

func (s *appState) pruneStaleTraktUpNextItems(activeKeys map[string]bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.watchState.Items) == 0 {
		return nil
	}

	filtered := s.watchState.Items[:0]
	changed := false
	for _, item := range s.watchState.Items {
		key := watchStateKey(item)
		isUpNext := strings.Contains(strings.ToLower(item.Source), "trakt-up-next")
		isBareUpNext := isUpNext && !item.Watched && item.ProgressPercent <= 0 && item.ProgressSeconds <= 0
		if isBareUpNext && !activeKeys[key] {
			changed = true
			continue
		}
		filtered = append(filtered, item)
	}
	if !changed {
		return nil
	}
	s.watchState.Items = filtered
	return s.saveWatchStateLocked()
}

func watchStateKeySet(items []watchStateItem) map[string]bool {
	keys := make(map[string]bool, len(items))
	for _, item := range items {
		key := watchStateKey(item)
		if key != "" {
			keys[key] = true
		}
	}
	return keys
}

func (s *appState) createTraktDeviceCode(ctx context.Context) (traktDeviceCodeResponse, error) {
	s.mu.RLock()
	clientID := strings.TrimSpace(s.config.Watch.Trakt.ClientID)
	s.mu.RUnlock()
	if clientID == "" {
		return traktDeviceCodeResponse{}, fmt.Errorf("Trakt client ID is required")
	}

	body, _ := json.Marshal(map[string]string{"client_id": clientID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.trakt.tv/oauth/device/code", bytes.NewReader(body))
	if err != nil {
		return traktDeviceCodeResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return traktDeviceCodeResponse{}, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return traktDeviceCodeResponse{}, fmt.Errorf("Trakt device code failed: HTTP %d %s", resp.StatusCode, responseMessage(data))
	}
	var out traktDeviceCodeResponse
	if err := json.Unmarshal(data, &out); err != nil {
		return traktDeviceCodeResponse{}, err
	}
	return out, nil
}

func (s *appState) pollTraktDeviceToken(ctx context.Context, deviceCode string) (map[string]any, error) {
	deviceCode = strings.TrimSpace(deviceCode)
	if deviceCode == "" {
		return nil, fmt.Errorf("device code is required")
	}
	s.mu.RLock()
	clientID := strings.TrimSpace(s.config.Watch.Trakt.ClientID)
	clientSecret := strings.TrimSpace(s.config.Watch.Trakt.ClientSecret)
	s.mu.RUnlock()
	if clientID == "" || clientSecret == "" {
		return nil, fmt.Errorf("Trakt client ID and secret are required")
	}

	body, _ := json.Marshal(map[string]string{
		"code":          deviceCode,
		"client_id":     clientID,
		"client_secret": clientSecret,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.trakt.tv/oauth/device/token", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("Trakt device token pending or failed: HTTP %d %s", resp.StatusCode, responseMessage(data))
	}
	var token traktTokenResponse
	if err := json.Unmarshal(data, &token); err != nil {
		return nil, err
	}
	expiresAt := time.Now().UTC().Add(time.Duration(token.ExpiresIn) * time.Second)
	s.mu.Lock()
	s.config.Watch.Trakt.AccessToken = token.AccessToken
	s.config.Watch.Trakt.RefreshToken = token.RefreshToken
	s.config.Watch.Trakt.TokenExpiresAt = expiresAt
	err = s.saveLocked()
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"ok":               true,
		"has_access_token": token.AccessToken != "",
		"expires_at":       expiresAt,
	}, nil
}

func (s *appState) runTraktWatchSyncWorker(ctx context.Context) {
	timer := time.NewTimer(traktWatchSyncInitialDelay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			s.syncTraktWatchStateIfConfigured(ctx, "background")
			timer.Reset(traktWatchSyncInterval)
		}
	}
}

func (s *appState) traktSyncConfigured() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return strings.TrimSpace(s.config.Watch.Trakt.ClientID) != "" &&
		strings.TrimSpace(s.config.Watch.Trakt.AccessToken) != ""
}

func (s *appState) syncTraktWatchStateIfConfigured(ctx context.Context, reason string) {
	if !s.traktSyncConfigured() {
		return
	}
	syncCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	items, err := s.syncTraktWatchStateExclusive(syncCtx, reason)
	if err != nil {
		log.Printf("Trakt watch sync %s failed: %v", reason, err)
		return
	}
	log.Printf("Trakt watch sync %s imported=%d total=%d", reason, len(items), s.watchStateCount())
}

func (s *appState) syncTraktWatchStateExclusive(ctx context.Context, reason string) ([]watchStateItem, error) {
	_ = reason
	s.watchSyncMu.Lock()
	defer s.watchSyncMu.Unlock()
	return s.syncTraktWatchState(ctx)
}

func (s *appState) syncTraktWatchState(ctx context.Context) ([]watchStateItem, error) {
	var watchedMovies []traktWatchedMovie
	var watchedShows []traktWatchedShow
	var playbackMovies []traktPlaybackMovie
	var playbackEpisodes []traktPlaybackEpisode

	if err := s.traktGetJSON(ctx, "/sync/watched/movies", &watchedMovies); err != nil {
		return nil, err
	}
	if err := s.traktGetJSON(ctx, "/sync/watched/shows", &watchedShows); err != nil {
		return nil, err
	}
	if err := s.traktGetJSON(ctx, "/sync/playback/movies", &playbackMovies); err != nil {
		return nil, err
	}
	if err := s.traktGetJSON(ctx, "/sync/playback/episodes", &playbackEpisodes); err != nil {
		return nil, err
	}

	upNextEpisodes := s.traktUpNextWatchItems(ctx, watchedShows)
	if atomOrder := s.traktUpNextAtomOrder(ctx); len(atomOrder) > 0 {
		applyTraktUpNextAtomOrder(upNextEpisodes, atomOrder)
	}

	items := make([]watchStateItem, 0, len(watchedMovies)+len(playbackMovies)+len(playbackEpisodes)+len(upNextEpisodes))
	for _, entry := range watchedMovies {
		items = append(items, watchItemFromTraktMovie(entry.Movie, true, entry.LastWatchedAt, 0, entry.Plays))
	}
	for _, show := range watchedShows {
		for _, season := range show.Seasons {
			for _, episode := range season.Episodes {
				item := watchItemFromTraktEpisode(show.Show, traktEpisode{
					Season: season.Number,
					Number: episode.Number,
				}, true, episode.LastWatchedAt, 0, episode.Plays)
				items = append(items, item)
			}
		}
	}
	for _, entry := range playbackMovies {
		items = append(items, watchItemFromTraktMovie(entry.Movie, entry.Progress >= 90, entry.PausedAt, entry.Progress, 0))
	}
	for _, entry := range playbackEpisodes {
		items = append(items, watchItemFromTraktEpisode(entry.Show, entry.Episode, entry.Progress >= 90, entry.PausedAt, entry.Progress, 0))
	}
	items = append(items, upNextEpisodes...)

	if err := s.upsertWatchStateItems(items); err != nil {
		return nil, err
	}
	if err := s.pruneStaleTraktUpNextItems(watchStateKeySet(upNextEpisodes)); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.config.Watch.Trakt.LastSyncAt = time.Now().UTC()
	err := s.saveLocked()
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return items, nil
}

func (s *appState) traktUpNextWatchItems(ctx context.Context, watchedShows []traktWatchedShow) []watchStateItem {
	const maxShows = 100

	shows := append([]traktWatchedShow(nil), watchedShows...)
	sort.SliceStable(shows, func(i, j int) bool {
		return shows[i].LastWatchedAt.After(shows[j].LastWatchedAt)
	})
	if len(shows) > maxShows {
		shows = shows[:maxShows]
	}

	now := time.Now().UTC()
	items := make([]watchStateItem, 0, len(shows))
	seen := make(map[string]bool, len(shows))
	for _, entry := range shows {
		if err := ctx.Err(); err != nil {
			break
		}
		showID := traktShowAPIID(entry.Show)
		if showID == "" {
			continue
		}

		var progress traktShowProgress
		path := "/shows/" + url.PathEscape(showID) + "/progress/watched?hidden=false&specials=false&count_specials=false&extended=full"
		if err := s.traktGetJSON(ctx, path, &progress); err != nil {
			log.Printf("Trakt up next skipped %q: %v", firstNonEmpty(entry.Show.Title, showID), err)
			continue
		}
		if progress.NextEpisode == nil {
			continue
		}
		episode := *progress.NextEpisode
		if episode.Season <= 0 || episode.Number <= 0 {
			continue
		}
		if !traktEpisodeHasAired(episode, now) {
			continue
		}

		updatedAt := maxTime(progress.LastWatchedAt, entry.LastWatchedAt)
		if updatedAt.IsZero() {
			updatedAt = now
		}
		item := watchItemFromTraktUpNextEpisode(entry.Show, episode, updatedAt)
		key := watchStateKey(item)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		items = append(items, item)
	}
	return items
}

func traktEpisodeHasAired(episode traktEpisode, now time.Time) bool {
	if episode.FirstAired.IsZero() {
		return false
	}
	return !episode.FirstAired.After(now.Add(6 * time.Hour))
}

func traktEpisodeAirDate(episode traktEpisode) string {
	if episode.FirstAired.IsZero() {
		return ""
	}
	return episode.FirstAired.UTC().Format("2006-01-02")
}

func (s *appState) traktUpNextAtomOrder(ctx context.Context) map[string]int {
	s.mu.RLock()
	feedURL := strings.TrimSpace(s.config.Watch.Trakt.UpNextAtomURL)
	s.mu.RUnlock()
	if feedURL == "" {
		return nil
	}
	parsedURL, err := url.Parse(feedURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		log.Printf("Trakt up next atom order skipped: invalid feed URL")
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, feedURL, nil)
	if err != nil {
		return nil
	}
	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("Trakt up next atom order skipped: %v", err)
		return nil
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("Trakt up next atom order skipped: HTTP %d %s", resp.StatusCode, responseMessage(data))
		return nil
	}
	var feed traktUpNextAtomFeed
	if err := xml.Unmarshal(data, &feed); err != nil {
		log.Printf("Trakt up next atom order skipped: %v", err)
		return nil
	}
	order := make(map[string]int, len(feed.Entries))
	for index, entry := range feed.Entries {
		for _, link := range entry.Links {
			match := traktAtomEpisodePattern.FindStringSubmatch(link.Href)
			if len(match) != 4 {
				continue
			}
			season, seasonErr := strconv.Atoi(match[2])
			episode, episodeErr := strconv.Atoi(match[3])
			if seasonErr != nil || episodeErr != nil {
				continue
			}
			key := traktAtomOrderKey(match[1], season, episode)
			if key != "" {
				if _, exists := order[key]; !exists {
					order[key] = index
				}
				break
			}
		}
	}
	return order
}

func applyTraktUpNextAtomOrder(items []watchStateItem, order map[string]int) {
	if len(items) == 0 || len(order) == 0 {
		return
	}
	sort.SliceStable(items, func(i, j int) bool {
		iOrder, iOK := order[watchStateAtomOrderKey(items[i])]
		jOrder, jOK := order[watchStateAtomOrderKey(items[j])]
		if iOK != jOK {
			return iOK
		}
		if iOK && iOrder != jOrder {
			return iOrder < jOrder
		}
		return items[i].UpdatedAt.After(items[j].UpdatedAt)
	})
	now := time.Now().UTC()
	for index := range items {
		if _, ok := order[watchStateAtomOrderKey(items[index])]; ok {
			items[index].UpdatedAt = now.Add(-time.Duration(index) * time.Second)
		}
	}
}

func watchStateAtomOrderKey(item watchStateItem) string {
	showSlug := firstNonEmpty(item.SourceUserID, slugForAtomOrder(item.ParentTitle))
	return traktAtomOrderKey(showSlug, item.Season, item.Episode)
}

func traktAtomOrderKey(showSlug string, season int, episode int) string {
	showSlug = strings.TrimSpace(strings.ToLower(showSlug))
	if showSlug == "" || season <= 0 || episode <= 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d:%d", showSlug, season, episode)
}

func slugForAtomOrder(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var builder strings.Builder
	lastDash := false
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			builder.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			builder.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(builder.String(), "-")
}

func (s *appState) traktGetJSON(ctx context.Context, path string, target any) error {
	s.mu.RLock()
	clientID := strings.TrimSpace(s.config.Watch.Trakt.ClientID)
	accessToken := strings.TrimSpace(s.config.Watch.Trakt.AccessToken)
	s.mu.RUnlock()
	if clientID == "" || accessToken == "" {
		return fmt.Errorf("Trakt client ID and access token are required")
	}
	u := "https://api.trakt.tv" + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("trakt-api-version", "2")
	req.Header.Set("trakt-api-key", clientID)
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Trakt %s failed: HTTP %d %s", path, resp.StatusCode, responseMessage(data))
	}
	return json.Unmarshal(data, target)
}

func (s *appState) traktPostJSON(ctx context.Context, path string, payload any, target any) error {
	s.mu.RLock()
	clientID := strings.TrimSpace(s.config.Watch.Trakt.ClientID)
	accessToken := strings.TrimSpace(s.config.Watch.Trakt.AccessToken)
	s.mu.RUnlock()
	if clientID == "" || accessToken == "" {
		return fmt.Errorf("Trakt client ID and access token are required")
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.trakt.tv"+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("trakt-api-version", "2")
	req.Header.Set("trakt-api-key", clientID)
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Trakt %s failed: HTTP %d %s", path, resp.StatusCode, responseMessage(data))
	}
	if target == nil {
		return nil
	}
	return json.Unmarshal(data, target)
}

func (s *appState) pushWatchStateMarkToTraktAndRefresh(item watchStateItem) {
	if !s.traktSyncConfigured() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := s.pushWatchStateMarkToTrakt(ctx, item); err != nil {
		log.Printf("Trakt watch-state push skipped: %v", err)
		return
	}
	s.syncTraktWatchStateIfConfigured(ctx, "app-mark")
}

func (s *appState) pushWatchStateMarkToTrakt(ctx context.Context, item watchStateItem) error {
	payload := traktHistoryPayloadForWatchStateItem(item)
	if payload == nil {
		return nil
	}
	path := "/sync/history/remove"
	if item.Watched {
		path = "/sync/history"
	}
	return s.traktPostJSON(ctx, path, payload, nil)
}

func traktHistoryPayloadForWatchStateItem(item watchStateItem) map[string]any {
	mediaType := strings.ToLower(strings.TrimSpace(item.MediaType))
	ids := traktIDsPayload(item)
	if len(ids) == 0 {
		return nil
	}
	if mediaType == "movie" {
		movie := map[string]any{"ids": ids}
		if item.Watched {
			movie["watched_at"] = time.Now().UTC().Format(time.RFC3339)
		}
		return map[string]any{"movies": []any{movie}}
	}
	if mediaType == "episode" && item.Season > 0 && item.Episode > 0 {
		episode := map[string]any{"number": item.Episode}
		if item.Watched {
			episode["watched_at"] = time.Now().UTC().Format(time.RFC3339)
		}
		show := map[string]any{
			"ids": ids,
			"seasons": []any{
				map[string]any{
					"number": item.Season,
					"episodes": []any{
						episode,
					},
				},
			},
		}
		return map[string]any{"shows": []any{show}}
	}
	return nil
}

func traktIDsPayload(item watchStateItem) map[string]any {
	ids := map[string]any{}
	if imdbID := strings.TrimSpace(item.IMDBID); imdbID != "" {
		ids["imdb"] = imdbID
	}
	if item.TMDBID > 0 {
		ids["tmdb"] = item.TMDBID
	}
	if item.TVDBID > 0 {
		ids["tvdb"] = item.TVDBID
	}
	if item.TraktID > 0 {
		ids["trakt"] = item.TraktID
	}
	return ids
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
	lookupIDs := s.vortexoStreamLookupIDs(r.Context(), req)
	if len(lookupIDs) == 0 {
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
		var lastErr error
		for _, lookupID := range lookupIDs {
			streams, err := s.fetchStreams(r.Context(), base, req, lookupID)
			if err != nil {
				lastErr = err
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
		if lastErr != nil && len(all) == 0 {
			log.Printf("streams %s failed for ids %v: %v", item.URL, lookupIDs, lastErr)
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

func (s *appState) vortexoStreamLookupIDs(ctx context.Context, req vortexoSourcesRequest) []string {
	imdbID := imdbFromID(req.IMDBID)
	if imdbID == "" && req.TMDBID > 0 {
		resolved, err := s.tmdbIMDbID(ctx, req.Type, req.TMDBID)
		if err != nil {
			log.Printf("TMDB imdb lookup failed for %s tmdb:%d: %v", req.Type, req.TMDBID, err)
		} else {
			imdbID = resolved
		}
	}
	return vortexoStreamLookupIDs(req, imdbID)
}

func vortexoStreamLookupIDs(req vortexoSourcesRequest, imdbID string) []string {
	seen := map[string]bool{}
	out := []string{}
	add := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" || seen[strings.ToLower(value)] {
			return
		}
		seen[strings.ToLower(value)] = true
		out = append(out, value)
	}
	add(imdbFromID(imdbID))
	if req.TMDBID > 0 {
		tmdbID := strconv.Itoa(req.TMDBID)
		add("tmdb:" + tmdbID)
		add(tmdbID)
	}
	return out
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

func (s *appState) handleTrailerPlay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	videoID := trailerVideoIDFromPath(r.URL.Path)
	if videoID == "" {
		respondError(w, http.StatusBadRequest, "invalid trailer request")
		return
	}

	playbackURL, container, err := s.resolveYouTubeTrailerURL(r.Context(), videoID)
	if err != nil {
		respondError(w, http.StatusBadGateway, "trailer playback unavailable: "+err.Error())
		return
	}

	if wantsJSON(r) {
		respondJSON(w, http.StatusOK, map[string]any{
			"url":        playbackURL,
			"stream_url": playbackURL,
			"direct_url": playbackURL,
			"container":  container,
		})
		return
	}

	http.Redirect(w, r, playbackURL, http.StatusFound)
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
	lookupIDs := s.vortexoStreamLookupIDs(r.Context(), req)
	if len(lookupIDs) == 0 {
		respondError(w, http.StatusNotFound, "missing media id")
		return
	}

	subtitle, base, err := s.findSubtitle(r.Context(), req, lookupIDs, language)
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

func (s *appState) fetchAddonCatalog(ctx context.Context, base string, catalog stremioCatalog, limit int) ([]addonCatalogEntry, error) {
	u := fmt.Sprintf("%s/addon_catalog/%s/%s.json", strings.TrimRight(base, "/"), url.PathEscape(catalog.Type), url.PathEscape(catalog.ID))
	var response addonCatalogResponse
	if err := s.getJSON(ctx, u, &response); err != nil {
		return nil, err
	}
	items := response.Addons
	if len(items) == 0 {
		items = response.Items
	}
	if len(items) == 0 {
		items = response.Metas
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

func (s *appState) findSubtitle(ctx context.Context, req vortexoSourcesRequest, lookupIDs []string, language string) (stremioSubtitle, string, error) {
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
		for _, lookupID := range lookupIDs {
			subtitles, err := s.fetchSubtitles(ctx, base, req, lookupID)
			if err != nil {
				lastErr = err
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
			log.Printf("subtitles %s failed for ids %v: %v", item.URL, lookupIDs, lastErr)
		}
	}
	if lastErr != nil {
		return stremioSubtitle{}, "", fmt.Errorf("subtitle not found: %w", lastErr)
	}
	return stremioSubtitle{}, "", fmt.Errorf("subtitle not found")
}

func (s *appState) fetchSubtitles(ctx context.Context, base string, req vortexoSourcesRequest, lookupID string) ([]stremioSubtitle, error) {
	var path string
	if req.Type == "episode" {
		if req.Season <= 0 || req.Episode <= 0 {
			return nil, fmt.Errorf("season and episode are required")
		}
		path = fmt.Sprintf("subtitles/series/%s:%d:%d.json", url.PathEscape(lookupID), req.Season, req.Episode)
	} else {
		path = fmt.Sprintf("subtitles/movie/%s.json", url.PathEscape(lookupID))
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
	if handled, err := s.getLocalStreamingCatalogJSON(ctx, rawURL, target); handled {
		return err
	}

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

func plexSettingsResponse(plex plexAuthConfig) map[string]any {
	return map[string]any{
		"client_id":         plex.ClientID,
		"has_access_token":  strings.TrimSpace(plex.AccessToken) != "",
		"user_id":           plex.UserID,
		"username":          plex.Username,
		"email":             plex.Email,
		"title":             plex.Title,
		"last_signed_in_at": plex.LastSignedInAt,
	}
}

func (s *appState) applyPlexUserLocked(user *plexUser) {
	if user == nil {
		return
	}
	s.config.Plex.UserID = user.ID
	s.config.Plex.Username = strings.TrimSpace(user.Username)
	s.config.Plex.Email = strings.TrimSpace(user.Email)
	s.config.Plex.Title = strings.TrimSpace(user.Title)
}

func (s *appState) plexClientID() string {
	s.mu.RLock()
	clientID := strings.TrimSpace(s.config.Plex.ClientID)
	s.mu.RUnlock()
	if clientID != "" {
		return clientID
	}

	clientID = randomToken()
	s.mu.Lock()
	if strings.TrimSpace(s.config.Plex.ClientID) == "" {
		s.config.Plex.ClientID = clientID
		_ = s.saveLocked()
	} else {
		clientID = s.config.Plex.ClientID
	}
	s.mu.Unlock()
	return clientID
}

func (s *appState) plexAccountToken() string {
	s.mu.RLock()
	token := strings.TrimSpace(s.config.Plex.AccessToken)
	s.mu.RUnlock()
	return token
}

func (s *appState) plexAuthHeaders(token string) map[string]string {
	headers := map[string]string{
		"Accept":                   "application/json",
		"User-Agent":               "VortexoManifestServer/1.0",
		"X-Plex-Product":           plexProduct,
		"X-Plex-Version":           plexVersion,
		"X-Plex-Platform":          plexPlatform,
		"X-Plex-Device":            plexDevice,
		"X-Plex-Device-Name":       plexDeviceName,
		"X-Plex-Client-Identifier": s.plexClientID(),
	}
	if strings.TrimSpace(token) != "" {
		headers["X-Plex-Token"] = strings.TrimSpace(token)
	}
	return headers
}

func (s *appState) plexDiscoverHeaders(token string) map[string]string {
	return map[string]string{
		"Accept":                          "application/json",
		"User-Agent":                      "VortexoManifestServer/1.0",
		"X-Plex-Product":                  "Plex HTPC",
		"X-Plex-Version":                  "5.92.0",
		"X-Plex-Platform":                 "macOS",
		"X-Plex-Platform-Version":         "15.5.0",
		"X-Plex-Model":                    "Mac",
		"X-Plex-Device":                   "Mac",
		"X-Plex-Device-Name":              "Mac",
		"X-Plex-Device-Vendor":            "Apple",
		"X-Plex-Device-Screen-Resolution": "1920x1080",
		"X-Plex-Drm":                      "widevine:video",
		"X-Plex-Language":                 "en",
		"X-Plex-Features":                 "external-media,indirect-media",
		"X-Plex-Client-Identifier":        s.plexClientID(),
		"X-Plex-Token":                    token,
	}
}

func (s *appState) plexDiscoverQueryItems(token string) []plexQueryItem {
	return []plexQueryItem{
		{Name: "X-Plex-Product", Value: "Plex HTPC"},
		{Name: "X-Plex-Version", Value: "5.92.0"},
		{Name: "X-Plex-Platform", Value: "macOS"},
		{Name: "X-Plex-Platform-Version", Value: "15.5.0"},
		{Name: "X-Plex-Model", Value: "Mac"},
		{Name: "X-Plex-Device", Value: "Mac"},
		{Name: "X-Plex-Device-Name", Value: "Mac"},
		{Name: "X-Plex-Device-Vendor", Value: "Apple"},
		{Name: "X-Plex-Device-Screen-Resolution", Value: "1920x1080"},
		{Name: "X-Plex-Drm", Value: "widevine:video"},
		{Name: "X-Plex-Language", Value: "en"},
		{Name: "X-Plex-Features", Value: "external-media,indirect-media"},
		{Name: "X-Plex-Client-Identifier", Value: s.plexClientID()},
		{Name: "X-Plex-Token", Value: token},
	}
}

func (s *appState) plexJSON(ctx context.Context, method string, rawURL string, body io.Reader, headers map[string]string, target any, okStatuses ...int) error {
	req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		return err
	}
	for key, value := range headers {
		if strings.TrimSpace(value) != "" {
			req.Header.Set(key, value)
		}
	}
	if body != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
	if err != nil {
		return err
	}
	ok := resp.StatusCode >= 200 && resp.StatusCode < 300
	if len(okStatuses) > 0 {
		ok = false
		for _, status := range okStatuses {
			if resp.StatusCode == status {
				ok = true
				break
			}
		}
	}
	if !ok {
		detail := responseMessage(data)
		if detail != "" {
			return fmt.Errorf("HTTP %d: %s", resp.StatusCode, detail)
		}
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	if target != nil && len(data) > 0 {
		if err := json.Unmarshal(data, target); err != nil {
			return err
		}
	}
	return nil
}

func (s *appState) createPlexPIN(ctx context.Context) (*plexPin, error) {
	var pin plexPin
	if err := s.plexJSON(ctx, http.MethodPost, "https://plex.tv/api/v2/pins?strong=true", nil, s.plexAuthHeaders(""), &pin, http.StatusOK, http.StatusCreated); err != nil {
		return nil, fmt.Errorf("Plex PIN request failed: %w", err)
	}
	if pin.ID <= 0 || strings.TrimSpace(pin.Code) == "" {
		return nil, fmt.Errorf("Plex PIN response was missing id/code")
	}
	return &pin, nil
}

func (s *appState) getPlexPIN(ctx context.Context, pinID int) (*plexPin, error) {
	var pin plexPin
	rawURL := fmt.Sprintf("https://plex.tv/api/v2/pins/%d", pinID)
	if err := s.plexJSON(ctx, http.MethodGet, rawURL, nil, s.plexAuthHeaders(""), &pin); err != nil {
		return nil, fmt.Errorf("Plex PIN check failed: %w", err)
	}
	return &pin, nil
}

func (s *appState) fetchPlexUser(ctx context.Context, token string) (*plexUser, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, fmt.Errorf("missing Plex token")
	}
	var user plexUser
	if err := s.plexJSON(ctx, http.MethodGet, "https://plex.tv/api/v2/user", nil, s.plexAuthHeaders(token), &user); err != nil {
		return nil, err
	}
	if user.ID == 0 && user.Username == "" && user.Email == "" && user.Title == "" {
		return nil, fmt.Errorf("Plex user response was empty")
	}
	return &user, nil
}

func plexAuthURL(clientID, code string) string {
	components := url.URL{
		Scheme: "https",
		Host:   "app.plex.tv",
		Path:   "/auth",
	}
	query := url.Values{}
	query.Set("clientID", clientID)
	query.Set("code", code)
	query.Set("context[device][product]", plexProduct)
	components.Fragment = "?" + query.Encode()
	return components.String()
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
	if err := json.Unmarshal(body, &decoded); err == nil {
		if message := findMessage(decoded); message != "" {
			return message
		}
	}
	var xmlMessage struct {
		Message string `xml:"message,attr"`
		Error   string `xml:"error,attr"`
		Text    string `xml:",chardata"`
	}
	if err := xml.Unmarshal(body, &xmlMessage); err == nil {
		if message := firstNonEmpty(xmlMessage.Message, xmlMessage.Error, strings.TrimSpace(xmlMessage.Text)); message != "" {
			return message
		}
	}
	message := strings.TrimSpace(string(bytes.TrimSpace(body)))
	if len(message) > 240 {
		return message[:240] + "..."
	}
	return message
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
		LandscapePath: "",
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

func (s *appState) runPlexArtworkSyncWorker(ctx context.Context) {
	log.Printf("[PlexArtwork] worker starting interval=%s initialDelay=%s", plexArtworkSyncInterval, plexArtworkInitialDelay)
	timer := time.NewTimer(plexArtworkInitialDelay)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[PlexArtwork] worker stopping")
			return
		case <-timer.C:
		}

		if s.plexArtworkSyncMu.TryLock() {
			s.setPlexArtworkSyncRunning(true)
			stats, err := s.syncPlexArtworkCache(ctx, plexArtworkSyncLimit, false)
			s.rememberPlexArtworkSyncStats(stats)
			s.setPlexArtworkSyncRunning(false)
			s.plexArtworkSyncMu.Unlock()
			if err != nil {
				log.Printf("[PlexArtwork] sync error: %v", err)
			} else {
				log.Printf("[PlexArtwork] sync complete attempted=%d ok=%d miss=%d skipped=%d failed=%d stopped=%q", stats.Attempted, stats.OK, stats.Miss, stats.Skipped, stats.Failed, stats.Stopped)
			}
		} else {
			log.Printf("[PlexArtwork] sync skipped because another sync is running")
		}

		timer.Reset(plexArtworkSyncInterval)
	}
}

func (s *appState) syncPlexArtworkCache(ctx context.Context, limit int, forceRepair bool) (*plexArtworkSyncStats, error) {
	if limit <= 0 {
		limit = plexArtworkSyncLimit
	}
	started := time.Now().UTC()
	stats := &plexArtworkSyncStats{
		Limit:     limit,
		StartedAt: started,
	}

	staleBefore := time.Now().Add(-plexArtworkStaleAfter)
	items := s.collectPlexArtworkSeedItems(ctx, limit, staleBefore, forceRepair)
	stats.Attempted = len(items)
	log.Printf("[PlexArtwork] starting sync selected=%d limit=%d delay=%s staleAfter=%s forceRepair=%t", len(items), limit, plexArtworkFetchDelay, plexArtworkStaleAfter, forceRepair)

	for index, item := range items {
		if err := ctx.Err(); err != nil {
			stats.CompletedAt = time.Now().UTC()
			return stats, err
		}

		idLabel := item.IMDBID
		if item.TMDBID > 0 {
			idLabel = "tmdb:" + strconv.Itoa(item.TMDBID)
		}
		label := fmt.Sprintf("%s:%s %s", normalizePlexArtworkMediaType(item.MediaType), idLabel, item.Title)
		if plexArtworkKey(item.MediaType, item.TMDBID, item.IMDBID, item.Title, item.Year) == "" || strings.TrimSpace(item.Title) == "" {
			stats.Skipped++
			log.Printf("[PlexArtwork] skip %d/%d invalid item mediaType=%q tmdb=%d imdb=%q title=%q", index+1, len(items), item.MediaType, item.TMDBID, item.IMDBID, item.Title)
			continue
		}

		entry, err := s.scrapePlexArtworkItem(ctx, item, plexArtworkFetchDelay)
		if err != nil {
			var stopErr *plexArtworkStopError
			if errors.As(err, &stopErr) {
				stats.Stopped = stopErr.Error()
				stats.CompletedAt = time.Now().UTC()
				log.Printf("[PlexArtwork] stop %d/%d %s: %v", index+1, len(items), label, err)
				return stats, nil
			}
			stats.Failed++
			_ = s.upsertPlexArtworkRecord(plexArtworkCacheRecord{
				plexArtworkEntry: plexArtworkEntry{
					Version:   1,
					MediaType: item.MediaType,
					TMDBID:    item.TMDBID,
					IMDBID:    item.IMDBID,
					Title:     item.Title,
					Year:      item.Year,
					UpdatedAt: time.Now().UTC(),
					Artwork:   plexArtwork{},
				},
				Status: "error",
				Error:  err.Error(),
			})
			log.Printf("[PlexArtwork] error %d/%d %s: %v", index+1, len(items), label, err)
			continue
		}

		if entry == nil || entry.Artwork.isEmpty() {
			stats.Miss++
			_ = s.upsertPlexArtworkRecord(plexArtworkCacheRecord{
				plexArtworkEntry: plexArtworkEntry{
					Version:   1,
					MediaType: item.MediaType,
					TMDBID:    item.TMDBID,
					IMDBID:    item.IMDBID,
					Title:     item.Title,
					Year:      item.Year,
					UpdatedAt: time.Now().UTC(),
					Artwork:   plexArtwork{},
				},
				Status: "miss",
				Error:  "no public Plex artwork found",
			})
			log.Printf("[PlexArtwork] miss %d/%d %s", index+1, len(items), label)
			continue
		}

		if err := s.upsertPlexArtworkRecord(plexArtworkCacheRecord{
			plexArtworkEntry: *entry,
			Status:           "ok",
		}); err != nil {
			stats.Failed++
			log.Printf("[PlexArtwork] cache error %d/%d %s: %v", index+1, len(items), label, err)
			continue
		}
		stats.OK++
		log.Printf("[PlexArtwork] ok %d/%d %s source=%s artwork=%s", index+1, len(items), label, entry.SourcePage, plexArtworkSummary(entry.Artwork))
	}

	stats.CompletedAt = time.Now().UTC()
	return stats, nil
}

func (s *appState) collectPlexArtworkSeedItems(ctx context.Context, limit int, staleBefore time.Time, forceRepair bool) []plexArtworkSeedItem {
	if limit <= 0 {
		limit = plexArtworkSyncLimit
	}

	now := time.Now()
	seen := map[string]bool{}
	items := make([]plexArtworkSeedItem, 0, limit)
	add := func(seed plexArtworkSeedItem) {
		if len(items) >= limit {
			return
		}
		seed.MediaType = normalizePlexArtworkMediaType(seed.MediaType)
		seed.Title = strings.TrimSpace(seed.Title)
		seed.IMDBID = imdbFromID(seed.IMDBID)
		if seed.Title == "" {
			return
		}
		key := plexArtworkKey(seed.MediaType, seed.TMDBID, seed.IMDBID, seed.Title, seed.Year)
		if key == "" || seen[key] || !s.plexArtworkSeedNeedsRefresh(seed, staleBefore, forceRepair, now) {
			return
		}
		seen[key] = true
		items = append(items, seed)
	}

	s.mu.RLock()
	watchItems := append([]watchStateItem(nil), s.watchState.Items...)
	s.mu.RUnlock()
	sort.SliceStable(watchItems, func(i, j int) bool {
		return watchItems[i].UpdatedAt.After(watchItems[j].UpdatedAt)
	})
	for _, item := range watchItems {
		if seed, ok := plexArtworkSeedFromWatchState(item); ok {
			add(seed)
		}
	}

	for _, entry := range s.enabledCatalogEntries(ctx) {
		if len(items) >= limit {
			break
		}
		if !manifestSupportsResource(entry.Manifest, "catalog") {
			continue
		}
		mediaType := normalizeCatalogType(entry.Catalog.Type)
		if mediaType == "" {
			continue
		}
		metas, err := s.fetchCatalog(ctx, entry.Base, entry.Catalog, plexArtworkSeedCatalogLimit)
		if err != nil {
			log.Printf("[PlexArtwork] seed catalog %s/%s failed: %v", entry.Catalog.Type, entry.Catalog.ID, err)
			continue
		}
		for _, meta := range metas {
			homeItem := homeItemFromStremio(meta, mediaType)
			if seed, ok := plexArtworkSeedFromHomeItem(homeItem); ok {
				add(seed)
			}
			if len(items) >= limit {
				break
			}
		}
	}

	return items
}

func (s *appState) plexArtworkSeedNeedsRefresh(seed plexArtworkSeedItem, staleBefore time.Time, forceRepair bool, now time.Time) bool {
	record, ok := s.getCachedPlexArtwork(seed.MediaType, seed.TMDBID, seed.IMDBID, seed.Title, seed.Year)
	if !ok {
		return true
	}
	if record.Status != "ok" {
		return forceRepair || record.FetchedAt.IsZero() || record.FetchedAt.Before(now.Add(-24*time.Hour))
	}
	if forceRepair && !plexArtworkHasCleanLandscape(record.Artwork) {
		return true
	}
	if s.plexAccountToken() != "" &&
		!plexArtworkHasCleanLandscape(record.Artwork) &&
		!isSignedPlexArtworkSource(record.SourcePage) {
		if signedInAt := s.plexLastSignedInAt(); !signedInAt.IsZero() && record.FetchedAt.Before(signedInAt) {
			return true
		}
		return record.FetchedAt.IsZero() || record.FetchedAt.Before(now.Add(-plexArtworkIncompleteRetryAfter))
	}
	return record.FetchedAt.IsZero() || record.FetchedAt.Before(staleBefore)
}

func (s *appState) plexLastSignedInAt() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config.Plex.LastSignedInAt
}

func plexArtworkHasCleanLandscape(artwork plexArtwork) bool {
	return len(artwork.Landscape) > 0 || len(artwork.CoverArt) > 0
}

func (s *appState) findPlexArtworkSeedItem(ctx context.Context, mediaType string, tmdbID int) plexArtworkSeedItem {
	normalizedType := normalizePlexArtworkMediaType(mediaType)
	s.mu.RLock()
	watchItems := append([]watchStateItem(nil), s.watchState.Items...)
	s.mu.RUnlock()
	for _, item := range watchItems {
		seed, ok := plexArtworkSeedFromWatchState(item)
		if ok && seed.TMDBID == tmdbID && normalizePlexArtworkMediaType(seed.MediaType) == normalizedType {
			return seed
		}
	}

	for _, entry := range s.enabledCatalogEntries(ctx) {
		if !manifestSupportsResource(entry.Manifest, "catalog") {
			continue
		}
		catalogType := normalizeCatalogType(entry.Catalog.Type)
		if normalizePlexArtworkMediaType(catalogType) != normalizedType {
			continue
		}
		metas, err := s.fetchCatalog(ctx, entry.Base, entry.Catalog, plexArtworkSeedCatalogLimit)
		if err != nil {
			continue
		}
		for _, meta := range metas {
			homeItem := homeItemFromStremio(meta, catalogType)
			seed, ok := plexArtworkSeedFromHomeItem(homeItem)
			if ok && seed.TMDBID == tmdbID {
				return seed
			}
		}
	}

	return plexArtworkSeedItem{MediaType: normalizedType, TMDBID: tmdbID}
}

func (s *appState) refreshPlexArtworkSeed(ctx context.Context, item plexArtworkSeedItem) (*plexArtworkCacheRecord, error) {
	entry, err := s.scrapePlexArtworkItem(ctx, item, plexArtworkFetchDelay)
	if err != nil {
		var stopErr *plexArtworkStopError
		if !errors.As(err, &stopErr) {
			return nil, err
		}
	}

	status := "ok"
	errorMessage := ""
	if entry == nil || entry.Artwork.isEmpty() {
		status = "miss"
		errorMessage = "no Plex artwork found"
		if err != nil {
			errorMessage = err.Error()
		}
		entry = &plexArtworkEntry{
			Version:   1,
			MediaType: item.MediaType,
			TMDBID:    item.TMDBID,
			IMDBID:    item.IMDBID,
			Title:     item.Title,
			Year:      item.Year,
			UpdatedAt: time.Now().UTC(),
			Artwork:   plexArtwork{},
		}
	}

	record := plexArtworkCacheRecord{
		plexArtworkEntry: *entry,
		Status:           status,
		Error:            errorMessage,
	}
	if err := s.upsertPlexArtworkRecord(record); err != nil {
		return nil, err
	}
	saved, _ := s.getCachedPlexArtwork(record.MediaType, record.TMDBID, record.IMDBID, record.Title, record.Year)
	return &saved, nil
}

func (s *appState) scrapePlexArtworkItem(ctx context.Context, item plexArtworkSeedItem, delay time.Duration) (*plexArtworkEntry, error) {
	if signedEntry, err := s.fetchSignedPlexDiscoverArtworkItem(ctx, item); err != nil {
		log.Printf("[PlexArtwork] signed Discover miss %s:%d %s: %v", item.MediaType, item.TMDBID, item.Title, err)
	} else if signedEntry != nil && !signedEntry.Artwork.isEmpty() {
		return signedEntry, nil
	}

	for _, pageURL := range candidatePlexArtworkURLs(item) {
		body, err := s.fetchPlexArtworkPage(ctx, pageURL, delay)
		if err != nil {
			var stopErr *plexArtworkStopError
			if errors.As(err, &stopErr) {
				return nil, err
			}
			log.Printf("[PlexArtwork] fetch miss %s:%d %s %v", item.MediaType, item.TMDBID, pageURL, err)
			continue
		}

		artwork := structuredPlexArtwork(body)
		if artwork.isEmpty() {
			continue
		}

		return &plexArtworkEntry{
			Version:    1,
			MediaType:  normalizePlexArtworkMediaType(item.MediaType),
			TMDBID:     item.TMDBID,
			IMDBID:     item.IMDBID,
			Title:      item.Title,
			Year:       item.Year,
			SourcePage: pageURL,
			UpdatedAt:  time.Now().UTC(),
			Artwork:    artwork,
		}, nil
	}
	return nil, nil
}

func (s *appState) fetchSignedPlexDiscoverArtworkItem(ctx context.Context, item plexArtworkSeedItem) (*plexArtworkEntry, error) {
	token := s.plexAccountToken()
	if token == "" {
		return nil, nil
	}
	query := strings.TrimSpace(item.Title)
	if query == "" {
		return nil, nil
	}

	results, err := s.searchSignedPlexDiscoverMetadata(ctx, token, query, 12)
	if err != nil {
		return nil, err
	}
	match := bestPlexDiscoverSearchMatch(results, item)
	if match == nil {
		return nil, nil
	}

	metadata := *match
	sourcePage := "plex-discover:search"
	if discoverID := plexDiscoverMetadataID(metadata); discoverID != "" {
		sourcePage = "https://discover.provider.plex.tv/library/metadata/" + discoverID
		if hydrated, err := s.getSignedPlexDiscoverMetadata(ctx, token, discoverID); err == nil && hydrated != nil {
			metadata = *hydrated
		} else if err != nil {
			log.Printf("[PlexArtwork] signed Discover metadata hydrate miss %s %s: %v", discoverID, item.Title, err)
		}
	}

	artwork := plexArtworkFromDiscoverMetadata(metadata, token)
	if artwork.isEmpty() {
		return nil, nil
	}

	discoveredTMDB, discoveredIMDB := plexDiscoverExternalIDs(metadata, normalizePlexArtworkMediaType(item.MediaType))
	return &plexArtworkEntry{
		Version:    1,
		MediaType:  normalizePlexArtworkMediaType(item.MediaType),
		TMDBID:     firstNonZero(item.TMDBID, discoveredTMDB),
		IMDBID:     firstNonEmpty(item.IMDBID, discoveredIMDB),
		Title:      firstNonEmpty(item.Title, metadata.Title, metadata.OriginalTitle),
		Year:       firstNonZero(item.Year, metadata.Year, yearFromText(metadata.OriginallyAvailableAt)),
		SourcePage: sourcePage,
		UpdatedAt:  time.Now().UTC(),
		Artwork:    artwork,
	}, nil
}

func (s *appState) searchSignedPlexDiscoverMetadata(ctx context.Context, token string, query string, limit int) ([]plexDiscoverMetadata, error) {
	components, err := url.Parse("https://discover.provider.plex.tv/library/search")
	if err != nil {
		return nil, err
	}
	items := s.plexDiscoverQueryItems(token)
	items = append(items,
		plexQueryItem{Name: "searchTypes", Value: "movies,tv"},
		plexQueryItem{Name: "searchProviders", Value: "discover"},
		plexQueryItem{Name: "includeMetadata", Value: "1"},
		plexQueryItem{Name: "includeGuids", Value: "1"},
		plexQueryItem{Name: "includeImages", Value: "1"},
		plexQueryItem{Name: "filterPeople", Value: "1"},
		plexQueryItem{Name: "limit", Value: strconv.Itoa(maxInt(1, minInt(limit, 50)))},
		plexQueryItem{Name: "query", Value: query},
	)
	values := url.Values{}
	for _, item := range items {
		values.Add(item.Name, item.Value)
	}
	components.RawQuery = values.Encode()

	var response plexDiscoverSearchResponse
	if err := s.plexJSON(ctx, http.MethodGet, components.String(), nil, s.plexDiscoverHeaders(token), &response); err != nil {
		return nil, err
	}

	var results []plexDiscoverMetadata
	seen := map[string]bool{}
	for _, group := range response.MediaContainer.SearchResults {
		for _, result := range group.SearchResult {
			if result.Metadata == nil {
				continue
			}
			meta := *result.Metadata
			key := firstNonEmpty(plexDiscoverMetadataID(meta), meta.RatingKey, meta.Key, meta.GUID, meta.Title)
			if key != "" {
				key = strings.ToLower(key)
				if seen[key] {
					continue
				}
				seen[key] = true
			}
			results = append(results, meta)
		}
	}
	return results, nil
}

func (s *appState) getSignedPlexDiscoverMetadata(ctx context.Context, token string, discoverID string) (*plexDiscoverMetadata, error) {
	discoverID = strings.Trim(discoverID, "/")
	if discoverID == "" {
		return nil, fmt.Errorf("missing Discover metadata id")
	}

	components, err := url.Parse("https://discover.provider.plex.tv/library/metadata/" + url.PathEscape(discoverID))
	if err != nil {
		return nil, err
	}
	items := s.plexDiscoverQueryItems(token)
	items = append(items,
		plexQueryItem{Name: "includeMeta", Value: "1"},
		plexQueryItem{Name: "includeExternalMetadata", Value: "1"},
		plexQueryItem{Name: "includeGuids", Value: "1"},
		plexQueryItem{Name: "includeImages", Value: "1"},
		plexQueryItem{Name: "includeExternalMedia", Value: "1"},
	)
	values := url.Values{}
	for _, item := range items {
		values.Add(item.Name, item.Value)
	}
	components.RawQuery = values.Encode()

	var response plexDiscoverMetadataResponse
	if err := s.plexJSON(ctx, http.MethodGet, components.String(), nil, s.plexDiscoverHeaders(token), &response); err != nil {
		return nil, err
	}
	if len(response.MediaContainer.Metadata) == 0 {
		return nil, nil
	}
	return &response.MediaContainer.Metadata[0], nil
}

func (s *appState) getSignedPlexDiscoverMetadataList(ctx context.Context, token string, rawPath string, limit int) ([]plexDiscoverMetadata, error) {
	rawPath = "/" + strings.TrimLeft(strings.TrimSpace(rawPath), "/")
	if rawPath == "/" {
		return nil, fmt.Errorf("missing Discover metadata path")
	}
	if limit <= 0 {
		limit = 5000
	}

	pageSize := minInt(limit, 100)
	itemsOut := make([]plexDiscoverMetadata, 0, minInt(limit, 256))
	for offset := 0; len(itemsOut) < limit; offset += pageSize {
		components, err := url.Parse("https://discover.provider.plex.tv" + rawPath)
		if err != nil {
			return nil, err
		}
		items := s.plexDiscoverQueryItems(token)
		items = append(items,
			plexQueryItem{Name: "includeMeta", Value: "1"},
			plexQueryItem{Name: "includeExternalMetadata", Value: "1"},
			plexQueryItem{Name: "includeGuids", Value: "1"},
			plexQueryItem{Name: "includeImages", Value: "1"},
			plexQueryItem{Name: "includeExternalMedia", Value: "1"},
			plexQueryItem{Name: "X-Plex-Container-Start", Value: strconv.Itoa(offset)},
			plexQueryItem{Name: "X-Plex-Container-Size", Value: strconv.Itoa(pageSize)},
		)
		values := url.Values{}
		for _, item := range items {
			values.Add(item.Name, item.Value)
		}
		components.RawQuery = values.Encode()

		var response plexDiscoverMetadataResponse
		if err := s.plexJSON(ctx, http.MethodGet, components.String(), nil, s.plexDiscoverHeaders(token), &response); err != nil {
			return nil, err
		}

		pageItems := make([]plexDiscoverMetadata, 0, len(response.MediaContainer.Directory)+len(response.MediaContainer.Metadata))
		pageItems = append(pageItems, response.MediaContainer.Directory...)
		pageItems = append(pageItems, response.MediaContainer.Metadata...)
		if len(pageItems) == 0 {
			break
		}
		remaining := limit - len(itemsOut)
		if len(pageItems) > remaining {
			pageItems = pageItems[:remaining]
		}
		itemsOut = append(itemsOut, pageItems...)

		totalSize := response.MediaContainer.TotalSize
		if totalSize > 0 && len(itemsOut) >= totalSize {
			break
		}
		if len(pageItems) < pageSize {
			break
		}
	}
	return itemsOut, nil
}

func (s *appState) fetchPlexArtworkPage(ctx context.Context, pageURL string, delay time.Duration) (string, error) {
	if err := s.waitForPlexArtworkSlot(ctx, delay); err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("User-Agent", "Mozilla/5.0 VortexoArtworkCache")

	resp, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := strings.TrimSpace(resp.Header.Get("Retry-After"))
		if retryAfter != "" {
			return "", &plexArtworkStopError{message: fmt.Sprintf("Plex returned HTTP %d retryAfter=%s url=%s", resp.StatusCode, retryAfter, pageURL)}
		}
		return "", &plexArtworkStopError{message: fmt.Sprintf("Plex returned HTTP %d url=%s", resp.StatusCode, pageURL)}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("HTTP %d %s", resp.StatusCode, pageURL)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	body := string(data)
	if isPlexCloudflareChallenge(body) {
		return "", &plexArtworkStopError{message: fmt.Sprintf("Cloudflare challenge detected url=%s", pageURL)}
	}
	return body, nil
}

func (s *appState) waitForPlexArtworkSlot(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	s.plexArtworkRequestMu.Lock()
	wait := time.Duration(0)
	if !s.plexArtworkLastRequestAt.IsZero() {
		elapsed := time.Since(s.plexArtworkLastRequestAt)
		if elapsed < delay {
			wait = delay - elapsed
		}
	}
	s.plexArtworkRequestMu.Unlock()

	if wait > 0 {
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}

	s.plexArtworkRequestMu.Lock()
	s.plexArtworkLastRequestAt = time.Now()
	s.plexArtworkRequestMu.Unlock()
	return nil
}

func (s *appState) applyCachedPlexArtworkToHomeItem(item *vortexoHomeItem) {
	if item == nil {
		return
	}
	seed, ok := plexArtworkSeedFromHomeItem(*item)
	if !ok {
		return
	}
	record, ok := s.getCachedPlexArtwork(seed.MediaType, seed.TMDBID, seed.IMDBID, seed.Title, seed.Year)
	if !ok || record.Status != "ok" || record.Artwork.isEmpty() {
		return
	}
	if landscape := firstPlexArtworkURL(record.Artwork.Landscape, record.Artwork.CoverArt); landscape != "" {
		item.LandscapePath = landscape
	}
	if background := firstPlexArtworkURL(record.Artwork.Background); background != "" {
		item.BackdropPath = firstNonEmpty(item.BackdropPath, background)
	}
	if logo := firstPlexArtworkURL(record.Artwork.ClearLogo); logo != "" {
		item.LogoPath = firstNonEmpty(item.LogoPath, logo)
	}
	if item.PosterPath == "" {
		item.PosterPath = firstPlexArtworkURL(record.Artwork.Thumbnail)
	}
}

func (s *appState) applyCachedPlexArtworkToWatchStateItem(item *watchStateItem) {
	if item == nil {
		return
	}
	seed, ok := plexArtworkSeedFromWatchState(*item)
	if !ok {
		return
	}
	record, ok := s.getCachedPlexArtwork(seed.MediaType, seed.TMDBID, seed.IMDBID, seed.Title, seed.Year)
	if !ok || record.Status != "ok" || record.Artwork.isEmpty() {
		return
	}
	if landscape := firstPlexArtworkURL(record.Artwork.Landscape, record.Artwork.CoverArt); landscape != "" {
		item.LandscapePath = landscape
	}
	if background := firstPlexArtworkURL(record.Artwork.Background); background != "" {
		item.BackdropPath = background
	}
	if logo := firstPlexArtworkURL(record.Artwork.ClearLogo); logo != "" {
		item.LogoPath = logo
	}
	if item.PosterPath == "" {
		item.PosterPath = firstPlexArtworkURL(record.Artwork.Thumbnail)
	}
}

func (s *appState) getCachedPlexArtwork(mediaType string, tmdbID int, imdbID string, title string, year int) (plexArtworkCacheRecord, bool) {
	normalizedType := normalizePlexArtworkMediaType(mediaType)
	normalizedIMDB := imdbFromID(imdbID)
	normalizedTitle := slugifyPlexArtworkTitle(title)
	keys := uniqueNonEmptyStrings([]string{
		plexArtworkKey(normalizedType, tmdbID, "", "", 0),
		plexArtworkKey(normalizedType, 0, normalizedIMDB, "", 0),
		plexArtworkKey(normalizedType, 0, "", title, year),
	})
	s.plexArtworkMu.RLock()
	defer s.plexArtworkMu.RUnlock()
	for _, key := range keys {
		if record, ok := s.plexArtwork[key]; ok {
			return record, true
		}
	}

	for _, record := range s.plexArtwork {
		if normalizePlexArtworkMediaType(record.MediaType) != normalizedType {
			continue
		}
		if tmdbID > 0 && record.TMDBID == tmdbID {
			return record, true
		}
		if normalizedIMDB != "" && imdbFromID(record.IMDBID) == normalizedIMDB {
			return record, true
		}
		if normalizedTitle != "" &&
			slugifyPlexArtworkTitle(record.Title) == normalizedTitle &&
			(year <= 0 || record.Year <= 0 || record.Year == year) {
			return record, true
		}
	}
	return plexArtworkCacheRecord{}, false
}

func (s *appState) upsertPlexArtworkRecord(record plexArtworkCacheRecord) error {
	record.MediaType = normalizePlexArtworkMediaType(record.MediaType)
	record.IMDBID = imdbFromID(record.IMDBID)
	if record.Version == 0 {
		record.Version = 1
	}
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = time.Now().UTC()
	}
	if record.FetchedAt.IsZero() {
		record.FetchedAt = time.Now().UTC()
	}
	if record.Status == "" {
		record.Status = "ok"
	}
	record.Artwork = dedupePlexArtwork(record.Artwork)

	key := plexArtworkKey(record.MediaType, record.TMDBID, record.IMDBID, record.Title, record.Year)
	if key == "" {
		return fmt.Errorf("invalid plex artwork cache key")
	}

	s.plexArtworkMu.Lock()
	if s.plexArtwork == nil {
		s.plexArtwork = map[string]plexArtworkCacheRecord{}
	}
	s.plexArtwork[key] = record
	snapshot := make([]plexArtworkCacheRecord, 0, len(s.plexArtwork))
	for _, item := range s.plexArtwork {
		snapshot = append(snapshot, item)
	}
	s.plexArtworkMu.Unlock()

	return s.savePlexArtworkCacheSnapshot(snapshot)
}

func plexArtworkSeedFromHomeItem(item vortexoHomeItem) (plexArtworkSeedItem, bool) {
	seed := plexArtworkSeedItem{
		MediaType: item.MediaType,
		TMDBID:    item.TMDBID,
		IMDBID:    item.IMDBID,
		Title:     item.Title,
		Year:      item.Year,
	}
	return seed, plexArtworkKey(seed.MediaType, seed.TMDBID, seed.IMDBID, seed.Title, seed.Year) != ""
}

func plexArtworkSeedFromWatchState(item watchStateItem) (plexArtworkSeedItem, bool) {
	mediaType := strings.ToLower(strings.TrimSpace(item.MediaType))
	switch mediaType {
	case "movie":
		seed := plexArtworkSeedItem{
			MediaType: "movie",
			TMDBID:    item.TMDBID,
			IMDBID:    item.IMDBID,
			Title:     item.Title,
			Year:      item.Year,
		}
		return seed, plexArtworkKey(seed.MediaType, seed.TMDBID, seed.IMDBID, seed.Title, seed.Year) != ""
	case "episode":
		seed := plexArtworkSeedItem{
			MediaType: "tv",
			TMDBID:    item.TMDBID,
			IMDBID:    item.IMDBID,
			Title:     firstNonEmpty(item.ParentTitle, item.Title),
			Year:      item.Year,
		}
		return seed, plexArtworkKey(seed.MediaType, seed.TMDBID, seed.IMDBID, seed.Title, seed.Year) != ""
	default:
		return plexArtworkSeedItem{}, false
	}
}

func candidatePlexArtworkURLs(item plexArtworkSeedItem) []string {
	kind := "movie"
	if normalizePlexArtworkMediaType(item.MediaType) == "tv" {
		kind = "show"
	}

	slug := slugifyPlexArtworkTitle(item.Title)
	var paths []string
	if slug != "" {
		if item.Year > 0 {
			paths = append(paths, fmt.Sprintf("/en-GB/%s/%s-%d", kind, slug, item.Year))
		}
		paths = append(paths, fmt.Sprintf("/en-GB/%s/%s", kind, slug))
	}

	seen := map[string]bool{}
	urls := make([]string, 0, len(paths))
	for _, path := range paths {
		path = strings.ReplaceAll(path, `\u002F`, "/")
		path = strings.ReplaceAll(path, `\/`, "/")
		path = strings.ReplaceAll(path, "&amp;", "&")
		if !strings.HasPrefix(path, "/") || seen[path] {
			continue
		}
		seen[path] = true
		urls = append(urls, "https://watch.plex.tv"+path)
	}
	return urls
}

func bestPlexDiscoverSearchMatch(results []plexDiscoverMetadata, item plexArtworkSeedItem) *plexDiscoverMetadata {
	preferredType := plexDiscoverPreferredType(item.MediaType)
	preferredTitle := normalizedSearchText(item.Title)
	preferredIMDB := imdbFromID(item.IMDBID)

	type scoredMatch struct {
		metadata plexDiscoverMetadata
		score    int
	}
	var matches []scoredMatch
	for _, candidate := range results {
		if plexDiscoverPreferredType(candidate.Type) != preferredType {
			continue
		}

		candidateTMDB, candidateIMDB := plexDiscoverExternalIDs(candidate, normalizePlexArtworkMediaType(item.MediaType))
		candidateTitle := normalizedSearchText(firstNonEmpty(candidate.Title, candidate.OriginalTitle))
		exactTitle := preferredTitle != "" && candidateTitle != "" && candidateTitle == preferredTitle
		partialTitle := preferredTitle != "" && candidateTitle != "" &&
			(strings.Contains(candidateTitle, preferredTitle) || strings.Contains(preferredTitle, candidateTitle))
		candidateYear := firstNonZero(candidate.Year, yearFromText(candidate.OriginallyAvailableAt))
		yearMatches := item.Year == 0 || candidateYear == 0 || candidateYear == item.Year

		score := 0
		if item.TMDBID > 0 && candidateTMDB == item.TMDBID {
			score += 1000
		}
		if preferredIMDB != "" && strings.EqualFold(candidateIMDB, preferredIMDB) {
			score += 950
		}
		if exactTitle {
			score += 250
		} else if partialTitle {
			score += 100
		}
		if item.Year > 0 && candidate.Year == item.Year {
			score += 50
		} else if item.Year > 0 && yearFromText(candidate.OriginallyAvailableAt) == item.Year {
			score += 50
		}

		if item.TMDBID > 0 && candidateTMDB > 0 && candidateTMDB != item.TMDBID {
			continue
		}
		if preferredIMDB != "" && candidateIMDB != "" && !strings.EqualFold(candidateIMDB, preferredIMDB) {
			continue
		}
		if item.TMDBID > 0 || preferredIMDB != "" {
			hasExternalMatch := (item.TMDBID > 0 && candidateTMDB == item.TMDBID) ||
				(preferredIMDB != "" && strings.EqualFold(candidateIMDB, preferredIMDB))
			if !hasExternalMatch && !(exactTitle && yearMatches) && !(partialTitle && item.Year > 0 && candidateYear == item.Year) {
				continue
			}
		}
		if score <= 0 || (!exactTitle && !partialTitle && item.TMDBID == 0 && preferredIMDB == "") {
			continue
		}
		matches = append(matches, scoredMatch{metadata: candidate, score: score})
	}

	sort.SliceStable(matches, func(i, j int) bool {
		if matches[i].score == matches[j].score {
			return strings.ToLower(matches[i].metadata.Title) < strings.ToLower(matches[j].metadata.Title)
		}
		return matches[i].score > matches[j].score
	})
	if len(matches) == 0 {
		return nil
	}
	return &matches[0].metadata
}

func plexDiscoverPreferredType(mediaType string) string {
	switch strings.ToLower(strings.TrimSpace(mediaType)) {
	case "show", "series", "tv", "episode":
		return "show"
	default:
		return "movie"
	}
}

func plexDiscoverMetadataID(meta plexDiscoverMetadata) string {
	for _, value := range []string{meta.RatingKey, meta.Key, meta.GUID, meta.PrimaryGUID} {
		if id := normalizedPlexDiscoverMetadataID(value); id != "" {
			return id
		}
	}
	for _, guid := range meta.Guid {
		if id := normalizedPlexDiscoverMetadataID(guid.ID); id != "" {
			return id
		}
	}
	for _, guid := range meta.Guids {
		if id := normalizedPlexDiscoverMetadataID(guid.ID); id != "" {
			return id
		}
	}
	return ""
}

func normalizedPlexDiscoverMetadataID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = strings.ReplaceAll(value, `\u002F`, "/")
	value = strings.ReplaceAll(value, `\/`, "/")
	value = html.UnescapeString(value)

	if strings.HasPrefix(strings.ToLower(value), "plex://") {
		parts := strings.Split(strings.Trim(value, "/"), "/")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[len(parts)-1])
		}
	}

	if parsed, err := url.Parse(value); err == nil && parsed.Host != "" {
		parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
		for i, part := range parts {
			if part == "metadata" && i+1 < len(parts) {
				return strings.TrimSpace(parts[i+1])
			}
		}
	}

	if idx := strings.Index(value, "/library/metadata/"); idx >= 0 {
		rest := strings.Trim(value[idx+len("/library/metadata/"):], "/")
		if rest != "" {
			return strings.Split(rest, "/")[0]
		}
	}

	if strings.Contains(value, "://") {
		return ""
	}
	return strings.Trim(value, "/")
}

func plexDiscoverExternalIDs(meta plexDiscoverMetadata, preferredType string) (int, string) {
	var values []string
	values = append(values, meta.GUID, meta.PrimaryGUID, meta.RatingKey, meta.Key)
	for _, guid := range meta.Guid {
		values = append(values, guid.ID)
	}
	for _, guid := range meta.Guids {
		values = append(values, guid.ID)
	}

	var tmdbID int
	var imdbID string
	for _, value := range values {
		if imdbID == "" {
			imdbID = imdbFromID(value)
		}
		if tmdbID == 0 {
			tmdbID = tmdbIDFromDiscoverValue(value, preferredType)
		}
	}
	return tmdbID, imdbID
}

func plexDiscoverMetadataHasPublicIDs(meta plexDiscoverMetadata) bool {
	rawType := strings.ToLower(strings.TrimSpace(meta.Type))
	switch rawType {
	case "movie":
		tmdbID, _ := plexDiscoverExternalIDs(meta, "movie")
		return tmdbID > 0
	case "show", "tv", "series":
		tmdbID, _ := plexDiscoverExternalIDs(meta, "tv")
		return tmdbID > 0
	default:
		return false
	}
}

func mergePlexDiscoverMetadata(base plexDiscoverMetadata, override plexDiscoverMetadata) plexDiscoverMetadata {
	out := base
	if override.RatingKey != "" {
		out.RatingKey = override.RatingKey
	}
	if override.Key != "" {
		out.Key = override.Key
	}
	if override.GUID != "" {
		out.GUID = override.GUID
	}
	if override.PrimaryGUID != "" {
		out.PrimaryGUID = override.PrimaryGUID
	}
	if override.Type != "" {
		out.Type = override.Type
	}
	if override.Title != "" {
		out.Title = override.Title
	}
	if override.OriginalTitle != "" {
		out.OriginalTitle = override.OriginalTitle
	}
	if override.Summary != "" {
		out.Summary = override.Summary
	}
	if override.Year > 0 {
		out.Year = override.Year
	}
	if override.Index > 0 {
		out.Index = override.Index
	}
	if override.ParentIndex > 0 {
		out.ParentIndex = override.ParentIndex
	}
	if override.LeafCount > 0 {
		out.LeafCount = override.LeafCount
	}
	if override.ChildCount > 0 {
		out.ChildCount = override.ChildCount
	}
	if override.Duration > 0 {
		out.Duration = override.Duration
	}
	if override.Rating > 0 {
		out.Rating = override.Rating
	}
	if override.AudienceRating > 0 {
		out.AudienceRating = override.AudienceRating
	}
	if override.OriginallyAvailableAt != "" {
		out.OriginallyAvailableAt = override.OriginallyAvailableAt
	}
	if override.PublicPagesURL != "" {
		out.PublicPagesURL = override.PublicPagesURL
	}
	if override.Slug != "" {
		out.Slug = override.Slug
	}
	if override.Thumb != "" {
		out.Thumb = override.Thumb
	}
	if override.Art != "" {
		out.Art = override.Art
	}
	if override.Banner != "" {
		out.Banner = override.Banner
	}
	if len(override.Image) > 0 {
		out.Image = override.Image
	}
	if !plexDiscoverImageSetIsZero(override.Images) {
		out.Images = override.Images
	}
	if len(override.Guid) > 0 {
		out.Guid = override.Guid
	}
	if len(override.Guids) > 0 {
		out.Guids = override.Guids
	}
	return out
}

func plexDiscoverImageSetIsZero(images plexDiscoverImageSet) bool {
	return images.CoverArt == "" &&
		images.Landscape == "" &&
		images.Wide == "" &&
		images.Tile == "" &&
		images.Snapshot == "" &&
		images.Background == "" &&
		images.BackgroundLandscape == "" &&
		images.Art == "" &&
		images.Hero == "" &&
		images.ClearLogo == "" &&
		images.Logo == "" &&
		images.Thumbnail == "" &&
		images.Thumb == "" &&
		images.Poster == "" &&
		images.CoverPoster == ""
}

func tmdbIDFromDiscoverValue(value string, preferredType string) int {
	value = strings.TrimSpace(html.UnescapeString(value))
	if value == "" {
		return 0
	}
	lower := strings.ToLower(value)
	if !strings.Contains(lower, "tmdb") && !strings.Contains(lower, "themoviedb") {
		return 0
	}

	re := regexp.MustCompile(`(?i)(?:tmdb|themoviedb)://(?:movie|tv|show|series)/(\d+)`)
	if matches := re.FindStringSubmatch(value); len(matches) == 2 {
		id, _ := strconv.Atoi(matches[1])
		return id
	}

	if preferredType != "" {
		typeKey := "movie"
		if normalizePlexArtworkMediaType(preferredType) == "tv" || preferredType == "show" {
			typeKey = "(?:tv|show|series)"
		}
		typeRE := regexp.MustCompile(`(?i)` + typeKey + `[/=:](\d+)`)
		if matches := typeRE.FindStringSubmatch(value); len(matches) == 2 {
			id, _ := strconv.Atoi(matches[1])
			return id
		}
	}
	return trailingInt(value)
}

func plexArtworkFromDiscoverMetadata(meta plexDiscoverMetadata, token string) plexArtwork {
	artwork := plexArtwork{}

	addImage := func(imageType string, rawURL string) {
		imageURL := normalizePlexDiscoverAssetURL(rawURL, token)
		if !isValidPlexArtworkURL(imageURL) || isPlexTrailerThumbnailURL(imageURL) {
			return
		}
		switch normalizedPlexDiscoverImageType(imageType) {
		case "coverart", "cover_art", "landscape", "wide", "tile", "snapshot":
			artwork.CoverArt = append(artwork.CoverArt, imageURL)
			artwork.Landscape = append(artwork.Landscape, imageURL)
		case "background", "backgroundlandscape", "background_landscape", "art", "hero":
			artwork.Background = append(artwork.Background, imageURL)
		case "clearlogo", "clear_logo", "logo":
			artwork.ClearLogo = append(artwork.ClearLogo, imageURL)
		case "thumbnail", "thumb", "poster", "coverposter", "cover_poster":
			artwork.Thumbnail = append(artwork.Thumbnail, imageURL)
		default:
			if strings.Contains(imageURL, "/extras/") {
				artwork.CoverArt = append(artwork.CoverArt, imageURL)
				artwork.Landscape = append(artwork.Landscape, imageURL)
			}
		}
	}

	for _, image := range meta.Image {
		addImage(image.Type, image.URL)
	}

	addImage("coverArt", meta.Images.CoverArt)
	addImage("landscape", meta.Images.Landscape)
	addImage("wide", meta.Images.Wide)
	addImage("tile", meta.Images.Tile)
	addImage("snapshot", meta.Images.Snapshot)
	addImage("background", meta.Images.Background)
	addImage("backgroundLandscape", meta.Images.BackgroundLandscape)
	addImage("art", meta.Images.Art)
	addImage("hero", meta.Images.Hero)
	addImage("clearLogo", meta.Images.ClearLogo)
	addImage("logo", meta.Images.Logo)
	addImage("thumbnail", meta.Images.Thumbnail)
	addImage("thumb", meta.Images.Thumb)
	addImage("poster", meta.Images.Poster)
	addImage("coverPoster", meta.Images.CoverPoster)
	addImage("thumb", meta.Thumb)
	addImage("art", meta.Art)
	addImage("background", meta.Banner)

	return dedupePlexArtwork(artwork)
}

func normalizedPlexDiscoverImageType(value string) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(value)), "-", "_")
}

func normalizePlexDiscoverAssetURL(rawPath string, token string) string {
	rawPath = strings.TrimSpace(rawPath)
	if rawPath == "" {
		return ""
	}
	rawPath = strings.ReplaceAll(rawPath, `\u002F`, "/")
	rawPath = strings.ReplaceAll(rawPath, `\/`, "/")
	rawPath = html.UnescapeString(rawPath)
	if parsed, err := url.Parse(rawPath); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		return parsed.String()
	}

	if !strings.HasPrefix(rawPath, "/") {
		rawPath = "/" + rawPath
	}
	components, err := url.Parse("https://discover.provider.plex.tv" + rawPath)
	if err != nil {
		return rawPath
	}
	if strings.TrimSpace(token) != "" {
		values := components.Query()
		if values.Get("X-Plex-Token") == "" {
			values.Set("X-Plex-Token", strings.TrimSpace(token))
			components.RawQuery = values.Encode()
		}
	}
	return components.String()
}

func structuredPlexArtwork(rawHTML string) plexArtwork {
	normalized := normalizePlexArtworkHTML(rawHTML)
	artwork := plexArtwork{}

	if landscape := structuredPlexLandscapeTileURL(normalized); landscape != "" {
		artwork.CoverArt = append(artwork.CoverArt, landscape)
		artwork.Landscape = append(artwork.Landscape, landscape)
	}
	if background := structuredPlexImageURL(normalized, "background"); background != "" {
		artwork.Background = append(artwork.Background, background)
	}
	if landscape := structuredPlexImageURL(normalized, "backgroundLandscape"); landscape != "" {
		artwork.Background = append(artwork.Background, landscape)
	}
	if logo := structuredPlexClearLogoURL(normalized); logo != "" {
		artwork.ClearLogo = append(artwork.ClearLogo, logo)
	}
	if preload := preloadedPlexImageURL(normalized); preload != "" {
		artwork.Background = append(artwork.Background, preload)
	}
	if social := metaPlexImageURL(normalized, "property", "og:image"); social != "" {
		artwork.Thumbnail = append(artwork.Thumbnail, social)
	} else if social := metaPlexImageURL(normalized, "name", "twitter:image"); social != "" {
		artwork.Thumbnail = append(artwork.Thumbnail, social)
	}

	return dedupePlexArtwork(artwork)
}

func structuredPlexImageURL(rawHTML, field string) string {
	pattern := fmt.Sprintf(`"%s":\{"image":\{"url":"([^"]+)"`, regexp.QuoteMeta(field))
	matches := regexp.MustCompile(pattern).FindStringSubmatch(rawHTML)
	if len(matches) < 2 {
		return ""
	}
	decoded := decodePlexArtworkURL(matches[1])
	if isValidPlexArtworkURL(decoded) && !isPlexTrailerThumbnailURL(decoded) {
		return decoded
	}
	return ""
}

func structuredPlexLandscapeTileURL(rawHTML string) string {
	re := regexp.MustCompile(`"orientation":"landscape","size":"m","id":"[^"]*/extras/[^"]+","image":\{"url":"([^"]+)"`)
	for _, matches := range re.FindAllStringSubmatch(rawHTML, -1) {
		if len(matches) < 2 {
			continue
		}
		decoded := decodePlexArtworkURL(matches[1])
		if strings.Contains(decoded, "provider-static.plex.tv/discover/logos/p/") {
			continue
		}
		if isValidPlexArtworkURL(decoded) && !isPlexTrailerThumbnailURL(decoded) {
			return decoded
		}
	}
	return ""
}

func structuredPlexClearLogoURL(rawHTML string) string {
	re := regexp.MustCompile(`"clearLogo":\{"url":"([^"]+)"`)
	for _, matches := range re.FindAllStringSubmatch(rawHTML, -1) {
		if len(matches) < 2 {
			continue
		}
		decoded := decodePlexArtworkURL(matches[1])
		if strings.Contains(decoded, "provider-static.plex.tv/discover/logos/p/") {
			continue
		}
		if isValidPlexArtworkURL(decoded) && !isPlexTrailerThumbnailURL(decoded) {
			return decoded
		}
	}
	return ""
}

func preloadedPlexImageURL(rawHTML string) string {
	for _, tag := range htmlTags("link", rawHTML) {
		if !strings.EqualFold(htmlAttribute("as", tag), "image") {
			continue
		}
		if largest := largestPlexImageURL(htmlAttribute("imageSrcSet", tag)); largest != "" {
			return largest
		}
	}
	return ""
}

func largestPlexImageURL(srcset string) string {
	if strings.TrimSpace(srcset) == "" {
		return ""
	}

	var candidates []string
	for _, raw := range strings.Split(srcset, ",") {
		fields := strings.Fields(strings.TrimSpace(raw))
		if len(fields) == 0 {
			continue
		}
		candidate := html.UnescapeString(fields[0])
		if candidate != "" {
			candidates = append(candidates, candidate)
		}
	}
	if len(candidates) == 0 {
		return ""
	}
	return candidates[len(candidates)-1]
}

func metaPlexImageURL(rawHTML, keyAttribute, keyValue string) string {
	for _, tag := range htmlTags("meta", rawHTML) {
		if !strings.EqualFold(htmlAttribute(keyAttribute, tag), keyValue) {
			continue
		}
		if content := htmlAttribute("content", tag); content != "" {
			return html.UnescapeString(content)
		}
	}
	return ""
}

func htmlTags(name, rawHTML string) []string {
	re := regexp.MustCompile(`(?is)<` + regexp.QuoteMeta(name) + `\b[^>]*>`)
	return re.FindAllString(rawHTML, -1)
}

func htmlAttribute(name, tag string) string {
	re := regexp.MustCompile(`(?i)\s` + regexp.QuoteMeta(name) + `="([^"]*)"`)
	matches := re.FindStringSubmatch(tag)
	if len(matches) < 2 {
		return ""
	}
	return matches[1]
}

func normalizePlexArtworkHTML(value string) string {
	value = strings.ReplaceAll(value, `\u002F`, "/")
	value = strings.ReplaceAll(value, `\/`, "/")
	value = strings.ReplaceAll(value, "&amp;", "&")
	return value
}

func decodePlexArtworkURL(value string) string {
	decoded := html.UnescapeString(value)
	decoded = strings.ReplaceAll(decoded, `\u002F`, "/")
	decoded = strings.ReplaceAll(decoded, `\/`, "/")
	return decoded
}

func isValidPlexArtworkURL(value string) bool {
	if strings.TrimSpace(value) == "" {
		return false
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" {
		return false
	}
	host := strings.ToLower(parsed.Hostname())
	return host == "metadata-static.plex.tv" ||
		strings.HasSuffix(host, ".metadata-static.plex.tv") ||
		host == "provider-static.plex.tv" ||
		strings.HasSuffix(host, ".provider-static.plex.tv") ||
		host == "images.plex.tv" ||
		strings.HasSuffix(host, ".images.plex.tv") ||
		host == "discover.provider.plex.tv" ||
		strings.HasSuffix(host, ".discover.provider.plex.tv")
}

func isPlexTrailerThumbnailURL(value string) bool {
	return isPlexTrailerThumbnailURLDepth(value, 0)
}

func isPlexTrailerThumbnailURLDepth(value string, depth int) bool {
	if depth > 2 {
		return false
	}
	parsed, err := url.Parse(strings.TrimSpace(html.UnescapeString(value)))
	if err != nil || parsed.Host == "" {
		return false
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "i.ytimg.com" ||
		strings.HasSuffix(host, ".ytimg.com") ||
		host == "img.youtube.com" ||
		strings.HasSuffix(host, ".youtube.com") ||
		host == "youtube.com" {
		return true
	}
	for _, key := range []string{"url", "src"} {
		nested := strings.TrimSpace(parsed.Query().Get(key))
		if nested != "" && nested != value && isPlexTrailerThumbnailURLDepth(nested, depth+1) {
			return true
		}
	}
	return false
}

func dedupePlexArtwork(artwork plexArtwork) plexArtwork {
	artwork.CoverArt = uniqueNonEmptyStrings(artwork.CoverArt)
	artwork.Landscape = uniqueNonEmptyStrings(artwork.Landscape)
	artwork.Background = uniqueNonEmptyStrings(artwork.Background)
	artwork.ClearLogo = uniqueNonEmptyStrings(artwork.ClearLogo)
	artwork.Thumbnail = uniqueNonEmptyStrings(artwork.Thumbnail)
	return artwork
}

func isPlexCloudflareChallenge(body string) bool {
	return strings.Contains(body, "cf_chl_") ||
		strings.Contains(body, "cf-browser-verification") ||
		strings.Contains(body, "challenge-platform") ||
		strings.Contains(body, "<title>Just a moment...</title>")
}

func slugifyPlexArtworkTitle(title string) string {
	title = strings.ToLower(strings.TrimSpace(title))
	var builder strings.Builder
	lastDash := false

	for _, r := range title {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			builder.WriteRune(r)
			lastDash = false
			continue
		}
		if r == '&' {
			if !lastDash && builder.Len() > 0 {
				builder.WriteByte('-')
			}
			builder.WriteString("and")
			lastDash = false
			continue
		}
		if !lastDash && builder.Len() > 0 {
			builder.WriteByte('-')
			lastDash = true
		}
	}

	return strings.Trim(builder.String(), "-")
}

func normalizePlexArtworkMediaType(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "show", "series", "episode":
		return "tv"
	default:
		return normalized
	}
}

func plexArtworkKey(mediaType string, tmdbID int, imdbID string, title string, year int) string {
	normalizedType := normalizePlexArtworkMediaType(mediaType)
	if normalizedType == "" {
		return ""
	}
	if tmdbID > 0 {
		return normalizedType + ":tmdb:" + strconv.Itoa(tmdbID)
	}
	if id := imdbFromID(imdbID); id != "" {
		return normalizedType + ":imdb:" + strings.ToLower(id)
	}
	if slug := slugifyPlexArtworkTitle(title); slug != "" {
		if year > 0 {
			return normalizedType + ":title:" + slug + ":" + strconv.Itoa(year)
		}
		return normalizedType + ":title:" + slug
	}
	return ""
}

func firstPlexArtworkURL(groups ...[]string) string {
	for _, group := range groups {
		for _, value := range group {
			if trimmed := strings.TrimSpace(value); trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

func plexArtworkSummary(artwork plexArtwork) string {
	counts := map[string]int{
		"coverArt":   len(artwork.CoverArt),
		"landscape":  len(artwork.Landscape),
		"background": len(artwork.Background),
		"clearLogo":  len(artwork.ClearLogo),
		"thumbnail":  len(artwork.Thumbnail),
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", key, counts[key]))
	}
	return strings.Join(parts, ",")
}

type plexArtworkStopError struct {
	message string
}

func (e *plexArtworkStopError) Error() string {
	return e.message
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

func manifestResourceNames(manifest stremioManifest) []string {
	seen := map[string]bool{}
	var names []string
	for _, raw := range manifest.Resources {
		name := ""
		switch value := raw.(type) {
		case string:
			name = value
		case map[string]any:
			name = fmt.Sprint(value["name"])
		}
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func manifestCapabilities(manifest stremioManifest) []string {
	var out []string
	for _, resource := range []string{"catalog", "meta", "stream", "subtitles"} {
		if manifestSupportsResource(manifest, resource) {
			out = append(out, resource)
		}
	}
	if hasLiveManifestCatalog(manifest) {
		out = append(out, "live_tv")
	}
	return out
}

func (s *appState) dashboardCatalogs(ctx context.Context) []dashboardManifestCatalog {
	s.mu.RLock()
	installed := append([]installedManifest(nil), s.config.Manifests...)
	prefs := catalogPreferenceMapLocked(s.config.Catalogs)
	s.mu.RUnlock()

	all := make([]dashboardManifestCatalog, 0)
	order := 0
	for _, item := range installed {
		if !item.Enabled {
			continue
		}
		manifest, _, err := s.fetchManifest(ctx, item.URL, false)
		if err != nil {
			continue
		}
		catalogs := dashboardCatalogs(item, manifest, prefs, order)
		order += len(manifest.Catalogs)
		all = append(all, catalogs...)
	}
	sortDashboardCatalogs(all)
	return all
}

func (s *appState) enabledCatalogEntries(ctx context.Context) []manifestCatalogEntry {
	s.mu.RLock()
	installed := append([]installedManifest(nil), s.config.Manifests...)
	prefs := catalogPreferenceMapLocked(s.config.Catalogs)
	s.mu.RUnlock()

	entries := make([]manifestCatalogEntry, 0)
	order := 0
	for _, item := range installed {
		if !item.Enabled || strings.TrimSpace(item.URL) == "" {
			continue
		}
		manifest, base, err := s.fetchManifest(ctx, item.URL, false)
		if err != nil {
			log.Printf("manifest %s failed: %v", item.URL, err)
			continue
		}
		for _, catalog := range manifest.Catalogs {
			pref := catalogPreferenceFor(prefs, item, catalog, order)
			order++
			if !pref.Enabled {
				continue
			}
			entries = append(entries, manifestCatalogEntry{
				Item:     item,
				Manifest: manifest,
				Base:     base,
				Catalog:  catalog,
				Pref:     pref,
				Order:    pref.SortOrder,
			})
		}
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Order != entries[j].Order {
			return entries[i].Order < entries[j].Order
		}
		return entries[i].Pref.Key < entries[j].Pref.Key
	})
	return entries
}

func dashboardCatalogs(item installedManifest, manifest stremioManifest, prefs map[string]catalogPreference, startOrder int) []dashboardManifestCatalog {
	catalogs := make([]dashboardManifestCatalog, 0, len(manifest.Catalogs))
	for index, catalog := range manifest.Catalogs {
		pref := catalogPreferenceFor(prefs, item, catalog, startOrder+index)
		entry := dashboardManifestCatalog{
			Key:          pref.Key,
			ManifestID:   pref.ManifestID,
			ManifestName: firstNonEmpty(item.Name, manifest.Name, item.ID),
			Type:         catalog.Type,
			ID:           catalog.ID,
			Name:         firstNonEmpty(pref.Name, catalog.Name, catalog.ID),
			OriginalName: firstNonEmpty(catalog.Name, catalog.ID),
			Enabled:      pref.Enabled,
			SortOrder:    pref.SortOrder,
		}
		for _, extra := range catalog.Extra {
			name := strings.TrimSpace(extra.Name)
			if name == "" {
				continue
			}
			if strings.EqualFold(name, "search") {
				entry.Search = true
			}
			if extra.IsRequired {
				entry.RequiredExtras = append(entry.RequiredExtras, name)
			} else {
				entry.OptionalExtras = append(entry.OptionalExtras, name)
			}
		}
		catalogs = append(catalogs, entry)
	}
	sortDashboardCatalogs(catalogs)
	return catalogs
}

func catalogPreferenceFor(prefs map[string]catalogPreference, item installedManifest, catalog stremioCatalog, order int) catalogPreference {
	key := catalogKey(item.ID, catalog.Type, catalog.ID)
	if pref, ok := prefs[key]; ok {
		pref.Key = key
		pref.ManifestID = firstNonEmpty(pref.ManifestID, item.ID)
		pref.CatalogType = firstNonEmpty(pref.CatalogType, catalog.Type)
		pref.CatalogID = firstNonEmpty(pref.CatalogID, catalog.ID)
		return pref
	}
	return catalogPreference{
		Key:         key,
		ManifestID:  item.ID,
		CatalogType: catalog.Type,
		CatalogID:   catalog.ID,
		Enabled:     true,
		SortOrder:   order,
	}
}

func catalogPreferenceMapLocked(items []catalogPreference) map[string]catalogPreference {
	out := make(map[string]catalogPreference, len(items))
	for _, item := range items {
		key := strings.TrimSpace(item.Key)
		if key == "" {
			key = catalogKey(item.ManifestID, item.CatalogType, item.CatalogID)
		}
		if key == "" {
			continue
		}
		item.Key = key
		out[key] = item
	}
	return out
}

func catalogKey(manifestID, catalogType, catalogID string) string {
	manifestID = strings.TrimSpace(manifestID)
	catalogType = strings.TrimSpace(catalogType)
	catalogID = strings.TrimSpace(catalogID)
	if manifestID == "" || catalogType == "" || catalogID == "" {
		return ""
	}
	return manifestID + "|" + catalogType + "|" + catalogID
}

func splitCatalogKey(key string) (string, string, string) {
	parts := strings.SplitN(strings.TrimSpace(key), "|", 3)
	if len(parts) != 3 {
		return "", "", ""
	}
	return parts[0], parts[1], parts[2]
}

func sortDashboardCatalogs(catalogs []dashboardManifestCatalog) {
	sort.SliceStable(catalogs, func(i, j int) bool {
		if catalogs[i].SortOrder != catalogs[j].SortOrder {
			return catalogs[i].SortOrder < catalogs[j].SortOrder
		}
		if catalogs[i].ManifestName != catalogs[j].ManifestName {
			return strings.ToLower(catalogs[i].ManifestName) < strings.ToLower(catalogs[j].ManifestName)
		}
		return strings.ToLower(catalogs[i].Name) < strings.ToLower(catalogs[j].Name)
	})
}

func removeManifestCatalogPreferences(items []catalogPreference, manifestID string) []catalogPreference {
	next := items[:0]
	prefix := manifestID + "|"
	for _, item := range items {
		if item.ManifestID == manifestID || strings.HasPrefix(item.Key, prefix) {
			continue
		}
		next = append(next, item)
	}
	return next
}

func installedManifestURLSet(items []installedManifest) map[string]bool {
	out := make(map[string]bool, len(items))
	for _, item := range items {
		if normalized := normalizeManifestURL(item.URL); normalized != "" {
			out[strings.ToLower(normalized)] = true
		}
	}
	return out
}

func dashboardAddonFromEntry(entry addonCatalogEntry, installedURLs map[string]bool) dashboardAddon {
	manifest := entry.Manifest
	if manifest.ID == "" {
		manifest.ID = entry.ID
	}
	if manifest.Name == "" {
		manifest.Name = entry.Name
	}
	if manifest.Description == "" {
		manifest.Description = entry.Description
	}
	if manifest.Logo == "" {
		manifest.Logo = entry.Logo
	}
	if manifest.Version == "" {
		manifest.Version = entry.Version
	}
	manifestURL := normalizeManifestURL(firstNonEmpty(entry.TransportURL, entry.URL))
	catalogs := dashboardCatalogs(installedManifest{ID: slug(firstNonEmpty(manifest.ID, manifest.Name, manifestURL)), Name: manifest.Name}, manifest, nil, 0)
	configRequired := manifestBoolHint(manifest, "configurationRequired", "requiresConfiguration")
	configurable := manifestBoolHint(manifest, "configurable") || configRequired
	configURL := ""
	if configurable {
		configURL = addonConfigURL(manifestURL)
	}
	return dashboardAddon{
		ID:                    firstNonEmpty(manifest.ID, slug(manifest.Name), slug(manifestURL)),
		Name:                  firstNonEmpty(manifest.Name, manifest.ID, manifestURL),
		Description:           manifest.Description,
		Version:               manifest.Version,
		Logo:                  manifest.Logo,
		URL:                   manifestURL,
		ConfigURL:             configURL,
		TransportName:         entry.TransportName,
		Installed:             installedURLs[strings.ToLower(manifestURL)],
		Configurable:          configurable,
		ConfigurationRequired: configRequired,
		Resources:             manifestResourceNames(manifest),
		Types:                 append([]string(nil), manifest.Types...),
		Capabilities:          manifestCapabilities(manifest),
		Catalogs:              catalogs,
	}
}

func addonMatchesFilters(addon dashboardAddon, query, capability, mediaType string) bool {
	query = strings.ToLower(strings.TrimSpace(query))
	if query != "" {
		text := strings.ToLower(strings.Join([]string{
			addon.ID,
			addon.Name,
			addon.Description,
			addon.URL,
			strings.Join(addon.Capabilities, " "),
			strings.Join(addon.Types, " "),
		}, " "))
		if !strings.Contains(text, query) {
			return false
		}
	}
	capability = strings.ToLower(strings.TrimSpace(capability))
	if capability != "" && capability != "all" {
		found := false
		for _, item := range append(append([]string{}, addon.Capabilities...), addon.Resources...) {
			if strings.EqualFold(item, capability) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	mediaType = strings.ToLower(strings.TrimSpace(mediaType))
	if mediaType != "" && mediaType != "all" {
		normalized := normalizeStremioType(mediaType)
		found := false
		for _, item := range addon.Types {
			if normalizeStremioType(item) == normalized || strings.EqualFold(item, mediaType) {
				found = true
				break
			}
		}
		if !found {
			for _, catalog := range addon.Catalogs {
				if normalizeStremioType(catalog.Type) == normalized || strings.EqualFold(catalog.Type, mediaType) {
					found = true
					break
				}
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func addonConfigURL(manifestURL string) string {
	manifestURL = normalizeManifestURL(manifestURL)
	if manifestURL == "" {
		return ""
	}
	return strings.TrimSuffix(manifestURL, "/manifest.json") + "/configure"
}

func manifestBoolHint(manifest stremioManifest, keys ...string) bool {
	hints, ok := manifest.BehaviorHints.(map[string]any)
	if !ok {
		return false
	}
	for _, key := range keys {
		for rawKey, rawValue := range hints {
			if !strings.EqualFold(rawKey, key) {
				continue
			}
			switch value := rawValue.(type) {
			case bool:
				if value {
					return true
				}
			case string:
				if parsed, err := strconv.ParseBool(value); err == nil && parsed {
					return true
				}
			case float64:
				if value != 0 {
					return true
				}
			}
		}
	}
	return false
}

func hasLiveManifestCatalog(manifest stremioManifest) bool {
	for _, catalog := range manifest.Catalogs {
		if isLiveCatalog(manifest, installedManifest{}, catalog) {
			return true
		}
	}
	return false
}

func manifestSupportsType(manifest stremioManifest, wanted string) bool {
	wanted = normalizeStremioType(wanted)
	if wanted == "" || len(manifest.Types) == 0 {
		return true
	}
	hasKnownType := false
	for _, raw := range manifest.Types {
		normalized := normalizeStremioType(raw)
		if normalized != "" {
			hasKnownType = true
		}
		if normalized == wanted {
			return true
		}
	}
	if !hasKnownType {
		return true
	}
	return false
}

func isLiveCatalog(manifest stremioManifest, item installedManifest, catalog stremioCatalog) bool {
	rawType := strings.ToLower(strings.TrimSpace(catalog.Type))
	switch rawType {
	case "channel", "channels", "live", "live-tv", "livetv", "iptv":
		return true
	case "tv":
		text := strings.ToLower(strings.Join([]string{
			manifest.ID,
			manifest.Name,
			item.Name,
			catalog.ID,
			catalog.Name,
		}, " "))
		return strings.Contains(text, "live") ||
			strings.Contains(text, "iptv") ||
			strings.Contains(text, "channel") ||
			manifestHasRawType(manifest, "tv")
	default:
		return false
	}
}

func manifestHasRawType(manifest stremioManifest, wanted string) bool {
	wanted = strings.ToLower(strings.TrimSpace(wanted))
	if wanted == "" {
		return false
	}
	for _, raw := range manifest.Types {
		if strings.ToLower(strings.TrimSpace(raw)) == wanted {
			return true
		}
	}
	return false
}

func liveStreamTypes(catalogType string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(value string) {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" || seen[value] {
			return
		}
		seen[value] = true
		out = append(out, value)
	}
	add(catalogType)
	add("channel")
	add("tv")
	add("live")
	add("iptv")
	return out
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
	case "mdblist":
		return "mixed"
	case "mixed", "all":
		return "mixed"
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

func watchItemFromTraktMovie(movie traktMovie, watched bool, updatedAt time.Time, progress float64, playCount int) watchStateItem {
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	item := watchStateItem{
		MediaType:       "movie",
		Title:           movie.Title,
		Year:            movie.Year,
		IMDBID:          strings.TrimSpace(movie.IDs.IMDB),
		TMDBID:          movie.IDs.TMDB,
		TVDBID:          movie.IDs.TVDB,
		TraktID:         movie.IDs.Trakt,
		Watched:         watched,
		ProgressPercent: progress,
		PlayCount:       playCount,
		Source:          "trakt",
		UpdatedAt:       updatedAt,
	}
	if watched {
		item.WatchedAt = updatedAt
	}
	item.ID = watchStateKey(item)
	return item
}

func watchItemFromTraktEpisode(show traktShow, episode traktEpisode, watched bool, updatedAt time.Time, progress float64, playCount int) watchStateItem {
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	season := episode.Season
	if season == 0 {
		season = 1
	}
	item := watchStateItem{
		MediaType:       "episode",
		Title:           firstNonEmpty(episode.Title, show.Title),
		ParentTitle:     show.Title,
		Year:            show.Year,
		IMDBID:          strings.TrimSpace(show.IDs.IMDB),
		TMDBID:          show.IDs.TMDB,
		TVDBID:          firstNonZero(show.IDs.TVDB, episode.IDs.TVDB),
		TraktID:         firstNonZero(episode.IDs.Trakt, show.IDs.Trakt),
		Season:          season,
		Episode:         episode.Number,
		Watched:         watched,
		ProgressPercent: progress,
		PlayCount:       playCount,
		AirDate:         traktEpisodeAirDate(episode),
		Source:          "trakt",
		UpdatedAt:       updatedAt,
	}
	if watched {
		item.WatchedAt = updatedAt
	}
	item.ID = watchStateKey(item)
	return item
}

func watchItemFromTraktUpNextEpisode(show traktShow, episode traktEpisode, updatedAt time.Time) watchStateItem {
	item := watchItemFromTraktEpisode(show, episode, false, updatedAt, 0, 0)
	item.Source = "trakt-up-next"
	item.SourceUserID = strings.TrimSpace(show.IDs.Slug)
	item.Watched = false
	item.WatchedAt = time.Time{}
	item.ProgressPercent = 0
	item.ProgressSeconds = 0
	item.DurationSeconds = 0
	item.ID = watchStateKey(item)
	return item
}

func traktShowAPIID(show traktShow) string {
	if id := strings.TrimSpace(show.IDs.Slug); id != "" {
		return id
	}
	if show.IDs.Trakt > 0 {
		return strconv.Itoa(show.IDs.Trakt)
	}
	return strings.TrimSpace(show.IDs.IMDB)
}

func mediaIDsFromStrings(rawIDs []string) (string, int, int) {
	var imdbID string
	var tmdbID int
	var tvdbID int
	for _, raw := range rawIDs {
		raw = strings.TrimSpace(raw)
		lower := strings.ToLower(raw)
		if imdbID == "" {
			imdbID = imdbFromID(raw)
		}
		if tmdbID == 0 && strings.Contains(lower, "tmdb") {
			tmdbID = trailingInt(raw)
		}
		if tvdbID == 0 && strings.Contains(lower, "tvdb") {
			tvdbID = trailingInt(raw)
		}
	}
	return imdbID, tmdbID, tvdbID
}

func watchStateKey(item watchStateItem) string {
	mediaType := strings.ToLower(strings.TrimSpace(item.MediaType))
	if mediaType != "movie" && mediaType != "episode" {
		return ""
	}
	id := firstNonEmpty(item.IMDBID)
	if id == "" && item.TMDBID > 0 {
		id = "tmdb:" + strconv.Itoa(item.TMDBID)
	}
	if id == "" && item.TVDBID > 0 {
		id = "tvdb:" + strconv.Itoa(item.TVDBID)
	}
	if id == "" && item.TraktID > 0 {
		id = "trakt:" + strconv.Itoa(item.TraktID)
	}
	if id == "" {
		id = slug(firstNonEmpty(item.ParentTitle, item.Title) + "-" + strconv.Itoa(item.Year))
	}
	if id == "" {
		return ""
	}
	if mediaType == "episode" {
		if item.Season <= 0 || item.Episode <= 0 {
			return ""
		}
		return fmt.Sprintf("episode:%s:%d:%d", strings.ToLower(id), item.Season, item.Episode)
	}
	return "movie:" + strings.ToLower(id)
}

func watchStateItemFromMarkRequest(req watchStateMarkRequest) watchStateItem {
	mediaType := strings.ToLower(strings.TrimSpace(req.MediaType))
	if mediaType == "" {
		mediaType = watchStateMediaTypeFromRatingKey(req.RatingKey)
	}
	imdbID := strings.ToLower(strings.TrimSpace(req.IMDBID))
	if imdbID == "" {
		imdbID = imdbFromID(req.RatingKey)
	}
	return watchStateItem{
		ID:          strings.TrimSpace(firstNonEmpty(req.ID, req.RatingKey)),
		MediaType:   mediaType,
		Title:       strings.TrimSpace(req.Title),
		ParentTitle: strings.TrimSpace(req.ParentTitle),
		Year:        req.Year,
		IMDBID:      imdbID,
		TMDBID:      req.TMDBID,
		TVDBID:      req.TVDBID,
		TraktID:     req.TraktID,
		Season:      req.Season,
		Episode:     req.Episode,
		Source:      "vortexo-manual",
	}
}

func watchStateMediaTypeFromRatingKey(ratingKey string) string {
	parts := strings.Split(strings.ToLower(strings.TrimSpace(ratingKey)), ":")
	for _, part := range parts {
		switch part {
		case "movie":
			return "movie"
		case "episode":
			return "episode"
		}
	}
	return ""
}

func (s *appState) watchStateIndexLocked(key string, req watchStateMarkRequest) int {
	requestID := strings.TrimSpace(firstNonEmpty(req.ID, req.RatingKey))
	requestItem := watchStateItemFromMarkRequest(req)
	for i := range s.watchState.Items {
		existing := s.watchState.Items[i]
		existingKey := watchStateKey(existing)
		if existingKey == key {
			return i
		}
		if requestID != "" && (strings.EqualFold(existing.ID, requestID) || strings.EqualFold(existingKey, requestID)) {
			return i
		}
		if watchStateItemsMatch(existing, requestItem) {
			return i
		}
	}
	return -1
}

func watchStateItemsMatch(left watchStateItem, right watchStateItem) bool {
	leftType := strings.ToLower(strings.TrimSpace(left.MediaType))
	rightType := strings.ToLower(strings.TrimSpace(right.MediaType))
	if leftType == "" || rightType == "" || leftType != rightType {
		return false
	}
	if leftType == "episode" && (left.Season != right.Season || left.Episode != right.Episode) {
		return false
	}
	if left.IMDBID != "" && right.IMDBID != "" && strings.EqualFold(left.IMDBID, right.IMDBID) {
		return true
	}
	if left.TMDBID > 0 && right.TMDBID > 0 && left.TMDBID == right.TMDBID {
		return true
	}
	if left.TVDBID > 0 && right.TVDBID > 0 && left.TVDBID == right.TVDBID {
		return true
	}
	if left.TraktID > 0 && right.TraktID > 0 && left.TraktID == right.TraktID {
		return true
	}
	return false
}

func applyWatchStateMark(item watchStateItem, watched bool, updatedAt time.Time) watchStateItem {
	item.Watched = watched
	item.UpdatedAt = updatedAt
	if watched {
		item.WatchedAt = maxTime(item.WatchedAt, updatedAt)
		item.PlayCount = maxInt(item.PlayCount, 1)
		item.ProgressPercent = 0
		item.ProgressSeconds = 0
		if key := watchStateKey(item); key != "" {
			item.ID = key
		}
		return item
	}

	item.WatchedAt = time.Time{}
	item.PlayCount = 0
	if key := watchStateKey(item); key != "" {
		item.ID = key
	}
	return item
}

func mergeWatchStateItem(existing watchStateItem, incoming watchStateItem) watchStateItem {
	if existing.ID == "" {
		existing.ID = watchStateKey(existing)
	}
	existing.Title = firstNonEmpty(existing.Title, incoming.Title)
	existing.ParentTitle = firstNonEmpty(existing.ParentTitle, incoming.ParentTitle)
	existing.Year = firstNonZero(existing.Year, incoming.Year)
	existing.IMDBID = firstNonEmpty(existing.IMDBID, incoming.IMDBID)
	existing.TMDBID = firstNonZero(existing.TMDBID, incoming.TMDBID)
	existing.TVDBID = firstNonZero(existing.TVDBID, incoming.TVDBID)
	existing.TraktID = firstNonZero(existing.TraktID, incoming.TraktID)
	existing.Season = firstNonZero(existing.Season, incoming.Season)
	existing.Episode = firstNonZero(existing.Episode, incoming.Episode)
	existing.Watched = existing.Watched || incoming.Watched
	existing.WatchedAt = maxTime(existing.WatchedAt, incoming.WatchedAt)
	existing.PlayCount = maxInt(existing.PlayCount, incoming.PlayCount)
	existing.Source = mergeSourceLabel(existing.Source, incoming.Source)
	existing.Overview = firstNonEmpty(existing.Overview, incoming.Overview)
	existing.PosterPath = firstNonEmpty(existing.PosterPath, incoming.PosterPath)
	existing.BackdropPath = firstNonEmpty(existing.BackdropPath, incoming.BackdropPath)
	existing.LandscapePath = firstNonEmpty(existing.LandscapePath, incoming.LandscapePath)
	existing.LogoPath = firstNonEmpty(existing.LogoPath, incoming.LogoPath)
	existing.StillPath = firstNonEmpty(existing.StillPath, incoming.StillPath)
	existing.ReleaseDate = firstNonEmpty(existing.ReleaseDate, incoming.ReleaseDate)
	existing.AirDate = firstNonEmpty(existing.AirDate, incoming.AirDate)
	existing.Runtime = firstNonZero(existing.Runtime, incoming.Runtime)
	if len(existing.Genres) == 0 {
		existing.Genres = uniqueNonEmptyStrings(incoming.Genres)
	}
	if existing.VoteAverage == 0 {
		existing.VoteAverage = incoming.VoteAverage
	}

	if !incoming.UpdatedAt.IsZero() && (existing.UpdatedAt.IsZero() || !incoming.UpdatedAt.Before(existing.UpdatedAt)) {
		if incoming.ProgressPercent > 0 {
			existing.ProgressPercent = incoming.ProgressPercent
		}
		if incoming.ProgressSeconds > 0 {
			existing.ProgressSeconds = incoming.ProgressSeconds
		}
		if incoming.DurationSeconds > 0 {
			existing.DurationSeconds = incoming.DurationSeconds
		}
		existing.UpdatedAt = incoming.UpdatedAt
	}
	if existing.UpdatedAt.IsZero() {
		existing.UpdatedAt = incoming.UpdatedAt
	}
	if existing.Watched {
		existing.ProgressPercent = 0
		existing.ProgressSeconds = 0
	}
	if key := watchStateKey(existing); key != "" {
		existing.ID = key
	}
	return existing
}

func mergeSourceLabel(existing string, incoming string) string {
	existing = strings.TrimSpace(existing)
	incoming = strings.TrimSpace(incoming)
	if existing == "" {
		return incoming
	}
	if incoming == "" || strings.Contains(","+existing+",", ","+incoming+",") {
		return existing
	}
	return existing + "," + incoming
}

func maxTime(a time.Time, b time.Time) time.Time {
	if a.IsZero() {
		return b
	}
	if b.After(a) {
		return b
	}
	return a
}

func minTime(a time.Time, b time.Time) time.Time {
	if a.IsZero() {
		return b
	}
	if b.IsZero() || a.Before(b) {
		return a
	}
	return b
}

func firstNonZero(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func firstNonZero64(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func firstNonZeroFloat(values ...float64) float64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func unixTime(value int64) time.Time {
	if value <= 0 {
		return time.Time{}
	}
	return time.Unix(value, 0).UTC()
}

func trailingInt(value string) int {
	matches := regexp.MustCompile(`\d+`).FindAllString(value, -1)
	if len(matches) == 0 {
		return 0
	}
	parsed, _ := strconv.Atoi(matches[len(matches)-1])
	return parsed
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

func joinedErrorMessage(errs ...error) string {
	parts := make([]string, 0, len(errs))
	seen := map[string]bool{}
	for _, err := range errs {
		if err == nil {
			continue
		}
		message := strings.TrimSpace(err.Error())
		if message == "" || seen[message] {
			continue
		}
		seen[message] = true
		parts = append(parts, message)
	}
	if len(parts) == 0 {
		return "request failed"
	}
	return strings.Join(parts, "; ")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func uniqueNonEmptyStrings(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, value)
	}
	return out
}

func stringSet(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = true
		}
	}
	return out
}

func normalizedSearchText(value string) string {
	value = strings.ToLower(strings.TrimSpace(html.UnescapeString(value)))
	if value == "" {
		return ""
	}
	var builder strings.Builder
	lastSpace := false
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			builder.WriteRune(r)
			lastSpace = false
			continue
		}
		if !lastSpace && builder.Len() > 0 {
			builder.WriteByte(' ')
			lastSpace = true
		}
	}
	return strings.TrimSpace(builder.String())
}

func absoluteAddonURL(raw string, base string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if parsed, err := url.Parse(raw); err == nil && parsed.IsAbs() {
		return raw
	}
	if strings.HasPrefix(raw, "/") && strings.TrimSpace(base) != "" {
		return strings.TrimRight(base, "/") + raw
	}
	return raw
}

func commaSet(raw string) map[string]bool {
	out := map[string]bool{}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out[part] = true
		}
	}
	return out
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
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

func truthyQuery(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "y", "on", "refresh":
		return true
	default:
		return false
	}
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

func trailerVideoIDFromPath(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 5 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "trailers" && parts[4] == "play" {
		return youtubeVideoID(parts[3])
	}
	if len(parts) == 8 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "media" && parts[5] == "trailers" && parts[7] == "play" {
		return youtubeVideoID(parts[6])
	}
	return ""
}

func (s *appState) resolveYouTubeTrailerURL(ctx context.Context, videoID string) (string, string, error) {
	videoID = youtubeVideoID(videoID)
	if videoID == "" {
		return "", "", fmt.Errorf("missing video id")
	}

	payloads := []map[string]any{
		{
			"videoId": videoID,
			"context": map[string]any{
				"client": map[string]any{
					"clientName":    "IOS",
					"clientVersion": "20.10.4",
					"deviceMake":    "Apple",
					"deviceModel":   "iPhone16,2",
					"osName":        "iPhone",
					"osVersion":     "18.3.2",
					"hl":            "en",
					"gl":            "US",
				},
			},
			"contentCheckOk": true,
			"racyCheckOk":    true,
		},
		{
			"videoId": videoID,
			"context": map[string]any{
				"client": map[string]any{
					"clientName":        "ANDROID",
					"clientVersion":     "20.10.38",
					"androidSdkVersion": 35,
					"osName":            "Android",
					"osVersion":         "15",
					"platform":          "MOBILE",
					"hl":                "en",
					"gl":                "US",
					"userAgent":         "com.google.android.youtube/20.10.38 (Linux; U; Android 15) gzip",
				},
			},
			"contentCheckOk": true,
			"racyCheckOk":    true,
		},
	}

	var lastErr error
	for _, payload := range payloads {
		response, err := s.fetchYouTubePlayerResponse(ctx, payload)
		if err != nil {
			lastErr = err
			continue
		}
		playbackURL, container, err := selectYouTubePlaybackURL(response)
		if err == nil {
			return playbackURL, container, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no playable stream")
	}
	return "", "", lastErr
}

func (s *appState) fetchYouTubePlayerResponse(ctx context.Context, payload map[string]any) (youtubePlayerResponse, error) {
	var out youtubePlayerResponse
	body, err := json.Marshal(payload)
	if err != nil {
		return out, err
	}

	endpoint := "https://www.youtube.com/youtubei/v1/player?key=" + url.QueryEscape(youtubePlayerAPIKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return out, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("User-Agent", "com.google.ios.youtube/20.10.4 (iPhone16,2; U; CPU iPhone OS 18_3_2 like Mac OS X; en_US)")

	resp, err := s.client.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return out, fmt.Errorf("youtube player status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&out); err != nil {
		return out, err
	}
	if strings.TrimSpace(out.PlayabilityStatus.Status) != "" && out.PlayabilityStatus.Status != "OK" {
		return out, errors.New(firstNonEmpty(out.PlayabilityStatus.Reason, out.PlayabilityStatus.Status))
	}
	return out, nil
}

func selectYouTubePlaybackURL(response youtubePlayerResponse) (string, string, error) {
	if strings.TrimSpace(response.StreamingData.HLSURL) != "" {
		return strings.TrimSpace(response.StreamingData.HLSURL), "m3u8", nil
	}

	formats := append([]youtubeFormat{}, response.StreamingData.Formats...)
	formats = append(formats, response.StreamingData.AdaptiveFormats...)
	playableFormats := make([]youtubeFormat, 0, len(formats))
	for _, format := range formats {
		if strings.TrimSpace(format.URL) == "" {
			continue
		}
		lowerMime := strings.ToLower(format.MimeType)
		if !strings.Contains(lowerMime, "video/") {
			continue
		}
		if !strings.Contains(lowerMime, "mp4") {
			continue
		}
		if !youtubeFormatHasAudio(format) {
			continue
		}
		playableFormats = append(playableFormats, format)
	}
	if len(playableFormats) == 0 {
		return "", "", fmt.Errorf("no direct trailer stream with audio")
	}

	formats = playableFormats
	sort.SliceStable(formats, func(i, j int) bool {
		left := youtubeFormatScore(formats[i])
		right := youtubeFormatScore(formats[j])
		if left != right {
			return left > right
		}
		return formats[i].Bitrate > formats[j].Bitrate
	})
	for _, format := range formats {
		return strings.TrimSpace(format.URL), youtubeContainerForFormat(format), nil
	}
	return "", "", fmt.Errorf("no direct trailer stream")
}

func youtubeFormatScore(format youtubeFormat) int {
	score := format.Height * 1000
	lowerMime := strings.ToLower(format.MimeType)
	if strings.Contains(lowerMime, "mp4") {
		score += 5000
	}
	if youtubeFormatHasAudio(format) {
		score += 1000000
	}
	if strings.Contains(lowerMime, "mp4a") {
		score += 1000
	}
	return score + format.Bitrate/10000
}

func youtubeFormatHasAudio(format youtubeFormat) bool {
	lowerMime := strings.ToLower(format.MimeType)
	return strings.TrimSpace(format.AudioQuality) != "" ||
		strings.Contains(lowerMime, "mp4a") ||
		strings.Contains(lowerMime, "opus") ||
		format.AudioChannels > 0
}

func youtubeContainerForFormat(format youtubeFormat) string {
	lower := strings.ToLower(format.MimeType)
	switch {
	case strings.Contains(lower, "mp4"):
		return "mp4"
	case strings.Contains(lower, "webm"):
		return "webm"
	default:
		return "video"
	}
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
  <title>Vortexo Add-on Server</title>
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
        <h1>Vortexo Add-on Server</h1>
        <div class="subtitle">Add-on setup wizard</div>
      </div>
    </div>
    <div class="steps">
      <button class="step active" data-step="welcome" onclick="showStep('welcome')"><span class="dot"></span><span>Welcome</span></button>
      <button class="step" data-step="signin" onclick="showStep('signin')"><span class="dot"></span><span>Sign In</span></button>
      <button class="step" data-step="accounts" onclick="showStep('accounts')"><span class="dot"></span><span>Accounts</span></button>
      <button class="step" data-step="catalogs" onclick="showStep('catalogs')"><span class="dot"></span><span>Catalogs</span></button>
      <button class="step" data-step="streams" onclick="showStep('streams')"><span class="dot"></span><span>Streams</span></button>
      <button class="step" data-step="watch" onclick="showStep('watch')"><span class="dot"></span><span>Watch Sync</span></button>
      <button class="step" data-step="install" onclick="showStep('install')"><span class="dot"></span><span>Install</span></button>
      <button class="step" data-step="finish" onclick="showStep('finish')"><span class="dot"></span><span>Finish</span></button>
    </div>
    <div class="fineprint">
      Vortexo Add-on Server stores installed manifest URLs and optional watch-sync credentials locally on this server.
    </div>
  </aside>
  <main>
    <div class="topbar">
      <div>
        <div class="subtitle">First-run setup for Vortexo Apple TV</div>
        <h1>Build your add-on-powered server</h1>
      </div>
      <div id="authPill" class="statusPill">Signed out</div>
    </div>
    <section class="stage">
      <div id="welcome" class="pane active">
        <div class="panel hero">
          <h2>Welcome to Vortexo Add-on Server</h2>
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
          <p class="muted">Enter your own keys here and Vortexo Add-on Server will create AIOMetadata and AIOStreams manifests for you. Keys are sent to the selected upstream addon instance to create its normal manifest configuration; Vortexo Add-on Server stores only the returned manifest URLs.</p>
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

      <div id="watch" class="pane">
        <div class="panel">
          <h2>Watch History & Artwork</h2>
          <p class="muted">Optional. Import watched state and let this server use Plex Discover artwork for installed add-on media.</p>
          <div class="grid">
            <div class="card">
              <h3>Plex Discover Artwork</h3>
              <p>Sign in with Plex so the hourly artwork cache can use enhanced Discover 16:9 landscapes.</p>
              <label>Plex Token</label>
              <input id="plexAccessToken" type="password" placeholder="Paste token or use PIN login">
              <div class="actions">
                <button onclick="startPlexLogin()">Plex PIN Login</button>
                <button class="secondary" onclick="savePlexSettings()">Save Token</button>
                <button class="ghost" onclick="clearPlexSettings()">Clear</button>
              </div>
              <div id="plexDeviceBox" class="message muted"></div>
              <div id="plexStatus" class="message muted">Sign in to load Plex artwork status.</div>
            </div>
            <div class="card">
              <h3>Trakt</h3>
              <p>Imports watched movies, watched episodes, paused playback progress, and Up Next entries from your Trakt account.</p>
              <label>Trakt Client ID</label>
              <input id="traktClientId" placeholder="Your Trakt app client ID">
              <label>Trakt Client Secret</label>
              <input id="traktClientSecret" type="password" placeholder="Leave blank to keep saved secret">
              <label>Access Token</label>
              <input id="traktAccessToken" type="password" placeholder="Paste token or use device login">
              <label>Refresh Token</label>
              <input id="traktRefreshToken" type="password" placeholder="Optional">
              <div class="actions">
                <button onclick="saveWatchSettings()">Save Trakt</button>
                <button class="secondary" onclick="startTraktDeviceLogin()">Device Login</button>
                <button class="secondary" onclick="syncTraktWatch()">Sync Trakt</button>
              </div>
              <div id="traktDeviceBox" class="message muted"></div>
            </div>
          </div>
          <div class="actions">
            <button class="secondary" onclick="loadWatchSettings()">Refresh Watch Status</button>
            <button onclick="showStep('install')">Continue</button>
          </div>
          <div id="watchStatus" class="message muted">Sign in to load watch sync status.</div>
        </div>
      </div>

      <div id="install" class="pane">
        <div class="panel">
          <h2>Install Into Vortexo Add-on Server</h2>
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
const stepOrder = ["welcome", "signin", "accounts", "catalogs", "streams", "watch", "install", "finish"];
function showStep(id) {
  document.querySelectorAll(".pane").forEach((el) => el.classList.toggle("active", el.id === id));
  document.querySelectorAll(".step").forEach((el) => el.classList.toggle("active", el.dataset.step === id));
  if (id === "watch") loadWatchSettings();
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
  loadWatchSettings();
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
async function loadWatchSettings() {
  if (!token) return;
  const res = await fetch("/api/v1/bridge/watch/settings", {headers:{authorization:"Bearer " + token}});
  if (!res.ok) { watchStatus.textContent = "Unable to load watch sync settings."; watchStatus.className = "message error"; return; }
  const data = await res.json();
  const trakt = data.trakt || {};
  const state = data.watch_state || {};
  traktClientId.value = trakt.client_id || "";
  watchStatus.innerHTML = "Watch items: <strong>" + escapeHtml(String(state.count || 0)) + "</strong>"
    + " · Trakt token: " + (trakt.has_access_token ? "<span class='ok'>saved</span>" : "<span class='muted'>missing</span>");
  watchStatus.className = "message muted";
  await loadPlexSettings();
}
async function loadPlexSettings() {
  if (!token) return;
  const res = await fetch("/api/v1/bridge/plex/settings", {headers:{authorization:"Bearer " + token}});
  const data = await res.json().catch(() => ({}));
  if (!res.ok) { plexStatus.textContent = data.message || "Unable to load Plex settings."; plexStatus.className = "message error"; return; }
  const plex = data.plex || {};
  const name = plex.username || plex.title || plex.email || "";
  plexStatus.innerHTML = "Plex token: " + (plex.has_access_token ? "<span class='ok'>saved</span>" : "<span class='muted'>missing</span>")
    + (name ? " · Account: <strong>" + escapeHtml(name) + "</strong>" : "");
  plexStatus.className = "message muted";
}
async function savePlexSettings() {
  if (!token) { plexStatus.textContent = "Sign in first."; plexStatus.className = "message error"; showStep("signin"); return; }
  const accessToken = plexAccessToken.value.trim();
  if (!accessToken) { plexStatus.textContent = "Paste a Plex token or use PIN login."; plexStatus.className = "message error"; return; }
  plexStatus.textContent = "Saving Plex token...";
  plexStatus.className = "message muted";
  const res = await fetch("/api/v1/bridge/plex/settings", {method:"POST", headers:{"content-type":"application/json", authorization:"Bearer " + token}, body: JSON.stringify({access_token: accessToken})});
  const data = await res.json().catch(() => ({}));
  if (!res.ok) { plexStatus.textContent = data.message || "Failed to save Plex token."; plexStatus.className = "message error"; return; }
  plexAccessToken.value = "";
  markDone("watch");
  plexStatus.textContent = "Plex connected. Artwork cache will use Discover landscapes on the next refresh.";
  plexStatus.className = "message ok";
  await loadPlexSettings();
}
async function clearPlexSettings() {
  if (!token) { plexStatus.textContent = "Sign in first."; plexStatus.className = "message error"; showStep("signin"); return; }
  const res = await fetch("/api/v1/bridge/plex/settings", {method:"POST", headers:{"content-type":"application/json", authorization:"Bearer " + token}, body: JSON.stringify({clear_token: true})});
  const data = await res.json().catch(() => ({}));
  if (!res.ok) { plexStatus.textContent = data.message || "Failed to clear Plex token."; plexStatus.className = "message error"; return; }
  plexAccessToken.value = "";
  plexDeviceBox.textContent = "";
  plexDeviceBox.className = "message muted";
  plexStatus.textContent = "Plex token cleared. Artwork will fall back to public pages and backdrop-plus-logo cards.";
  plexStatus.className = "message ok";
  await loadPlexSettings();
}
async function startPlexLogin() {
  if (!token) { plexStatus.textContent = "Sign in first."; plexStatus.className = "message error"; showStep("signin"); return; }
  plexDeviceBox.textContent = "Requesting Plex PIN...";
  plexDeviceBox.className = "message muted";
  const res = await fetch("/api/v1/bridge/plex/pin", {method:"POST", headers:{authorization:"Bearer " + token}});
  const data = await res.json().catch(() => ({}));
  if (!res.ok) { plexDeviceBox.textContent = data.message || "Plex PIN login failed."; plexDeviceBox.className = "message error"; return; }
  localStorage.setItem("vortexoPlexPinID", String(data.id || ""));
  const link = data.authorization_url || "https://plex.tv/link";
  plexDeviceBox.innerHTML = "Open <a href='" + escapeAttr(link) + "' target='_blank' rel='noreferrer'>Plex login</a> and enter code <code>" + escapeHtml(data.code || "") + "</code>. Then click Check Login.";
  plexDeviceBox.className = "message ok";
  if (!document.getElementById("checkPlexPinButton")) {
    const btn = document.createElement("button");
    btn.id = "checkPlexPinButton";
    btn.className = "secondary";
    btn.textContent = "Check Login";
    btn.onclick = pollPlexLogin;
    plexDeviceBox.appendChild(document.createElement("br"));
    plexDeviceBox.appendChild(btn);
  }
}
async function pollPlexLogin() {
  const pinID = parseInt(localStorage.getItem("vortexoPlexPinID") || "0", 10);
  if (!pinID) { plexDeviceBox.textContent = "No Plex PIN saved. Start Plex login again."; plexDeviceBox.className = "message error"; return; }
  plexDeviceBox.textContent = "Checking Plex login...";
  plexDeviceBox.className = "message muted";
  const res = await fetch("/api/v1/bridge/plex/pin/token", {method:"POST", headers:{"content-type":"application/json", authorization:"Bearer " + token}, body: JSON.stringify({pin_id: pinID})});
  const data = await res.json().catch(() => ({}));
  if (!res.ok) { plexDeviceBox.textContent = data.message || "Still waiting for Plex approval."; plexDeviceBox.className = "message error"; return; }
  if (!data.authenticated) { plexDeviceBox.textContent = "Still waiting for Plex approval."; plexDeviceBox.className = "message muted"; return; }
  localStorage.removeItem("vortexoPlexPinID");
  plexDeviceBox.textContent = "Plex connected.";
  plexDeviceBox.className = "message ok";
  markDone("watch");
  await loadPlexSettings();
}
async function saveWatchSettings() {
  if (!token) { watchStatus.textContent = "Sign in first."; watchStatus.className = "message error"; showStep("signin"); return; }
  const payload = {
    trakt_client_id: traktClientId.value.trim(),
    trakt_client_secret: traktClientSecret.value.trim(),
    trakt_access_token: traktAccessToken.value.trim(),
    trakt_refresh_token: traktRefreshToken.value.trim()
  };
  const res = await fetch("/api/v1/bridge/watch/settings", {method:"POST", headers:{"content-type":"application/json", authorization:"Bearer " + token}, body: JSON.stringify(payload)});
  const data = await res.json();
  if (!res.ok) { watchStatus.textContent = data.message || "Failed to save watch settings."; watchStatus.className = "message error"; return; }
  traktClientSecret.value = "";
  traktAccessToken.value = "";
  traktRefreshToken.value = "";
  markDone("watch");
  watchStatus.textContent = "Watch sync settings saved.";
  watchStatus.className = "message ok";
  await loadWatchSettings();
}
async function startTraktDeviceLogin() {
  if (!token) { watchStatus.textContent = "Sign in first."; watchStatus.className = "message error"; showStep("signin"); return; }
  await saveWatchSettings();
  traktDeviceBox.textContent = "Requesting Trakt device code...";
  const res = await fetch("/api/v1/bridge/watch/trakt/device-code", {method:"POST", headers:{"content-type":"application/json", authorization:"Bearer " + token}, body: JSON.stringify({client_id: traktClientId.value.trim(), client_secret: traktClientSecret.value.trim()})});
  const data = await res.json();
  if (!res.ok) { traktDeviceBox.textContent = data.message || "Trakt device login failed."; traktDeviceBox.className = "message error"; return; }
  localStorage.setItem("vortexoTraktDeviceCode", data.device_code || "");
  traktDeviceBox.innerHTML = "Open <a href='" + escapeAttr(data.verification_url || "https://trakt.tv/activate") + "' target='_blank' rel='noreferrer'>Trakt activation</a> and enter code <code>" + escapeHtml(data.user_code || "") + "</code>. Then click Check Login.";
  traktDeviceBox.className = "message ok";
  if (!document.getElementById("checkTraktDeviceButton")) {
    const btn = document.createElement("button");
    btn.id = "checkTraktDeviceButton";
    btn.className = "secondary";
    btn.textContent = "Check Login";
    btn.onclick = pollTraktDeviceLogin;
    traktDeviceBox.appendChild(document.createElement("br"));
    traktDeviceBox.appendChild(btn);
  }
}
async function pollTraktDeviceLogin() {
  const deviceCode = localStorage.getItem("vortexoTraktDeviceCode") || "";
  if (!deviceCode) { traktDeviceBox.textContent = "No device code saved. Start device login again."; traktDeviceBox.className = "message error"; return; }
  traktDeviceBox.textContent = "Checking Trakt login...";
  const res = await fetch("/api/v1/bridge/watch/trakt/device-token", {method:"POST", headers:{"content-type":"application/json", authorization:"Bearer " + token}, body: JSON.stringify({device_code: deviceCode})});
  const data = await res.json();
  if (!res.ok) { traktDeviceBox.textContent = data.message || "Still waiting for Trakt approval."; traktDeviceBox.className = "message error"; return; }
  localStorage.removeItem("vortexoTraktDeviceCode");
  traktDeviceBox.textContent = "Trakt connected.";
  traktDeviceBox.className = "message ok";
  await loadWatchSettings();
}
async function syncTraktWatch() {
  if (!token) { watchStatus.textContent = "Sign in first."; watchStatus.className = "message error"; showStep("signin"); return; }
  watchStatus.textContent = "Syncing Trakt watch history...";
  watchStatus.className = "message muted";
  const res = await fetch("/api/v1/bridge/watch/trakt/sync", {method:"POST", headers:{authorization:"Bearer " + token}});
  const data = await res.json();
  if (!res.ok) { watchStatus.textContent = data.message || "Trakt sync failed."; watchStatus.className = "message error"; return; }
  markDone("watch");
  watchStatus.textContent = "Trakt sync imported " + (data.imported || 0) + " items. Total watch items: " + (data.total || 0) + ".";
  watchStatus.className = "message ok";
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
