package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestStructuredPlexArtworkKeepsLandscapeTileSeparateFromBackground(t *testing.T) {
	html := `
		<script>
			{"orientation":"landscape","size":"m","id":"tv.plex.provider.discover-123/extras/456","image":{"url":"https:\/\/metadata-static.plex.tv\/extras\/iva\/123\/landscape.jpg"}}
			{"backgroundLandscape":{"image":{"url":"https:\/\/metadata-static.plex.tv\/b\/gracenote\/background.jpg"}}}
			{"clearLogo":{"url":"https:\/\/metadata-static.plex.tv\/logo.png"}}
		</script>
	`

	artwork := structuredPlexArtwork(html)
	if len(artwork.Landscape) != 1 || artwork.Landscape[0] != "https://metadata-static.plex.tv/extras/iva/123/landscape.jpg" {
		t.Fatalf("expected extras landscape tile, got %#v", artwork.Landscape)
	}
	if len(artwork.CoverArt) != 1 || artwork.CoverArt[0] != artwork.Landscape[0] {
		t.Fatalf("expected coverArt to mirror landscape tile, got %#v", artwork.CoverArt)
	}
	if len(artwork.Background) != 1 || artwork.Background[0] != "https://metadata-static.plex.tv/b/gracenote/background.jpg" {
		t.Fatalf("expected backgroundLandscape to stay background-only, got %#v", artwork.Background)
	}
	if len(artwork.ClearLogo) != 1 || artwork.ClearLogo[0] != "https://metadata-static.plex.tv/logo.png" {
		t.Fatalf("expected clear logo, got %#v", artwork.ClearLogo)
	}
}

func TestCachedPlexArtworkAppliesShowLandscapeToEpisodeWatchState(t *testing.T) {
	state := &appState{
		plexArtwork: map[string]plexArtworkCacheRecord{},
	}
	record := plexArtworkCacheRecord{
		plexArtworkEntry: plexArtworkEntry{
			Version:   1,
			MediaType: "tv",
			TMDBID:    12345,
			Title:     "Ghosts",
			Artwork: plexArtwork{
				Landscape:  []string{"https://metadata-static.plex.tv/extras/iva/ghosts/landscape.jpg"},
				Background: []string{"https://metadata-static.plex.tv/g/gracenote/ghosts-background.jpg"},
				ClearLogo:  []string{"https://metadata-static.plex.tv/ghosts-logo.png"},
			},
		},
		Status: "ok",
	}
	state.plexArtwork[plexArtworkKey(record.MediaType, record.TMDBID, record.IMDBID, record.Title, record.Year)] = record

	item := watchStateItem{
		MediaType:     "episode",
		ParentTitle:   "Ghosts",
		Title:         "Episode",
		TMDBID:        12345,
		Season:        5,
		Episode:       17,
		LandscapePath: "https://i.ytimg.com/vi/fallback/hq720.jpg",
		BackdropPath:  "https://image.tmdb.org/t/p/original/background.jpg",
	}

	state.applyCachedPlexArtworkToWatchStateItem(&item)

	if item.LandscapePath != record.Artwork.Landscape[0] {
		t.Fatalf("expected cached Plex landscape, got %q", item.LandscapePath)
	}
	if item.BackdropPath != record.Artwork.Background[0] {
		t.Fatalf("expected cached Plex background, got %q", item.BackdropPath)
	}
	if item.LogoPath != record.Artwork.ClearLogo[0] {
		t.Fatalf("expected cached Plex logo, got %q", item.LogoPath)
	}
}

func TestCachedPlexArtworkFindsTMDBKeyedRecordByIMDbHomeItem(t *testing.T) {
	state := &appState{
		plexArtwork: map[string]plexArtworkCacheRecord{},
	}
	record := plexArtworkCacheRecord{
		plexArtworkEntry: plexArtworkEntry{
			Version:   1,
			MediaType: "movie",
			TMDBID:    12345,
			IMDBID:    "tt1234567",
			Title:     "Keyed By TMDB",
			Artwork: plexArtwork{
				Landscape: []string{"https://metadata-static.plex.tv/extras/iva/keyed/landscape.jpg"},
			},
		},
		Status: "ok",
	}
	state.plexArtwork[plexArtworkKey(record.MediaType, record.TMDBID, record.IMDBID, record.Title, record.Year)] = record

	item := vortexoHomeItem{
		MediaType: "movie",
		IMDBID:    "tt1234567",
		Title:     "Different Catalog Title",
	}

	state.applyCachedPlexArtworkToHomeItem(&item)

	if item.LandscapePath != record.Artwork.Landscape[0] {
		t.Fatalf("expected IMDb home item to find TMDB-keyed landscape, got %q", item.LandscapePath)
	}
}

func TestDiscoverMetadataArtworkKeepsTrailersOutOfLandscape(t *testing.T) {
	metadata := plexDiscoverMetadata{
		Title: "Ghosts",
		Type:  "show",
		Image: []plexDiscoverImage{
			{Type: "coverArt", URL: "https://metadata-static.plex.tv/extras/iva/ghosts/landscape.jpg"},
			{Type: "coverArt", URL: "https://i.ytimg.com/vi/trailer/hq720.jpg"},
			{Type: "coverArt", URL: "https://images.plex.tv/photo?url=https%3A%2F%2Fi.ytimg.com%2Fvi%2Ftrailer%2Fhq720.jpg"},
			{Type: "background", URL: "https://metadata-static.plex.tv/b/gracenote/ghosts-background.jpg"},
			{Type: "clearLogo", URL: "/photo/:/transcode?url=https%3A%2F%2Fmetadata-static.plex.tv%2Fghosts-logo.png"},
		},
	}

	artwork := plexArtworkFromDiscoverMetadata(metadata, "plex-token")
	if len(artwork.Landscape) != 1 || artwork.Landscape[0] != "https://metadata-static.plex.tv/extras/iva/ghosts/landscape.jpg" {
		t.Fatalf("expected only proper Discover landscape, got %#v", artwork.Landscape)
	}
	if len(artwork.CoverArt) != 1 || artwork.CoverArt[0] != artwork.Landscape[0] {
		t.Fatalf("expected coverArt to mirror proper landscape, got %#v", artwork.CoverArt)
	}
	if len(artwork.Background) != 1 || artwork.Background[0] != "https://metadata-static.plex.tv/b/gracenote/ghosts-background.jpg" {
		t.Fatalf("expected backdrop to stay background, got %#v", artwork.Background)
	}
	if len(artwork.ClearLogo) != 1 {
		t.Fatalf("expected normalized Discover clearLogo, got %#v", artwork.ClearLogo)
	}
}

func TestDiscoverMetadataArtworkUsesImageSetAndSnapshotAsLandscape(t *testing.T) {
	metadata := plexDiscoverMetadata{
		Title: "The Boroughs",
		Type:  "show",
		Image: []plexDiscoverImage{
			{Type: "snapshot", URL: "https://metadata-static.plex.tv/snapshot.jpg"},
		},
		Images: plexDiscoverImageSet{
			CoverArt:            "https://metadata-static.plex.tv/cover-art.jpg",
			BackgroundLandscape: "https://metadata-static.plex.tv/background.jpg",
			ClearLogo:           "/photo/:/transcode?url=https%3A%2F%2Fmetadata-static.plex.tv%2Flogo.png",
			CoverPoster:         "https://metadata-static.plex.tv/poster.jpg",
		},
	}

	artwork := plexArtworkFromDiscoverMetadata(metadata, "plex-token")
	if len(artwork.Landscape) != 2 {
		t.Fatalf("expected snapshot and images.coverArt as clean landscapes, got %#v", artwork.Landscape)
	}
	if artwork.Landscape[0] != "https://metadata-static.plex.tv/snapshot.jpg" ||
		artwork.Landscape[1] != "https://metadata-static.plex.tv/cover-art.jpg" {
		t.Fatalf("unexpected landscape order: %#v", artwork.Landscape)
	}
	if len(artwork.Background) != 1 || artwork.Background[0] != "https://metadata-static.plex.tv/background.jpg" {
		t.Fatalf("expected backgroundLandscape as backdrop, got %#v", artwork.Background)
	}
	if len(artwork.ClearLogo) != 1 || len(artwork.Thumbnail) != 1 {
		t.Fatalf("expected logo and poster images, got logo=%#v thumbnail=%#v", artwork.ClearLogo, artwork.Thumbnail)
	}
}

func TestDiscoverImageSetDecodesNestedValues(t *testing.T) {
	var metadata plexDiscoverMetadata
	err := json.Unmarshal([]byte(`{
		"title": "The Boroughs",
		"images": {
			"coverArt": {"image": {"url": "https://metadata-static.plex.tv/cover-art.jpg"}},
			"snapshot": [{"url": "https://metadata-static.plex.tv/snapshot.jpg"}],
			"clearLogo": "https://metadata-static.plex.tv/logo.png"
		}
	}`), &metadata)
	if err != nil {
		t.Fatalf("expected nested image object to decode, got %v", err)
	}
	if metadata.Images.CoverArt != "https://metadata-static.plex.tv/cover-art.jpg" {
		t.Fatalf("expected nested coverArt URL, got %q", metadata.Images.CoverArt)
	}
	if metadata.Images.Snapshot != "https://metadata-static.plex.tv/snapshot.jpg" {
		t.Fatalf("expected array snapshot URL, got %q", metadata.Images.Snapshot)
	}
	if metadata.Images.ClearLogo != "https://metadata-static.plex.tv/logo.png" {
		t.Fatalf("expected string clearLogo URL, got %q", metadata.Images.ClearLogo)
	}
}

func TestBestPlexDiscoverSearchMatchAllowsExactTitleWhenIDsAreMissing(t *testing.T) {
	results := []plexDiscoverMetadata{
		{
			Type:  "show",
			Title: "The Boroughs",
			Year:  2026,
			GUID:  "plex://show/abc123",
		},
	}
	item := plexArtworkSeedItem{
		MediaType: "tv",
		TMDBID:    224941,
		Title:     "The Boroughs",
		Year:      2026,
	}

	match := bestPlexDiscoverSearchMatch(results, item)
	if match == nil || match.Title != "The Boroughs" {
		t.Fatalf("expected exact title/year match without external IDs, got %#v", match)
	}
}

func TestPlexArtworkSeedNeedsRefreshRepairsBackdropOnlyWithToken(t *testing.T) {
	now := time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)
	state := &appState{
		config: bridgeConfig{
			Plex: plexAuthConfig{
				AccessToken:    "token",
				LastSignedInAt: now.Add(-2 * time.Hour),
			},
		},
		plexArtwork: map[string]plexArtworkCacheRecord{},
	}
	record := plexArtworkCacheRecord{
		plexArtworkEntry: plexArtworkEntry{
			Version:    1,
			MediaType:  "tv",
			TMDBID:     91371,
			Title:      "The UnXplained",
			SourcePage: "https://watch.plex.tv/en-GB/show/the-unxplained",
			Artwork: plexArtwork{
				Background: []string{"https://metadata-static.plex.tv/background.jpg"},
			},
		},
		Status:    "ok",
		FetchedAt: now.Add(-2 * plexArtworkIncompleteRetryAfter),
	}
	state.plexArtwork[plexArtworkKey(record.MediaType, record.TMDBID, record.IMDBID, record.Title, record.Year)] = record

	seed := plexArtworkSeedItem{MediaType: "tv", TMDBID: 91371, Title: "The UnXplained"}
	if !state.plexArtworkSeedNeedsRefresh(seed, now.Add(-plexArtworkStaleAfter), false, now) {
		t.Fatalf("expected stale backdrop-only public artwork to be rechecked with Plex token")
	}
}
