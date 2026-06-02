# Vortexo Server

Vortexo Server is a self-hosted companion backend for the Vortexo Apple TV app.

It provides a simple first-run setup wizard that lets users install and manage manifest URLs for catalog metadata and playback sources. Vortexo Apple TV can then connect to this server and use those manifests for Home rows, metadata, and stream/source lookup.

## What it does

Vortexo Server helps turn a clean server into a working Vortexo backend.

It supports:

- AIOMetadata-style catalog manifests
- AIOStreams-style stream manifests
- Debrid-backed playback sources
- Real-Debrid, TorBox, Premiumize, AllDebrid, and other providers supported by your AIOStreams instance
- Optional TMDB, TVDB, Gemini, and RPDB configuration
- Easy Apple TV connection using one server URL
- Installed manifest management

## How it works

1. Sign in to Vortexo Server.
2. Prepare your accounts and optional API keys.
3. Create or paste your catalog manifest URL.
4. Create or paste your stream manifest URL.
5. Install the manifests into Vortexo Server.
6. Open Vortexo Apple TV.
7. Go to Settings → Servers.
8. Enable Vortexo Server and connect using your server URL.

## Privacy

Vortexo Server stores only the installed manifest URLs.

Debrid, TMDB, TVDB, Gemini, and RPDB keys stay inside the upstream addon configurations you create. Vortexo Server does not need to store those keys directly.

## Catalogs

Catalog manifests are used by Vortexo Apple TV to create landscape Home rows and metadata-driven browsing sections.

## Playback

Stream manifests are used by Vortexo Apple TV for source lookup when opening movies and episodes.

If a manifest returns only torrent hashes, those sources are skipped until a debrid-backed playable URL is returned.

## Project status

This project is in early development. Features, setup steps, and behavior may change as Vortexo Server improves.
