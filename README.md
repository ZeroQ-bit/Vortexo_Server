# Vortexo Manifest Server

A clean Vortexo backend for Apple TV that starts empty and becomes useful when
you install Stremio-compatible manifests.

The Apple TV app keeps using the existing Vortexo Server settings:

- `GET /api/v1/vortexo/home`
- `POST /api/v1/vortexo/sources`
- `GET /api/v1/vortexo/play/{token}`

Install an AIOMetadata-style manifest for catalog rows and an AIOStreams-style
manifest for source lookup. The server hides manifest/provider details from the
Apple TV app and exposes only the Vortexo API.

## First Run Wizard

Open the server in a browser to use the setup wizard. It walks through:

- Signing in with the bridge credentials
- Preparing debrid, TMDB, TVDB, Gemini, and RPDB account requirements
- Generating an AIOMetadata catalog manifest from Vortexo's compact preset
- Generating an AIOStreams source manifest from Vortexo's compact preset
- Pasting existing manifest URLs manually when you want a custom external setup
- Installing those manifests into Vortexo Bridge
- Connecting the Vortexo Apple TV app to the same server URL

Vortexo Bridge stores installed manifest URLs. Third-party keys are sent to the
selected upstream addon instances only to create their normal manifest
configuration.

The setup endpoint is available at:

- `POST /api/v1/bridge/perfect-setup`

## Local Run

```bash
go run .
```

Default credentials:

- Username: `vortexo`
- Password: `vortexo`

Override with:

```bash
VORTEXO_ADMIN_USERNAME=myuser VORTEXO_ADMIN_PASSWORD=mypass go run .
```

Persistent data is stored in `VORTEXO_DATA_DIR`, defaulting to `/data` in
Docker and `./data` if supplied locally.
