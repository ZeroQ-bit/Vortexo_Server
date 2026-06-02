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
