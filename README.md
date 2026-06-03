# Vortexo Manifest Server

Vortexo Manifest Server is the self-hosted companion server for the Vortexo Apple TV app. It lets you install Stremio-compatible manifest URLs on your own server, then Vortexo reads the server as a single configured source.

The server stores your installed manifest URLs and local configuration. It does not bundle add-ons, content, debrid accounts, fallback API keys, or paid service credentials.

## What It Provides

- Manifest-based catalog rows for Vortexo Home.
- Movie, show, season, and episode metadata from installed catalog manifests.
- Stream lookup from installed stream manifests.
- Optional subtitle lookups from installed subtitle manifests.
- Optional Live TV manifests when supported by the installed add-on.
- Optional watch state and Up Next rows from Trakt and Plex imports.
- A browser setup wizard for installing and managing manifests.

Vortexo Pro is required in the Apple TV app to use manifest-server features.

## Quick Start With Docker Compose

Clone the repo:

```bash
git clone https://github.com/ZeroQ-bit/Vortexo-Manifest-Server.git
cd Vortexo-Manifest-Server
```

Create your local environment file:

```bash
cp .env.example .env
```

Edit `.env` and choose a password:

```env
VORTEXO_ADMIN_USERNAME=vortexo
VORTEXO_ADMIN_PASSWORD=change-this-password
```

Start the server:

```bash
docker compose up -d --build
```

Open the dashboard:

```text
http://<your-server-ip>:18456
```

For example:

```text
http://192.168.1.63:18456
```

Do not use `localhost` in the Apple TV app unless the server is actually running on the Apple TV simulator host. For a real Apple TV, use the LAN IP address of the machine running Docker.

## Apple TV Setup

1. Open the Vortexo Manifest Server dashboard in a browser.
2. Sign in with the admin username and password from `.env`.
3. Install your catalog, stream, subtitle, Live TV, Trakt, or Plex manifests.
4. Open Vortexo on Apple TV.
5. Go to `Settings` > `Vortexo Server`.
6. Set the server URL to:

```text
http://<your-server-ip>:18456
```

7. Enter the same dashboard username and password.
8. Select `Connect Vortexo Server`.

After this, Vortexo can load manifest-powered Home rows, metadata, streams, subtitles, Live TV, and Continue Watching where those features are available from your installed manifests and account connections.

## Docker Run

If you do not want to use Compose:

```bash
docker build -t vortexo-manifest-server:latest .
docker run -d \
  --name vortexo-manifest-server \
  --restart unless-stopped \
  -p 18456:8080 \
  -e VORTEXO_ADMIN_USERNAME=vortexo \
  -e VORTEXO_ADMIN_PASSWORD=change-this-password \
  -v "$(pwd)/data:/data" \
  vortexo-manifest-server:latest
```

Open:

```text
http://<your-server-ip>:18456
```

## Updating

```bash
git pull
docker compose up -d --build
```

Your installed manifests and settings are stored in `./data`, so normal rebuilds keep your setup.

## Data And Backups

The container stores persistent data in:

```text
./data
```

Back up this folder if you want to keep installed manifests, credentials, watch state, and setup progress.

## Environment Variables

| Variable | Default | Description |
| --- | --- | --- |
| `VORTEXO_LISTEN_ADDR` | `:8080` | Address and port used inside the container. |
| `VORTEXO_DATA_DIR` | `/data` | Directory for persistent server data. |
| `VORTEXO_ADMIN_USERNAME` | `vortexo` | Admin username used on first run. |
| `VORTEXO_ADMIN_PASSWORD` | `vortexo` | Admin password used on first run. |
| `PORT` | unset | Alternative port variable. Used only if `VORTEXO_LISTEN_ADDR` is not set. |
| `DATA_DIR` | unset | Alternative data directory variable. Used only if `VORTEXO_DATA_DIR` is not set. |

The admin username and password environment variables seed the first saved configuration. If you already started the server once, changing `.env` may not change the saved admin login. To reset the server, stop the container and remove or edit the saved data in `./data`.

## Health Check

The Docker image includes a health check:

```text
http://127.0.0.1:8080/api/v1/health
```

From another machine on your LAN, use:

```text
http://<your-server-ip>:18456/api/v1/health
```

## Troubleshooting

If the browser says connection refused, check that the container is running:

```bash
docker compose ps
docker compose logs -f
```

If Apple TV cannot connect, confirm the server URL uses the Docker host LAN IP and port `18456`.

If Home rows are empty, sign in to the dashboard and install at least one catalog manifest. Stream playback also needs a stream manifest.

If streams are visible but locked in Vortexo, make sure Vortexo Pro is active in the Apple TV app.

If credentials do not change after editing `.env`, remember that the environment variables are only used for the first saved configuration.

## Security

Run this server on a trusted network. Choose a strong admin password. Do not expose it directly to the public internet unless you understand how to put it behind HTTPS, authentication, and a secure reverse proxy.
