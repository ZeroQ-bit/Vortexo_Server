import React, { useEffect, useMemo, useState, useCallback } from "react";
import { createRoot } from "react-dom/client";
import { ConfigProvider, useConfig } from "./ConfigContext";
import {
  Activity,
  BadgeCheck,
  ChevronDown,
  ChevronUp,
  CircleAlert,
  Clapperboard,
  Database,
  ExternalLink,
  Eye,
  Globe2,
  LayoutDashboard,
  Library,
  ListChecks,
  Lock,
  Play,
  Radio,
  RefreshCw,
  Search,
  Server,
  Settings,
  ShieldCheck,
  Sparkles,
  Trash2,
} from "lucide-react";
import "./styles.css";

const DEFAULT_REGISTRY_URL = "https://stremio-addons.net/api/manifest.json";
const STREAMING_CATALOG_PROVIDERS = [
  ["nfx", "Netflix"],
  ["hbm", "HBO Max"],
  ["dnp", "Disney+"],
  ["amp", "Prime Video"],
  ["atp", "Apple TV+"],
  ["pmp", "Paramount+"],
  ["pcp", "Peacock"],
  ["hlu", "Hulu"],
  ["nfk", "Netflix Kids"],
  ["cru", "Crunchyroll"],
  ["jhs", "JioHotstar"],
  ["zee", "Zee5"],
  ["mgl", "MagellanTV"],
  ["cts", "Curiosity Stream"],
  ["mbi", "Mubi"],
  ["shd", "Shudder"],
  ["bbo", "BritBox"],
  ["act", "Acorn TV"],
  ["itv", "ITVX"],
  ["bbc", "BBC iPlayer"],
  ["al4", "Channel 4"],
];
const STREAMING_CATALOG_TYPES = [
  ["movie", "Movies"],
  ["series", "Shows"],
];
const STREAMING_CATALOG_SORTS = [
  ["TRENDING", "Trending"],
  ["POPULAR", "Popular"],
  ["NEW", "New"],
];

const NAV = [
  { id: "overview", label: "Overview", icon: LayoutDashboard },
  { id: "discover", label: "Discover", icon: Search },
  { id: "addons", label: "Add-ons", icon: Sparkles },
  { id: "catalogs", label: "Catalogs", icon: Library },
  { id: "live", label: "Live TV", icon: Radio },
  { id: "setup", label: "Setup", icon: ListChecks },
  { id: "watch", label: "Watch Sync", icon: Activity },
  { id: "settings", label: "Settings", icon: Settings },
];

function AppContent() {
    const {
      token,
      setToken,
      signedIn,
      serverUrl,
    view,
    setView,
    message,
    setMessage,
    busy,
    setBusy,
    dashboard,
    setDashboard,
    resetDashboard,
    summary,
    login,
    setLogin,
    registry,
    setRegistry,
    streamingCatalogs,
    setStreamingCatalogs,
    keywordRows,
    setKeywordRows,
    watchForm,
    setWatchForm,
    homeRows,
    setHomeRows,
    liveRows,
    setLiveRows,
    registryAddons,
    setRegistryAddons,
    plexSettings,
    setPlexSettings,
    plexPin,
    setPlexPin,
    plexAccessToken,
    setPlexAccessToken,
    watchStatus,
    setWatchStatus,
    registryLoading,
    setRegistryLoading,
    plexStatus,
    setPlexStatus,
  } = useConfig();

  const [manual, setManual] = useState({ name: "", url: "" });
  const [traktDevice, setTraktDevice] = useState({
    code: "",
    userCode: "",
    verificationUrl: "https://trakt.tv/activate",
  });
  const keywordRowsStatus = dashboard.tmdb_keyword_rows || {};

  // ============ OPTIMIZED REQUEST FUNCTION ============
  const request = useCallback(
    async (path, options = {}) => {
      const headers = { ...(options.headers || {}) };
      if (options.body && !headers["content-type"])
        headers["content-type"] = "application/json";
      if (token) headers.authorization = `Bearer ${token}`;
      const res = await fetch(path, { ...options, headers });
      let data = {};
      try {
        data = await res.json();
      } catch {
        data = {};
      }
      if (!res.ok) {
        throw new Error(data.message || data.error || `HTTP ${res.status}`);
      }
      return data;
    },
    [token]
  );

  // ============ MEMOIZED DATA LOADERS ============
  const loadPublicHome = useCallback(async () => {
    try {
      const data = await fetch(
        "/api/v1/vortexo/home?row_limit=8&item_limit=12"
      ).then((res) => res.json());
      setHomeRows(data.rows || []);
    } catch {
      setHomeRows([]);
    }
  }, []);

  const loadLiveRows = useCallback(async () => {
    try {
      const data = await fetch("/api/v1/vortexo/live-tv/rows?limit=80").then(
        (res) => res.json()
      );
      setLiveRows(data.rows || []);
    } catch {
      setLiveRows([]);
    }
  }, []);

  const loadDashboard = useCallback(
    async (activeToken = token) => {
      if (!activeToken) return;
      setBusy(true);
      try {
        const res = await fetch("/api/v1/bridge/dashboard", {
          headers: { authorization: `Bearer ${activeToken}` },
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.message || "Dashboard failed");
        setDashboard((prev) => ({ ...prev, ...data }));
      } catch (error) {
        setMessage(error.message);
      } finally {
        setBusy(false);
      }
    },
    [token, setMessage]
  );

  const loadWatchSettings = useCallback(
    async (activeToken = token) => {
      if (!activeToken) return;
      try {
        const res = await fetch("/api/v1/bridge/watch/settings", {
          headers: { authorization: `Bearer ${activeToken}` },
        });
        const data = await res.json();
        if (!res.ok) return;
        setWatchForm((current) => ({
          ...current,
          traktClientId: data.trakt?.client_id || "",
        }));
        const watchState = data.watch_state || {};
        const watchCount = watchState.count || 0;
        setWatchStatus(
          `Watch items: ${watchCount} · Trakt token: ${
            data.trakt?.has_access_token ? "saved" : "missing"
          }`
        );
      } catch {
        // Optional panel; keep the dashboard usable.
      }
    },
    [token]
  );

  const loadPlexSettings = useCallback(
    async (activeToken = token) => {
      if (!activeToken) return;
      try {
        const res = await fetch("/api/v1/bridge/plex/settings", {
          headers: { authorization: `Bearer ${activeToken}` },
        });
        const data = await res.json();
        if (!res.ok) return;
        setPlexSettings(data.plex || {});
      } catch {
        // Optional panel; keep the dashboard usable.
      }
    },
    [token]
  );

  // ============ PARALLEL DATA LOADING (Optimization #1) ============
  const loadInitialData = useCallback(
    async (activeToken) => {
      if (!activeToken) return;
      try {
        // Load all in parallel - much faster than sequential
        await Promise.all([
          loadDashboard(activeToken),
          loadPublicHome(),
          loadLiveRows(),
          loadWatchSettings(activeToken),
          loadPlexSettings(activeToken),
        ]);
      } catch (error) {
        console.error("Initial data load failed:", error);
      }
    },
    [loadDashboard, loadPublicHome, loadLiveRows, loadWatchSettings, loadPlexSettings]
  );

  const loadRegistry = useCallback(async () => {
    if (!token) return;
    setRegistryLoading(true);
    try {
      const params = new URLSearchParams();
      params.set("limit", "120");
      if (registry.url) params.set("registry_url", registry.url);
      if (registry.q.trim()) params.set("q", registry.q.trim());
      if (registry.capability !== "all")
        params.set("capability", registry.capability);
      if (registry.type !== "all") params.set("type", registry.type);
      const data = await request(
        `/api/v1/bridge/addon-registry?${params.toString()}`
      );
      setRegistryAddons(data.addons || []);
      setRegistry((current) => ({
        ...current,
        url: data.registry_url || current.url,
      }));
      setMessage("");
    } catch (error) {
      setMessage(error.message);
      setRegistryAddons([]);
    } finally {
      setRegistryLoading(false);
    }
  }, [token, registry.url, registry.q, registry.capability, registry.type, request, setMessage]);

  // ============ EFFECT: Load data on token change ============
  useEffect(() => {
    if (token) {
      loadInitialData(token);
    } else {
      resetDashboard();
    }
  }, [token, loadInitialData, resetDashboard]);

  // ============ EFFECT: Update registry URL when dashboard changes ============
  useEffect(() => {
    setRegistry((current) => ({
      ...current,
      url: dashboard.registry_url || current.url || DEFAULT_REGISTRY_URL,
    }));
  }, [dashboard.registry_url]);

  // ============ EFFECT: Load keyword rows config ============
  useEffect(() => {
    const config = dashboard.tmdb_keyword_rows;
    if (!config) return;
    setKeywordRows((current) => ({
      ...current,
      enabled: Boolean(config.enabled),
      rowCount: config.row_count || current.rowCount || 10,
      language: config.language || current.language || "en-US",
      region: config.region || current.region || "US",
    }));
  }, [dashboard.tmdb_keyword_rows]);

  // ============ EFFECT: Lazy load registry and live rows (Optimization #2) ============
  useEffect(() => {
    if (
      signedIn &&
      view === "discover" &&
      registryAddons.length === 0 &&
      !registryLoading
    ) {
      loadRegistry();
    }
    if (view === "live") {
      loadLiveRows();
    }
  }, [signedIn, view, registryAddons.length, registryLoading, loadRegistry, loadLiveRows]);

  // ============ MEMOIZED HANDLERS ============
  const signIn = useCallback(
    async (event) => {
      event.preventDefault();
      setBusy(true);
      try {
        const data = await fetch("/api/v1/auth/login", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            username: login.username,
            password: login.password,
          }),
        }).then(async (res) => {
          const body = await res.json();
          if (!res.ok) throw new Error(body.message || "Sign in failed");
          return body;
        });
        const nextToken = data.token || data.access_token;
        setToken(nextToken);
        setMessage("Signed in");
        setView("overview");
      } catch (error) {
        setMessage(error.message);
      } finally {
        setBusy(false);
      }
    },
    [login.username, login.password, setMessage]
  );

  const signOut = useCallback(() => {
    setToken("");
    setMessage("Signed out");
  }, [setMessage]);

  const installManifest = useCallback(
    async (event) => {
      event.preventDefault();
      if (!manual.url.trim()) {
        setMessage("Paste a manifest URL first.");
        return;
      }
      await installManifestURL(
        manual.url.trim(),
        manual.name.trim(),
        "Manifest installed"
      );
      setManual({ name: "", url: "" });
      setView("addons");
    },
    [manual.url, manual.name, setMessage]
  );

  const installManifestURL = useCallback(
    async (url, name, successMessage = "Add-on installed") => {
      setBusy(true);
      try {
        await request("/api/v1/bridge/manifests", {
          method: "POST",
          body: JSON.stringify({ name, url, enabled: true }),
        });
        setMessage(successMessage);
        // Parallel updates
        await Promise.all([
          loadDashboard(),
          loadPublicHome(),
          loadLiveRows(),
          view === "discover" ? loadRegistry() : Promise.resolve(),
        ]);
      } catch (error) {
        setMessage(error.message);
      } finally {
        setBusy(false);
      }
    },
    [request, setMessage, loadDashboard, loadPublicHome, loadLiveRows, loadRegistry, view]
  );

  const updateManifest = useCallback(
    async (id, patch) => {
      setBusy(true);
      try {
        await request(`/api/v1/bridge/manifests/${encodeURIComponent(id)}`, {
          method: "PUT",
          body: JSON.stringify(patch),
        });
        setMessage("Add-on updated");
        await Promise.all([
          loadDashboard(),
          loadPublicHome(),
          loadLiveRows(),
        ]);
      } catch (error) {
        setMessage(error.message);
      } finally {
        setBusy(false);
      }
    },
    [request, setMessage, loadDashboard, loadPublicHome, loadLiveRows]
  );

  const removeManifest = useCallback(
    async (id) => {
      setBusy(true);
      try {
        await request(`/api/v1/bridge/manifests/${encodeURIComponent(id)}`, {
          method: "DELETE",
        });
        setMessage("Add-on removed");
        await Promise.all([
          loadDashboard(),
          loadPublicHome(),
          loadLiveRows(),
          view === "discover" ? loadRegistry() : Promise.resolve(),
        ]);
      } catch (error) {
        setMessage(error.message);
      } finally {
        setBusy(false);
      }
    },
    [request, setMessage, loadDashboard, loadPublicHome, loadLiveRows, loadRegistry, view]
  );

  const copyServerURL = useCallback(async () => {
    await navigator.clipboard?.writeText(serverUrl);
    setMessage("Server URL copied");
  }, [serverUrl, setMessage]);

  const refresh = useCallback(() => {
    loadDashboard();
    loadPublicHome();
    loadLiveRows();
  }, [loadDashboard, loadPublicHome, loadLiveRows]);

  const saveStreamingCatalogs = useCallback(
    async () => {
      if (!token) {
        setMessage("Sign in to save streaming catalog settings.");
        return;
      }
      if (streamingCatalogs.providers.length === 0) {
        setMessage("Select at least one streaming provider.");
        return;
      }
      if (streamingCatalogs.types.length === 0) {
        setMessage("Select at least one catalog type.");
        return;
      }
      setBusy(true);
      try {
        await request("/api/v1/bridge/streaming-catalogs", {
          method: "POST",
          body: JSON.stringify({
            install: true,
            providers: streamingCatalogs.providers,
            types: streamingCatalogs.types,
            merge_providers: streamingCatalogs.mergeProviders,
            merge_all: streamingCatalogs.mergeAll,
            sort_by: streamingCatalogs.sortBy,
            rpdb_key: streamingCatalogs.rpdbKey,
          }),
        });
        setMessage("Streaming catalogs updated.");
        await Promise.all([loadDashboard(), loadPublicHome(), loadLiveRows()]);
      } catch (error) {
        setMessage(error.message);
      } finally {
        setBusy(false);
      }
    },
    [
      token,
      streamingCatalogs,
      request,
      setMessage,
      loadDashboard,
      loadPublicHome,
      loadLiveRows,
    ]
  );

  const saveKeywordRows = useCallback(
    async () => {
      if (!token) {
        setMessage("Sign in to save keyword row settings.");
        return;
      }

      const maxRows = Math.max(1, Number(keywordRowsStatus?.max_row_count || 50));
      const rowCount = Math.max(
        1,
        Math.min(maxRows, Number(keywordRows.rowCount || keywordRowsStatus?.default_row_count || 10))
      );
      const payload = {
        enabled: Boolean(keywordRows.enabled),
        row_count: rowCount,
        tmdb_api_key: (keywordRows.tmdbKey || "").trim(),
        tmdb_access_token: (keywordRows.tmdbToken || "").trim(),
        language: (keywordRows.language || "en-US").trim(),
        region: (keywordRows.region || "US").trim().toUpperCase(),
        clear_credentials: Boolean(keywordRows.clearCredentials),
      };

      if (
        payload.enabled &&
        !payload.tmdb_api_key &&
        !payload.tmdb_access_token &&
        !keywordRowsStatus?.has_api_key &&
        !keywordRowsStatus?.has_access_token
      ) {
        setMessage("TMDB API key or access token is required before enabling keyword rows.");
        return;
      }

      setBusy(true);
      try {
        await request("/api/v1/bridge/tmdb-keyword-rows", {
          method: "POST",
          body: JSON.stringify(payload),
        });
        setMessage(
          payload.enabled
            ? "Keyword row settings saved."
            : "Keyword rows disabled and no longer added to home."
        );
        setKeywordRows((current) => ({
          ...current,
          rowCount,
          clearCredentials: false,
        }));
        await Promise.all([loadDashboard(), loadPublicHome()]);
      } catch (error) {
        setMessage(error.message);
      } finally {
        setBusy(false);
      }
    },
    [
      token,
      keywordRows,
      keywordRowsStatus,
      setKeywordRows,
      request,
      setMessage,
      loadDashboard,
      loadPublicHome,
    ]
  );

  const saveWatchSettings = useCallback(
    async () => {
      if (!token) {
        setWatchStatus("Sign in first.");
        return;
      }
      setBusy(true);
      try {
        await request("/api/v1/bridge/watch/settings", {
          method: "POST",
          body: JSON.stringify({
            trakt_client_id: watchForm.traktClientId,
            trakt_client_secret: watchForm.traktClientSecret,
            trakt_access_token: watchForm.traktAccessToken,
            trakt_refresh_token: watchForm.traktRefreshToken,
          }),
        });
        setWatchForm((current) => ({
          ...current,
          traktClientSecret: "",
          traktAccessToken: "",
          traktRefreshToken: "",
        }));
        setWatchStatus("Watch settings saved.");
        await Promise.all([loadWatchSettings(), loadDashboard()]);
      } catch (error) {
        setWatchStatus(error.message);
      } finally {
        setBusy(false);
      }
    },
    [token, watchForm, request, setWatchForm, setWatchStatus, loadWatchSettings, loadDashboard, setMessage]
  );

  const clearTraktTokens = useCallback(
    async () => {
      if (!token) {
        setWatchStatus("Sign in first.");
        return;
      }
      setBusy(true);
      try {
        await request("/api/v1/bridge/watch/settings", {
          method: "POST",
          body: JSON.stringify({ clear_trakt_tokens: true }),
        });
        setWatchStatus("Trakt tokens cleared.");
        await Promise.all([loadWatchSettings(), loadDashboard()]);
      } catch (error) {
        setWatchStatus(error.message);
      } finally {
        setBusy(false);
      }
    },
    [token, request, loadWatchSettings, loadDashboard, setWatchStatus]
  );

  const startTraktDeviceLogin = useCallback(
    async () => {
      if (!token) {
        setWatchStatus("Sign in first.");
        return;
      }
      if (!watchForm.traktClientId.trim() && !dashboard.watch?.trakt_client_config) {
        setWatchStatus("Add a Trakt client ID and save it first.");
        return;
      }
      setBusy(true);
      try {
        const data = await request("/api/v1/bridge/watch/trakt/device-code", {
          method: "POST",
          body: JSON.stringify({
            client_id: watchForm.traktClientId,
            client_secret: watchForm.traktClientSecret,
          }),
        });
        setTraktDevice({
          code: data.device_code || "",
          userCode: data.user_code || "",
          verificationUrl: data.verification_url || "https://trakt.tv/activate",
        });
        setWatchStatus(
          data.user_code
            ? `Open ${data.verification_url || "https://trakt.tv/activate"} and enter code ${data.user_code}`
            : "Trakt device login started. Click check login when approved."
        );
      } catch (error) {
        setWatchStatus(error.message);
      } finally {
        setBusy(false);
      }
    },
    [
      token,
      request,
      watchForm.traktClientId,
      watchForm.traktClientSecret,
      dashboard.watch?.trakt_client_config,
      setWatchStatus,
      setTraktDevice,
    ]
  );

  const checkTraktDeviceLogin = useCallback(
    async () => {
      if (!token) {
        setWatchStatus("Sign in first.");
        return;
      }
      if (!traktDevice.code) {
        setWatchStatus("No Trakt device code. Start device login first.");
        return;
      }
      setBusy(true);
      try {
        const data = await request("/api/v1/bridge/watch/trakt/device-token", {
          method: "POST",
          body: JSON.stringify({ device_code: traktDevice.code }),
        });
        if (!data?.has_access_token) {
          setWatchStatus("Still waiting for Trakt approval.");
          return;
        }
        setTraktDevice({
          code: "",
          userCode: "",
          verificationUrl: "https://trakt.tv/activate",
        });
        setWatchStatus("Trakt connected.");
        await Promise.all([loadWatchSettings(), loadDashboard()]);
      } catch (error) {
        setWatchStatus(error.message);
      } finally {
        setBusy(false);
      }
    },
    [token, traktDevice.code, request, loadWatchSettings, loadDashboard, setWatchStatus, setTraktDevice]
  );

  const syncTraktWatch = useCallback(
    async () => {
      if (!token) {
        setWatchStatus("Sign in first.");
        return;
      }
      setBusy(true);
      try {
        const data = await request("/api/v1/bridge/watch/trakt/sync", {
          method: "POST",
        });
        setWatchStatus(
          `Trakt sync imported ${data.imported || 0} items. Total watch items: ${
            data.total || 0
          }.`
        );
        await loadWatchSettings();
      } catch (error) {
        setWatchStatus(error.message);
      } finally {
        setBusy(false);
      }
    },
    [token, request, loadWatchSettings, setWatchStatus]
  );

  const clearWatchHistory = useCallback(
    async () => {
      if (!token) {
        setWatchStatus("Sign in first.");
        return;
      }
      setBusy(true);
      try {
        const data = await request("/api/v1/bridge/watch/history", {
          method: "DELETE",
        });
        setWatchStatus(`Watch history cleared (${data.removed || 0} removed).`);
        await loadWatchSettings();
      } catch (error) {
        setWatchStatus(error.message);
      } finally {
        setBusy(false);
      }
    },
    [token, request, loadWatchSettings, setWatchStatus]
  );

  const savePlexSettings = useCallback(
    async () => {
      if (!token) {
        setPlexStatus("Sign in first.");
        return;
      }
      if (!plexAccessToken.trim()) {
        setPlexStatus("Paste a Plex token first.");
        return;
      }
      setBusy(true);
      try {
        await request("/api/v1/bridge/plex/settings", {
          method: "POST",
          body: JSON.stringify({ access_token: plexAccessToken.trim() }),
        });
        setPlexAccessToken("");
        setPlexStatus("Plex connected. Artwork cache will use signed Discover data on refresh.");
        await Promise.all([loadPlexSettings(), loadDashboard()]);
      } catch (error) {
        setPlexStatus(error.message);
      } finally {
        setBusy(false);
      }
    },
    [token, plexAccessToken, request, setPlexAccessToken, setPlexStatus, loadPlexSettings, loadDashboard]
  );

  const clearPlexSettings = useCallback(
    async () => {
      if (!token) {
        setPlexStatus("Sign in first.");
        return;
      }
      setBusy(true);
      try {
        await request("/api/v1/bridge/plex/settings", {
          method: "POST",
          body: JSON.stringify({ clear_token: true }),
        });
        setPlexStatus(
          "Plex token cleared. Artwork will use public pages for fallback cards."
        );
        setPlexPin(null);
        setPlexAccessToken("");
        await Promise.all([loadPlexSettings(), loadDashboard()]);
      } catch (error) {
        setPlexStatus(error.message);
      } finally {
        setBusy(false);
      }
    },
    [token, request, setPlexAccessToken, setPlexPin, setPlexStatus, loadPlexSettings, loadDashboard]
  );

  const startPlexLogin = useCallback(
    async () => {
      if (!token) {
        setPlexStatus("Sign in first.");
        return;
      }
      setBusy(true);
      try {
        const data = await request("/api/v1/bridge/plex/pin", {
          method: "POST",
        });
        setPlexPin(Number(data.id || 0) || null);
        setPlexStatus(
          `Open ${data.verification_url || "https://plex.tv/link"} and enter code ${
            data.code || ""
          }. Then click check login.`
        );
      } catch (error) {
        setPlexStatus(error.message);
      } finally {
        setBusy(false);
      }
    },
    [token, request, setPlexPin, setPlexStatus]
  );

  const checkPlexLogin = useCallback(
    async () => {
      if (!token) {
        setPlexStatus("Sign in first.");
        return;
      }
      const pinID = Number(plexPin || 0);
      if (!pinID) {
        setPlexStatus("No Plex PIN saved. Start Plex login again.");
        return;
      }
      setBusy(true);
      try {
        const data = await request("/api/v1/bridge/plex/pin/token", {
          method: "POST",
          body: JSON.stringify({ pin_id: pinID }),
        });
        if (!data?.authenticated) {
          setPlexStatus("Still waiting for Plex approval.");
          return;
        }
        setPlexPin(null);
        setPlexStatus("Plex connected.");
        await Promise.all([loadPlexSettings(), loadDashboard()]);
      } catch (error) {
        setPlexStatus(error.message);
      } finally {
        setBusy(false);
      }
    },
    [token, plexPin, request, setPlexPin, setPlexStatus, loadPlexSettings, loadDashboard]
  );

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">V</div>
          <div>
            <strong>Vortexo</strong>
            <span>Add-on Server</span>
          </div>
        </div>
        <nav className="nav-list">
          {NAV.map((item) => {
            const Icon = item.icon;
            return (
              <button
                key={item.id}
                className={view === item.id ? "nav-item active" : "nav-item"}
                onClick={() => setView(item.id)}
              >
                <Icon size={18} />
                <span>{item.label}</span>
              </button>
            );
          })}
        </nav>
        <div className="sidebar-status">
          <span className={signedIn ? "status-dot ok" : "status-dot"} />
          <div>
            <strong>{signedIn ? "Signed in" : "Signed out"}</strong>
            <span>
              {signedIn
                ? "Dashboard controls enabled"
                : "Sign in to manage add-ons"}
            </span>
          </div>
        </div>
      </aside>

      <main className="content">
        <header className="topbar">
          <div>
            <p className="eyebrow">Self-hosted control room</p>
            <h1>{pageTitle(view)}</h1>
          </div>
          <div className="top-actions">
            <button
              className="icon-button"
              title="Refresh"
              onClick={refresh}
              disabled={!signedIn || busy}
            >
              <RefreshCw size={18} />
            </button>
            <button
              className="server-pill"
              onClick={copyServerURL}
              title="Copy server URL"
            >
              <Server size={17} />
              <span>{serverUrl.replace(/^https?:\/\//, "")}</span>
            </button>
          </div>
        </header>

        {message && (
          <div
            className={
              isErrorMessage(message) ? "notice error" : "notice"
            }
          >
            {message}
          </div>
        )}

        {!signedIn && view !== "settings" ? (
          <SignInCard
            login={login}
            setLogin={setLogin}
            onSubmit={signIn}
            busy={busy}
          />
        ) : (
          <>
            {view === "overview" && (
              <Overview
                summary={summary}
                dashboard={dashboard}
                homeRows={homeRows}
              />
            )}
            {view === "discover" && (
              <Discover
                registry={registry}
                setRegistry={setRegistry}
                addons={registryAddons}
                loading={registryLoading}
                busy={busy}
                onSearch={loadRegistry}
                onInstall={(addon) =>
                  installManifestURL(
                    addon.url,
                    addon.name,
                    `${addon.name} installed`
                  )
                }
              />
            )}
            {view === "addons" && (
              <Addons
                manifests={dashboard.manifests || []}
                manual={manual}
                setManual={setManual}
                onInstall={installManifest}
                onUpdate={updateManifest}
                onRemove={removeManifest}
                busy={busy}
              />
            )}
            {view === "catalogs" && (
              <Catalogs catalogs={dashboard.catalogs || []} busy={busy} />
            )}
            {view === "live" && (
              <LiveTV rows={liveRows} summary={summary} />
            )}
            {view === "setup" && (
              <Setup
                streamingCatalogs={streamingCatalogs}
                setStreamingCatalogs={setStreamingCatalogs}
                keywordRows={keywordRows}
                setKeywordRows={setKeywordRows}
                keywordRowsStatus={dashboard.tmdb_keyword_rows || {}}
                onSaveStreamingCatalogs={saveStreamingCatalogs}
                onSaveKeywordRows={saveKeywordRows}
                busy={busy}
              />
            )}
            {view === "watch" && (
              <WatchSync
                watch={dashboard.watch || {}}
                form={watchForm}
                setForm={setWatchForm}
                status={watchStatus}
                plex={plexSettings}
                artwork={dashboard.artwork || {}}
                plexAccessToken={plexAccessToken}
                setPlexAccessToken={setPlexAccessToken}
                plexPin={plexPin}
                plexStatus={plexStatus}
                traktDeviceCode={traktDevice.code}
                traktUserCode={traktDevice.userCode}
                traktVerificationUrl={traktDevice.verificationUrl}
                onSaveWatchSettings={saveWatchSettings}
                onClearTraktTokens={clearTraktTokens}
                onStartTraktLogin={startTraktDeviceLogin}
                onCheckTraktLogin={checkTraktDeviceLogin}
                onSyncTrakt={syncTraktWatch}
                onClearWatchHistory={clearWatchHistory}
                onSavePlexSettings={savePlexSettings}
                onClearPlexSettings={clearPlexSettings}
                onStartPlexLogin={startPlexLogin}
                onCheckPlexLogin={checkPlexLogin}
                busy={busy}
              />
            )}
            {view === "settings" && (
              <SettingsView
                signedIn={signedIn}
                login={login}
                setLogin={setLogin}
                onSignIn={signIn}
                onSignOut={signOut}
                serverUrl={serverUrl}
                registry={registry}
                setRegistry={setRegistry}
                onCopy={copyServerURL}
                busy={busy}
              />
            )}
          </>
        )}
      </main>
    </div>
  );
}

function App() {
  return (
    <ConfigProvider>
      <AppContent />
    </ConfigProvider>
  );
}

// ============ COMPONENTS ============
function SignInCard({ login, setLogin, onSubmit, busy }) {
  return (
    <form className="panel sign-in-card" onSubmit={onSubmit}>
      <div className="lock-icon">
        <Lock size={22} />
      </div>
      <div>
        <p className="eyebrow">Admin</p>
        <h2>Sign in to manage the server</h2>
      </div>
      <div className="form-grid two">
        <TextField
          label="Username"
          value={login.username}
          onChange={(value) => setLogin({ ...login, username: value })}
        />
        <TextField
          label="Password"
          type="password"
          value={login.password}
          onChange={(value) => setLogin({ ...login, password: value })}
        />
      </div>
      <div className="form-actions">
        <button type="submit" disabled={busy}>
          Sign in
        </button>
      </div>
    </form>
  );
}

function Overview({ summary, dashboard, homeRows }) {
  const previewRows = (homeRows || []).slice(0, 5);
  const artwork = dashboard.artwork || {};
  const artworkGaps = (artwork.miss || 0) + (artwork.error || 0);
  const artworkStatus = artwork.running
    ? "sync running"
    : artwork.has_plex_token
      ? "Plex connected"
      : "Plex missing";

  return (
    <section className="stack">
      <div className="metric-grid overview-grid">
        <Metric
          icon={Database}
          label="Installed add-ons"
          value={summary.manifests}
          detail={`${summary.enabled} enabled`}
        />
        <Metric
          icon={Library}
          label="Catalog rows"
          value={summary.activeCatalogs}
          detail={`${summary.catalogs} managed`}
        />
        <Metric
          icon={Play}
          label="Stream providers"
          value={summary.streamProviders}
          detail={`${summary.subtitleProviders} subtitle providers`}
        />
        <Metric
          icon={Eye}
          label="Watch items"
          value={summary.watchItems}
          detail={
            dashboard.watch?.trakt_connected ? "Trakt connected" : "Local state"
          }
        />
        <Metric
          icon={Clapperboard}
          label="Plex artwork"
          value={summary.artworkClean}
          detail={`${artwork.total || 0} cached · ${artworkStatus}`}
        />
      </div>

      <div className="panel split-panel">
        <div>
          <p className="eyebrow">Health</p>
          <h2>Server output</h2>
          <p className="muted">
            {summary.broken === 0
              ? "Installed add-ons are responding cleanly."
              : `${summary.broken} add-on needs attention.`}
          </p>
        </div>
        <div className="health-list">
          <HealthRow
            ok={summary.manifests > 0}
            label="Add-ons"
            value={`${summary.manifests} installed`}
          />
          <HealthRow
            ok={summary.activeCatalogs > 0}
            label="Catalogs"
            value={`${summary.activeCatalogs} active`}
          />
          <HealthRow
            ok={summary.streamProviders > 0}
            label="Streams"
            value={`${summary.streamProviders} providers`}
          />
          <HealthRow
            ok={summary.broken === 0}
            label="Errors"
            value={summary.broken === 0 ? "None" : `${summary.broken} found`}
          />
          <HealthRow
            ok={(artwork.clean_landscape || 0) > 0}
            label="Clean landscapes"
            value={`${artwork.clean_landscape || 0} items`}
          />
          <HealthRow
            ok={(artwork.backdrop_only || 0) === 0}
            label="Backdrop-only"
            value={`${artwork.backdrop_only || 0} items`}
          />
          <HealthRow
            ok={artworkGaps === 0}
            label="Artwork gaps"
            value={artworkGaps === 0 ? "None" : `${artworkGaps} items`}
          />
          <HealthRow
            ok={(artwork.signed_discover || 0) > 0}
            label="Signed Discover"
            value={`${artwork.signed_discover || 0} hits`}
          />
        </div>
      </div>

      <div className="panel">
        <div className="section-head">
          <div>
            <p className="eyebrow">Home</p>
            <h2>Rows Vortexo can read</h2>
          </div>
        </div>
        {previewRows.length === 0 ? (
          <EmptyState
            icon={Library}
            title="No rows yet"
            text="Install a catalog add-on or run the guided setup to populate the Vortexo home feed."
          />
        ) : (
          <div className="preview-rows">
            {previewRows.map((row) => (
              <div className="preview-row" key={row.id}>
                <div className="row-title">
                  <strong>{row.title}</strong>
                  <span>{row.reason || "Catalog row"}</span>
                </div>
                <div className="poster-strip">
                  {(row.items || [])
                    .slice(0, 7)
                    .map((item) => (
                      <div
                        className="poster"
                        key={item.id || item.ratingKey || item.title}
                      >
                        {imageForItem(item) ? (
                          <img
                            src={imageForItem(item)}
                            alt=""
                            loading="lazy"
                          />
                        ) : (
                          <Clapperboard size={20} />
                        )}
                      </div>
                    ))}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </section>
  );
}

function Discover({ registry, setRegistry, addons, loading, busy, onSearch, onInstall }) {
  return (
    <section className="stack">
      <form
        className="panel compact-form"
        onSubmit={(event) => {
          event.preventDefault();
          onSearch();
        }}
      >
        <div className="section-head">
          <div>
            <p className="eyebrow">Registry</p>
            <h2>Browse add-ons</h2>
          </div>
          <Globe2 size={22} />
        </div>
        <div className="form-grid registry-form">
          <label className="wide-field">
            <span>Registry manifest URL</span>
            <input
              value={registry.url}
              onChange={(event) =>
                setRegistry({ ...registry, url: event.target.value })
              }
            />
          </label>
          <TextField
            label="Search"
            value={registry.q}
            onChange={(value) => setRegistry({ ...registry, q: value })}
          />
          <SelectField
            label="Capability"
            value={registry.capability}
            onChange={(value) =>
              setRegistry({ ...registry, capability: value })
            }
            options={[
              ["all", "All"],
              ["catalog", "Catalogs"],
              ["meta", "Metadata"],
              ["stream", "Streams"],
              ["subtitles", "Subtitles"],
              ["live_tv", "Live TV"],
            ]}
          />
          <SelectField
            label="Type"
            value={registry.type}
            onChange={(value) => setRegistry({ ...registry, type: value })}
            options={[
              ["all", "All"],
              ["movie", "Movies"],
              ["series", "Series"],
              ["tv", "TV"],
              ["channel", "Live channels"],
            ]}
          />
        </div>
        <div className="form-actions">
          <button type="submit" disabled={loading || busy}>
            {loading ? "Loading" : "Search"}
          </button>
        </div>
      </form>

      <div className="panel">
        <div className="section-head">
          <div>
            <p className="eyebrow">Available</p>
            <h2>Add-ons from registry</h2>
          </div>
          <span className="count-pill">{addons.length}</span>
        </div>
        {loading ? (
          <EmptyState
            icon={RefreshCw}
            title="Loading registry"
            text="Fetching available add-ons from the configured registry."
          />
        ) : addons.length === 0 ? (
          <EmptyState
            icon={Search}
            title="No add-ons found"
            text="Try clearing the filters or checking the registry URL."
          />
        ) : (
          <div className="addon-grid">
            {addons.map((addon) => (
              <article
                className="addon-card"
                key={addon.url || addon.id}
              >
                <div className="addon-top">
                  <div className="addon-icon">
                    {initials(addon.name || addon.id)}
                  </div>
                  <div>
                    <h3>{addon.name || addon.id}</h3>
                    <span
                      className={
                        addon.installed
                          ? "small-status ok"
                          : addon.configuration_required
                            ? "small-status warn"
                            : "small-status"
                      }
                    >
                      {addon.installed
                        ? "installed"
                        : addon.configuration_required
                          ? "configure first"
                          : "available"}
                    </span>
                  </div>
                  {addon.config_url ? (
                    <a
                      className="icon-link"
                      href={addon.config_url}
                      target="_blank"
                      rel="noreferrer"
                      title="Configure"
                    >
                      <ExternalLink size={17} />
                    </a>
                  ) : (
                    <span />
                  )}
                </div>
                {addon.description && (
                  <p className="muted clamp">{addon.description}</p>
                )}
                <div className="chip-row">
                  {(addon.capabilities || [])
                    .slice(0, 5)
                    .map((capability) => (
                      <span className="chip" key={capability}>
                        {labelCapability(capability)}
                      </span>
                    ))}
                  {addon.configurable && (
                    <span className="chip amber">Configurable</span>
                  )}
                </div>
                <div className="addon-meta">
                  <span>{addon.catalogs?.length || 0} catalogs</span>
                  <span>{(addon.types || []).join(", ") || "Any type"}</span>
                </div>
                <div className="form-actions compact-actions">
                  <button
                    type="button"
                    disabled={busy || addon.installed || !addon.url}
                    onClick={() => onInstall(addon)}
                  >
                    {addon.installed ? "Installed" : "Install"}
                  </button>
                  {addon.url && (
                    <a
                      className="text-link"
                      href={addon.url}
                      target="_blank"
                      rel="noreferrer"
                    >
                      Manifest
                    </a>
                  )}
                </div>
              </article>
            ))}
          </div>
        )}
      </div>
    </section>
  );
}

function Addons({ manifests, manual, setManual, onInstall, onUpdate, onRemove, busy }) {
  return (
    <section className="stack">
      <form className="panel compact-form" onSubmit={onInstall}>
        <div>
          <p className="eyebrow">Install</p>
          <h2>Add manifest URL</h2>
        </div>
        <div className="form-grid">
          <label>
            <span>Name</span>
            <input
              value={manual.name}
              onChange={(event) =>
                setManual({ ...manual, name: event.target.value })
              }
              placeholder="Optional display name"
            />
          </label>
          <label>
            <span>Manifest URL</span>
            <input
              value={manual.url}
              onChange={(event) =>
                setManual({ ...manual, url: event.target.value })
              }
              placeholder="https://example.com/.../manifest.json"
            />
          </label>
        </div>
        <div className="form-actions">
          <button type="submit" disabled={busy}>
            Install
          </button>
        </div>
      </form>

      <div className="panel">
        <div className="section-head">
          <div>
            <p className="eyebrow">Installed</p>
            <h2>Add-ons</h2>
          </div>
          <span className="count-pill">{manifests.length}</span>
        </div>
        {manifests.length === 0 ? (
          <EmptyState
            icon={Sparkles}
            title="No add-ons installed"
            text="Install a catalog, stream, subtitle, or Live TV manifest to start feeding Vortexo."
          />
        ) : (
          <div className="addon-grid">
            {manifests.map((item) => (
              <article className="addon-card" key={item.id}>
                <div className="addon-top">
                  <div className="addon-icon">
                    {initials(item.name || item.id)}
                  </div>
                  <div>
                    <h3>{item.name || item.id}</h3>
                    <span
                      className={
                        item.status === "ok"
                          ? "small-status ok"
                          : item.status === "error"
                            ? "small-status error"
                            : "small-status"
                      }
                    >
                      {item.status || (item.enabled ? "enabled" : "disabled")}
                    </span>
                  </div>
                  <button
                    className="icon-button danger"
                    title="Remove"
                    onClick={() => onRemove(item.id)}
                    disabled={busy}
                  >
                    <Trash2 size={17} />
                  </button>
                </div>
                {item.description && (
                  <p className="muted clamp">{item.description}</p>
                )}
                <div className="chip-row">
                  {(item.capabilities || []).map((capability) => (
                    <span className="chip" key={capability}>
                      {labelCapability(capability)}
                    </span>
                  ))}
                  {item.capabilities?.length === 0 && (
                    <span className="chip muted-chip">No capabilities</span>
                  )}
                </div>
                <div className="addon-meta">
                  <span>{item.catalogs?.length || 0} catalogs</span>
                  <span>{(item.types || []).join(", ") || "Any type"}</span>
                </div>
                {item.message && (
                  <div className="inline-error">{item.message}</div>
                )}
              </article>
            ))}
          </div>
        )}
      </div>
    </section>
  );
}

function Catalogs({ catalogs, busy }) {
  const sorted = sortedCatalogs(catalogs || []);

  return (
    <section className="panel">
      <div className="section-head">
        <div>
          <p className="eyebrow">Catalog manager</p>
          <h2>Rows coming from add-ons</h2>
        </div>
        <span className="count-pill">{sorted.length}</span>
      </div>
      {sorted.length === 0 ? (
        <EmptyState
          icon={Library}
          title="No catalogs"
          text="Install a catalog-capable add-on to see rows here."
        />
      ) : (
        <div className="catalog-list managed">
          {sorted.map((catalog) => (
            <div
              className={
                catalog.enabled === false ? "catalog-row disabled" : "catalog-row"
              }
              key={catalog.key}
            >
              <div>
                <strong>{catalog.original_name || catalog.id}</strong>
                <span>{catalog.manifest_name}</span>
              </div>
              <div className="chip-row tight">
                <span className="chip">{catalog.type}</span>
                {catalog.search && <span className="chip">Search</span>}
                {catalog.required_extras?.map((extra) => (
                  <span className="chip amber" key={extra}>
                    {extra}
                  </span>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

function LiveTV({ rows, summary }) {
  const channels = rows.flatMap((row) => row.items || []);
  return (
    <section className="stack">
      <div className="metric-grid two-cols">
        <Metric
          icon={Radio}
          label="Live providers"
          value={summary.liveProviders}
          detail="Add-ons with live catalogs"
        />
        <Metric
          icon={Eye}
          label="Channels"
          value={channels.length}
          detail={`${rows.length} rows available`}
        />
      </div>
      <div className="panel">
        <div className="section-head">
          <div>
            <p className="eyebrow">Live TV</p>
            <h2>Channels Vortexo can read</h2>
          </div>
        </div>
        {rows.length === 0 ? (
          <EmptyState
            icon={Radio}
            title="No live channels"
            text="Install a Live TV manifest add-on to populate the Live TV tab in Vortexo."
          />
        ) : (
          <div className="live-list">
            {rows.map((row) => (
              <div className="live-row" key={row.id}>
                <div className="row-title">
                  <strong>{row.title}</strong>
                  <span>{row.reason}</span>
                </div>
                <div className="channel-strip">
                  {(row.items || [])
                    .slice(0, 10)
                    .map((channel) => (
                      <div className="channel-card" key={channel.id}>
                        {channel.logo ? (
                          <img
                            src={channel.logo}
                            alt=""
                            loading="lazy"
                          />
                        ) : (
                          <Radio size={18} />
                        )}
                        <span>{channel.name}</span>
                      </div>
                    ))}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </section>
  );
}

function Setup({
  streamingCatalogs,
  setStreamingCatalogs,
  keywordRows,
  setKeywordRows,
  keywordRowsStatus,
  onSaveStreamingCatalogs,
  onSaveKeywordRows,
  busy,
}) {
  const streamingLayout = streamingCatalogs.mergeAll
    ? "all"
    : streamingCatalogs.mergeProviders
      ? "provider"
      : "separate";
  const setStreamingLayout = (layout) =>
    setStreamingCatalogs({
      ...streamingCatalogs,
      mergeProviders: layout === "provider",
      mergeAll: layout === "all",
      sortBy: layout === "all" ? "NEW" : streamingCatalogs.sortBy,
    });
  const keywordMaxRows = Math.max(1, Number(keywordRowsStatus?.max_row_count || 50));
  const defaultKeywordRows = Math.max(
    1,
    Number(keywordRowsStatus?.default_row_count || 10)
  );
  const currentKeywordRows = Math.max(
    1,
    Math.min(keywordMaxRows, Number(keywordRows.rowCount || defaultKeywordRows))
  );

  return (
    <section className="stack">
      <form
        className="panel compact-form"
        onSubmit={(event) => {
          event.preventDefault();
          onSaveStreamingCatalogs();
        }}
      >
        <div className="section-head">
          <div>
            <p className="eyebrow">Built-in catalogs</p>
            <h2>Streaming Catalogs</h2>
          </div>
          <Clapperboard size={22} />
        </div>
        <div className="choice-block">
          <span>Catalog providers</span>
          <div className="choice-grid">
            {STREAMING_CATALOG_PROVIDERS.map(([id, label]) => (
              <label
                className={
                  streamingCatalogs.providers.includes(id)
                    ? "choice-chip selected"
                    : "choice-chip"
                }
                key={id}
              >
                <input
                  type="checkbox"
                  checked={streamingCatalogs.providers.includes(id)}
                  onChange={() =>
                    setStreamingCatalogs({
                      ...streamingCatalogs,
                      providers: toggleArrayValue(
                        streamingCatalogs.providers,
                        id
                      ),
                    })
                  }
                />
                {label}
              </label>
            ))}
          </div>
        </div>
        <div className="choice-block">
          <span>Catalog types</span>
          <div className="choice-grid compact-choice-grid">
            {STREAMING_CATALOG_TYPES.map(([type, label]) => (
              <label
                className={
                  streamingCatalogs.types.includes(type)
                    ? "choice-chip selected"
                    : "choice-chip"
                }
                key={type}
              >
                <input
                  type="checkbox"
                  checked={streamingCatalogs.types.includes(type)}
                  onChange={() =>
                    setStreamingCatalogs({
                      ...streamingCatalogs,
                      types: toggleArrayValue(streamingCatalogs.types, type),
                    })
                  }
                />
                {label}
              </label>
            ))}
          </div>
        </div>
        <div className="choice-block">
          <span>Merge behavior</span>
          <div className="choice-grid compact-choice-grid">
            <label
              className={
                streamingLayout === "separate" ? "choice-chip selected" : "choice-chip"
              }
            >
              <input
                type="radio"
                checked={streamingLayout === "separate"}
                onChange={() => setStreamingLayout("separate")}
              />
              Separate rows
            </label>
            <label
              className={
                streamingLayout === "provider" ? "choice-chip selected" : "choice-chip"
              }
            >
              <input
                type="radio"
                checked={streamingLayout === "provider"}
                onChange={() => setStreamingLayout("provider")}
              />
              Merge by provider
            </label>
            <label
              className={
                streamingLayout === "all" ? "choice-chip selected" : "choice-chip"
              }
            >
              <input
                type="radio"
                checked={streamingLayout === "all"}
                onChange={() => setStreamingLayout("all")}
              />
              Merge all
            </label>
          </div>
          <label className="wide-field" style={{ marginTop: 8 }}>
            <span>Sort order</span>
            <select
              value={streamingCatalogs.sortBy}
              onChange={(event) =>
                setStreamingCatalogs({
                  ...streamingCatalogs,
                  sortBy: event.target.value,
                })
              }
            >
              {STREAMING_CATALOG_SORTS.map(([value, label]) => (
                <option key={value} value={value}>
                  {label}
                </option>
              ))}
            </select>
          </label>
        </div>
        <label className="wide-field">
          <span>RPDB Key (optional)</span>
          <input
            value={streamingCatalogs.rpdbKey}
            placeholder="Optional for richer ratings metadata"
            onChange={(event) =>
              setStreamingCatalogs({
                ...streamingCatalogs,
                rpdbKey: event.target.value,
              })
            }
          />
        </label>
        <div className="form-actions">
          <button type="submit" disabled={busy}>
            {busy ? "Saving..." : "Save streaming catalog settings"}
          </button>
          <span className="small-status muted-chip">
            {streamingCatalogs.providers.length} providers ·{" "}
            {streamingCatalogs.types.join(", ")}
          </span>
        </div>
      </form>

      <form
        className="panel compact-form"
        onSubmit={(event) => {
          event.preventDefault();
          onSaveKeywordRows();
        }}
      >
        <div className="section-head">
          <div>
            <p className="eyebrow">Discover rows</p>
            <h2>TMDB keyword rows</h2>
          </div>
        </div>
        <label>
          <span>Enable keyword rows</span>
          <input
            type="checkbox"
            checked={Boolean(keywordRows.enabled)}
            onChange={(event) =>
              setKeywordRows({
                ...keywordRows,
                enabled: event.target.checked,
              })
            }
          />
        </label>
        <div className="form-grid three">
          <label>
            <span>Rows to add</span>
            <input
              type="number"
              min={1}
              max={keywordMaxRows}
              value={currentKeywordRows}
              onChange={(event) =>
                setKeywordRows({
                  ...keywordRows,
                  rowCount: Number(event.target.value) || 1,
                })
              }
            />
          </label>
          <label>
            <span>Language</span>
            <input
              value={keywordRows.language}
              onChange={(event) =>
                setKeywordRows({
                  ...keywordRows,
                  language: event.target.value,
                })
              }
            />
          </label>
          <label>
            <span>Region</span>
            <input
              value={keywordRows.region}
              onChange={(event) =>
                setKeywordRows({
                  ...keywordRows,
                  region: event.target.value,
                })
              }
            />
          </label>
        </div>
        <div className="form-grid three">
          <TextField
            label="TMDB API Key"
            value={keywordRows.tmdbKey}
            onChange={(value) => setKeywordRows({ ...keywordRows, tmdbKey: value })}
            placeholder="tmdb key"
            help="Required when access token is not set."
          />
          <TextField
            label="TMDB Access Token"
            value={keywordRows.tmdbToken}
            onChange={(value) =>
              setKeywordRows({ ...keywordRows, tmdbToken: value })
            }
            placeholder="tmdb access token"
            help="Alternative to API key for TMDB requests."
          />
          <label>
            <span>Clear saved TMDB credentials</span>
            <input
              type="checkbox"
              checked={Boolean(keywordRows.clearCredentials)}
              onChange={(event) =>
                setKeywordRows({
                  ...keywordRows,
                  clearCredentials: event.target.checked,
                })
              }
            />
          </label>
        </div>
        <div className="form-actions">
          <button type="submit" disabled={busy}>
            {busy ? "Saving..." : "Save keyword rows"}
          </button>
          <span className="small-status muted-chip">
            {keywordRowsStatus?.has_access_token || keywordRowsStatus?.has_api_key
              ? "Credentials available"
              : "No saved TMDB credentials"}
          </span>
          <span className="small-status muted-chip">
            Max rows: {keywordMaxRows}
          </span>
        </div>
      </form>
    </section>
  );
}

function WatchSync({
  watch,
  form,
  setForm,
  status,
  plex,
  artwork,
  plexAccessToken,
  setPlexAccessToken,
  plexPin,
  plexStatus,
  traktDeviceCode,
  traktUserCode,
  traktVerificationUrl,
  onSaveWatchSettings,
  onClearTraktTokens,
  onStartTraktLogin,
  onCheckTraktLogin,
  onSyncTrakt,
  onClearWatchHistory,
  onSavePlexSettings,
  onClearPlexSettings,
  onStartPlexLogin,
  onCheckPlexLogin,
  busy,
}) {
  const hasTraktConfig = Boolean(
    watch.trakt_client_config || form.traktClientId.trim()
  );
  const connectedAccounts = [
    watch.trakt_connected ? "Trakt" : "",
    plex?.has_access_token ? "Plex" : "",
  ].filter(Boolean);

  return (
    <section className="stack">
      <div className="metric-grid two-cols">
        <Metric
          icon={Eye}
          label="Watch items"
          value={watch.count || 0}
          detail="Local normalized state"
        />
        <Metric
          icon={BadgeCheck}
          label="Connections"
          value={connectedAccounts.length}
          detail={connectedAccounts.join(", ") || "None"}
        />
      </div>

      <form
        className="panel compact-form"
        onSubmit={(event) => {
          event.preventDefault();
          onSaveWatchSettings();
        }}
      >
        <div className="section-head">
          <div>
            <p className="eyebrow">Trakt</p>
            <h2>Watch sync settings</h2>
          </div>
          <Lock size={22} />
        </div>
        <div className="form-grid three">
          <TextField
            label="Trakt Client ID"
            value={form.traktClientId}
            onChange={(value) =>
              setForm({ ...form, traktClientId: value })
            }
          />
          <TextField
            label="Client Secret"
            value={form.traktClientSecret}
            onChange={(value) =>
              setForm({ ...form, traktClientSecret: value })
            }
          />
          <TextField
            label="Access Token (optional)"
            value={form.traktAccessToken}
            onChange={(value) =>
              setForm({ ...form, traktAccessToken: value })
            }
          />
        </div>
        <TextField
          label="Refresh Token (optional)"
          value={form.traktRefreshToken}
          onChange={(value) => setForm({ ...form, traktRefreshToken: value })}
        />
        <div className="form-actions">
          <button type="submit" disabled={busy}>
            {busy ? "Saving..." : "Save Trakt settings"}
          </button>
          <button
            type="button"
            className="secondary"
            onClick={onClearTraktTokens}
            disabled={busy}
          >
            Clear Trakt tokens
          </button>
          {traktUserCode && traktDeviceCode && (
            <a
              className="text-link"
              href={traktVerificationUrl}
              target="_blank"
              rel="noreferrer"
            >
              Open Trakt activation
            </a>
          )}
        </div>
        <div className="field-actions form-actions compact-actions">
          <button
            type="button"
            onClick={onStartTraktLogin}
            disabled={busy || !hasTraktConfig}
          >
            Start Trakt login
          </button>
          <button
            type="button"
            onClick={onCheckTraktLogin}
            disabled={busy || !traktDeviceCode}
          >
            Check Trakt login
          </button>
          <button type="button" onClick={onSyncTrakt} disabled={busy}>
            Sync Trakt history
          </button>
          <button
            type="button"
            className="danger-action"
            onClick={onClearWatchHistory}
            disabled={busy}
          >
            Clear watch items
          </button>
        </div>
        {traktUserCode && (
          <p className="muted">
            Enter code <strong>{traktUserCode}</strong> at{" "}
            {traktVerificationUrl}.
          </p>
        )}
        {status && (
          <div
            className={
              isErrorMessage(status) ? "inline-error" : "inline-note"
            }
          >
            {status}
          </div>
        )}
      </form>

      <form
        className="panel compact-form"
        onSubmit={(event) => {
          event.preventDefault();
          onSavePlexSettings();
        }}
      >
        <div className="section-head">
          <div>
            <p className="eyebrow">Plex</p>
            <h2>Artwork + proxy credentials</h2>
          </div>
          <ShieldCheck size={22} />
        </div>
        <p className="muted">
          Signed in users improve poster metadata and trailer lookup reliability.
        </p>
        <TextField
          label="Plex Access Token"
          value={plexAccessToken}
          onChange={setPlexAccessToken}
        />
        <div className="form-actions">
          <button
            type="submit"
            disabled={busy || !plexAccessToken.trim()}
          >
            Save Plex token
          </button>
          <button
            type="button"
            className="secondary"
            onClick={onClearPlexSettings}
            disabled={busy}
          >
            Clear Plex token
          </button>
          <button type="button" onClick={onStartPlexLogin} disabled={busy}>
            Start Plex login
          </button>
          <button
            type="button"
            onClick={onCheckPlexLogin}
            disabled={busy || !plexPin}
          >
            Check Plex login
          </button>
        </div>
        {plexPin && (
          <div className="inline-note">Your Plex PIN: {plexPin}</div>
        )}
        {plex?.has_access_token ? (
          <div className="inline-note">
            Plex connected.
          </div>
        ) : (
          <div className="inline-note">
            Plex not connected.
          </div>
        )}
        {plexStatus && (
          <div
            className={
              isErrorMessage(plexStatus) ? "inline-error" : "inline-note"
            }
          >
            {plexStatus}
          </div>
        )}
      </form>
    </section>
  );
}

function SettingsView({
  signedIn,
  login,
  setLogin,
  onSignIn,
  onSignOut,
  serverUrl,
  registry,
  setRegistry,
  onCopy,
  busy,
}) {
  return (
    <section className="stack">
      <div className="panel split-panel">
        <div>
          <p className="eyebrow">Apple TV</p>
          <h2>Server URL</h2>
          <p className="server-url">{serverUrl}</p>
        </div>
        <button onClick={onCopy}>Copy URL</button>
      </div>
      {signedIn ? (
        <div className="panel split-panel">
          <div>
            <p className="eyebrow">Session</p>
            <h2>Signed in</h2>
            <p className="muted">
              Dashboard management is active in this browser.
            </p>
          </div>
          <button className="secondary" onClick={onSignOut}>
            Sign out
          </button>
        </div>
      ) : (
        <SignInCard
          login={login}
          setLogin={setLogin}
          onSubmit={onSignIn}
          busy={busy}
        />
      )}
    </section>
  );
}

// ============ UTILITY COMPONENTS ============
function Metric({ icon: Icon, label, value, detail }) {
  return (
    <div className="metric-card">
      <Icon size={21} />
      <span>{label}</span>
      <strong>{value}</strong>
      <p>{detail}</p>
    </div>
  );
}

function HealthRow({ ok, label, value }) {
  return (
    <div className="health-row">
      {ok ? <BadgeCheck size={18} /> : <CircleAlert size={18} />}
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function EmptyState({ icon: Icon, title, text }) {
  return (
    <div className="empty-state">
      <Icon size={28} />
      <strong>{title}</strong>
      <span>{text}</span>
    </div>
  );
}

function TextField({ label, value, onChange, type = "text", placeholder = "", help = "" }) {
  return (
    <label>
      <span>{label}</span>
      <input
        type={type}
        value={value}
        placeholder={placeholder}
        onChange={(event) => onChange(event.target.value)}
      />
      {help && <small className="field-help">{help}</small>}
    </label>
  );
}

function SelectField({ label, value, onChange, options }) {
  return (
    <label>
      <span>{label}</span>
      <select value={value} onChange={(event) => onChange(event.target.value)}>
        {options.map(([optionValue, optionLabel]) => (
          <option key={optionValue} value={optionValue}>
            {optionLabel}
          </option>
        ))}
      </select>
    </label>
  );
}

// ============ HELPER FUNCTIONS ============
function pageTitle(view) {
  return {
    overview: "Dashboard",
    discover: "Discover",
    addons: "Add-ons",
    catalogs: "Catalogs",
    live: "Live TV",
    setup: "Setup",
    watch: "Watch Sync",
    settings: "Settings",
  }[view] || "Dashboard";
}

function initials(value) {
  return String(value || "V")
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0])
    .join("")
    .toUpperCase();
}

function labelCapability(value) {
  return {
    catalog: "Catalogs",
    meta: "Metadata",
    stream: "Streams",
    subtitles: "Subtitles",
    live_tv: "Live TV",
  }[value] || value;
}

function imageForItem(item) {
  return (
    item?.poster_path ||
    item?.poster ||
    item?.thumb ||
    item?.art ||
    item?.background ||
    item?.backdrop_path ||
    item?.landscape_path ||
    item?.logo_path ||
    item?.logo ||
    ""
  );
}

function sortedCatalogs(catalogs) {
  return [...catalogs].sort((a, b) => {
    const left = Number.isFinite(a.sort_order) ? a.sort_order : 0;
    const right = Number.isFinite(b.sort_order) ? b.sort_order : 0;
    if (left !== right) return left - right;
    return String(a.name || a.id).localeCompare(String(b.name || b.id));
  });
}

function toggleArrayValue(values, value) {
  return values.includes(value)
    ? values.filter((item) => item !== value)
    : [...values, value];
}

function isErrorMessage(message) {
  const text = String(message || "").toLowerCase();
  return (
    text.includes("failed") ||
    text.includes("error") ||
    text.includes("invalid") ||
    text.includes("required") ||
    text.includes("no plex row choices")
  );
}

createRoot(document.getElementById("root")).render(<App />);
