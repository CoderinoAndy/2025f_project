(() => { // Keep mailbox rows synchronized with server updates.
  const pageRoot = document.body; // Page root stores list metadata in data-* attributes.
  const shell = document.querySelector(".list-shell"); // Row container to replace on each refresh.
  if (!pageRoot || !shell) { // Exit when this page is not a live list view.
    return;
  }

  const listView = pageRoot.dataset.liveEmailView || ""; // API view key (all, junk, archive, etc.).
  if (!listView) { // Only mailbox views opt into live polling.
    return;
  }

  const parsedPollIntervalMs = Number(pageRoot.dataset.pollIntervalMs || "2000"); // Read interval from server-rendered data.
  const pollIntervalMs = Number.isFinite(parsedPollIntervalMs) // Clamp invalid values to safe defaults.
    ? Math.max(1000, parsedPollIntervalMs) // Never poll faster than once per second.
    : 2000;
  const sortSelect = document.querySelector(".sort-select"); // Sort dropdown affects polling query params.
  let currentSort = pageRoot.dataset.sort || "date_desc"; // Current sort mode sent to API.
  let lastFingerprint = pageRoot.dataset.fingerprint || ""; // Last known content hash from server.
  let timerId = null; // Active setInterval handle, if polling is running.
  let inFlight = false; // Prevent overlapping fetches when responses are slow.

  const buildUrl = () => { // Build an API URL for the latest rows of this view/sort.
    const params = new URLSearchParams();
    params.set("view", listView); // Mailbox tab currently open.
    params.set("sort", currentSort); // Sort code selected by user.
    params.set("next", `${window.location.pathname}${window.location.search}`); // Preserve return URL context.
    params.set("t", Date.now().toString()); // Cache-busting timestamp for proxies/browsers.
    return `/api/list-emails?${params.toString()}`;
  };

  const refreshList = async () => { // Pull fresh rows and patch DOM only when data changed.
    if (inFlight) { // Skip if previous poll is still pending.
      return;
    }
    inFlight = true;
    try {
      const response = await fetch(buildUrl(), {
        method: "GET",
        headers: {
          Accept: "application/json", // API returns JSON payload with rendered HTML.
        },
        cache: "no-store", // Always request fresh server state.
      });
      if (!response.ok) { // Ignore transient HTTP failures; next poll will retry.
        return;
      }
      const payload = await response.json();
      if (!payload || typeof payload.html !== "string") { // Guard against malformed responses.
        return;
      }
      if (payload.fingerprint && payload.fingerprint === lastFingerprint) { // Skip DOM write when content is unchanged.
        return;
      }
      shell.innerHTML = payload.html; // Replace mailbox rows with latest server-rendered markup.
      lastFingerprint = payload.fingerprint || ""; // Update local hash to support no-op checks.
      pageRoot.dataset.fingerprint = lastFingerprint; // Persist hash for other scripts/debugging.
    } catch (error) {
      return; // Swallow network errors to keep polling loop resilient.
    } finally {
      inFlight = false; // Always release fetch lock.
    }
  };

  const stopPolling = () => { // Clear interval safely when tab hides or page unloads.
    if (timerId === null) {
      return;
    }
    window.clearInterval(timerId);
    timerId = null;
  };

  const startPolling = () => { // Restart interval with current configured cadence.
    stopPolling(); // Ensure only one interval exists.
    timerId = window.setInterval(refreshList, pollIntervalMs);
  };

  if (sortSelect) {
    sortSelect.addEventListener("change", (event) => {
      currentSort = event.target.value || currentSort; // Poll next cycle using new sort value.
    });
  }

  document.addEventListener("visibilitychange", () => { // Pause background polling to reduce unnecessary requests.
    if (document.hidden) {
      stopPolling();
      return;
    }
    refreshList(); // Fetch immediately when tab becomes visible again.
    startPolling(); // Resume recurring updates.
  });

  window.addEventListener("beforeunload", stopPolling); // Cleanup interval during navigation away.
  refreshList(); // Perform an immediate refresh on initial page load.
  startPolling(); // Then continue periodic refreshes.
})();
