(() => {
  const pageRoot = document.body;
  const shell = document.querySelector(".list-shell");
  if (!pageRoot || !shell) {
    return;
  }

  const listView = pageRoot.dataset.liveEmailView || "";
  if (!listView) {
    return;
  }

  const pollIntervalMs = Number(pageRoot.dataset.pollIntervalMs || "5000");
  const sortSelect = document.querySelector(".sort-select");
  let currentSort = pageRoot.dataset.sort || "date_desc";
  let lastFingerprint = pageRoot.dataset.fingerprint || "";
  let timerId = null;
  let inFlight = false;

  const buildUrl = () => {
    const params = new URLSearchParams();
    params.set("view", listView);
    params.set("sort", currentSort);
    params.set("next", `${window.location.pathname}${window.location.search}`);
    params.set("t", Date.now().toString());
    return `/api/list-emails?${params.toString()}`;
  };

  const refreshList = async () => {
    if (inFlight) {
      return;
    }
    inFlight = true;
    try {
      const response = await fetch(buildUrl(), {
        method: "GET",
        headers: {
          Accept: "application/json",
        },
        cache: "no-store",
      });
      if (!response.ok) {
        return;
      }
      const payload = await response.json();
      if (!payload || typeof payload.html !== "string") {
        return;
      }
      if (payload.fingerprint && payload.fingerprint === lastFingerprint) {
        return;
      }
      shell.innerHTML = payload.html;
      lastFingerprint = payload.fingerprint || "";
      pageRoot.dataset.fingerprint = lastFingerprint;
    } catch (error) {
      return;
    } finally {
      inFlight = false;
    }
  };

  const stopPolling = () => {
    if (timerId === null) {
      return;
    }
    window.clearInterval(timerId);
    timerId = null;
  };

  const startPolling = () => {
    stopPolling();
    timerId = window.setInterval(refreshList, pollIntervalMs);
  };

  if (sortSelect) {
    sortSelect.addEventListener("change", (event) => {
      currentSort = event.target.value || currentSort;
    });
  }

  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      stopPolling();
      return;
    }
    refreshList();
    startPolling();
  });

  window.addEventListener("beforeunload", stopPolling);
  startPolling();
})();
