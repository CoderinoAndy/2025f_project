// Controller-side behavior.
// Handle row selection mode and bulk actions on mailbox list pages.
(() => { // Handle selection mode and bulk actions for mailbox rows.
  const shell = document.querySelector(".list-shell"); // List container that receives live row updates.
  const sideMenu = document.querySelector("[data-bulk-side-menu]"); // Floating menu for bulk actions.
  if (!shell || !sideMenu) { // Exit on pages that do not render bulk-selection UI.
    return;
  }

  const listHeader = document.querySelector(".list-header"); // Header fallback for toggle button placement.
  const sortForm = document.querySelector(".sort-form"); // Preferred location for the Select/Done button.
  let selectionMode = false; // True while click-to-select behavior is active.
  let bulkRequestInFlight = false; // Prevent concurrent bulk submissions.

  const selectToggleButton = document.createElement("button"); // Button created in JS so templates stay clean.
  selectToggleButton.type = "button"; // Prevent form submission when clicked.
  selectToggleButton.className = "list-select-toggle-btn"; // Reuse existing mailbox button styling.
  selectToggleButton.textContent = "Select"; // Default label before entering selection mode.
  selectToggleButton.setAttribute("aria-pressed", "false"); // Accessibility: initial toggle state.
  selectToggleButton.setAttribute("aria-label", "Toggle selection mode"); // Accessibility: clear control purpose.
  const selectAllButton = document.createElement("button"); // Secondary control appears only in selection mode.
  selectAllButton.type = "button"; // Prevent form submission when clicked.
  selectAllButton.className = "list-select-toggle-btn"; // Match the Select/Done button styling.
  selectAllButton.textContent = "Select all"; // Copy requested by the user.
  selectAllButton.hidden = true; // Only reveal this button while selection mode is active.
  selectAllButton.setAttribute("aria-label", "Select all emails in this view"); // Accessibility: clarify scope.
  selectAllButton.setAttribute("aria-pressed", "false"); // Accessibility: explicit toggle semantics.
  if (sortForm) { // Place next to sort controls when available.
    sortForm.appendChild(selectToggleButton);
    sortForm.appendChild(selectAllButton);
  } else if (listHeader) { // Fallback mount point if sort form is not present.
    listHeader.appendChild(selectToggleButton);
    listHeader.appendChild(selectAllButton);
  } else { // No valid mount point means this view cannot support selection.
    return;
  }

  const selectedCount = sideMenu.querySelector("[data-selected-count]"); // Counter text in bulk menu header.
  const clearButton = sideMenu.querySelector("[data-clear-selection]"); // "Clear" action in bulk menu.
  const form = sideMenu.querySelector("[data-bulk-form]"); // Form posted for bulk server actions.
  const idsInput = sideMenu.querySelector("[data-selected-ids-input]"); // Hidden comma-separated selected IDs.
  const actionInput = sideMenu.querySelector("[data-selected-action-input]"); // Hidden selected action code.
  const typeInput = sideMenu.querySelector("[data-selected-type-input]"); // Hidden destination type for moves.
  const scopeInput = sideMenu.querySelector("[data-selection-scope-input]"); // Hidden selection scope for all-vs-explicit behavior.
  const listViewInput = sideMenu.querySelector("[data-list-view-input]"); // Hidden mailbox-view identifier for server-side select-all resolution.
  const searchQueryInput = sideMenu.querySelector("[data-search-query-input]"); // Hidden search filter so select-all honors the current query.
  const actionButtons = Array.from( // All buttons that trigger a bulk action.
    sideMenu.querySelectorAll("[data-bulk-action-btn]")
  );
  const moveGroup = sideMenu.querySelector(".bulk-move-group"); // Optional section for type-move actions.

  if ( // Guard against missing required DOM hooks.
    !selectedCount ||
    !clearButton ||
    !form ||
    !idsInput ||
    !actionInput ||
    !typeInput ||
    !scopeInput ||
    !listViewInput
  ) {
    return;
  }

  const selectedIds = new Set(); // Single source of truth for selected email IDs.
  const bodyDataset = document.body ? document.body.dataset : {}; // Read page-level mailbox metadata once on load.
  const listView = (bodyDataset.liveEmailView || "").trim(); // Stable mailbox view code used by the server for whole-view actions.
  const pageSearchQuery = bodyDataset.searchQuery || ""; // Current search filter that whole-view actions must preserve.
  let allMatchingSelected = false; // True after "Select all" chooses every email in the current mailbox view.

  listViewInput.value = listView; // Keep the bulk form aligned with the current mailbox tab.
  if (searchQueryInput) {
    searchQueryInput.value = pageSearchQuery;
  }

  const requestRefresh = () => { // Ask live list script to refresh rows in place.
    if (typeof window.mailboxRefreshList === "function") {
      window.mailboxRefreshList();
      return;
    }
    window.dispatchEvent(new Event("mailbox:refresh-requested"));
  };

  const submitBulkAction = async () => { // Post bulk action without leaving the current page.
    const method = (form.getAttribute("method") || "post").toUpperCase();
    const actionUrl = form.getAttribute("action") || (typeof form.action === "string" ? form.action : "");
    if (!actionUrl) {
      return false;
    }
    const response = await fetch(actionUrl, {
      method,
      body: new FormData(form),
      headers: {
        "X-Requested-With": "fetch",
      },
      credentials: "same-origin",
    });
    return response.ok;
  };

  const parseBool = (value) => value === "1" || value === "true"; // Normalize bool-ish dataset strings.
  const listRows = () => // Read current rows each time because live polling can replace DOM nodes.
    Array.from(shell.querySelectorAll(".email-row[data-email-id]"));

  const selectedRows = () => // Resolve selected rows from IDs right before each capability check.
    listRows().filter((row) => selectedIds.has(row.dataset.emailId));

  const totalMatchingCount = () => { // Read the full mailbox count so select-all spans every page in this view.
    const pagination = document.querySelector("[data-mailbox-pagination]");
    const rawCount = pagination && pagination.dataset ? pagination.dataset.totalCount : "";
    const parsedCount = Number.parseInt(rawCount || "", 10);
    if (Number.isFinite(parsedCount) && parsedCount >= 0) {
      return parsedCount;
    }
    return listRows().length;
  };

  const currentSelectionCount = () => // One place to read the active selection size for guards and UI.
    (allMatchingSelected ? totalMatchingCount() : selectedIds.size);

  const isRowSelected = (row) => // Whole-view selection treats every currently rendered row as selected.
    allMatchingSelected || selectedIds.has(row.dataset.emailId);

  const fixedTypeView = () => { // Views tied to a single mailbox type should hide "move to same type" when all are selected.
    if (
      listView === "read-only" ||
      listView === "response-needed" ||
      listView === "junk" ||
      listView === "junk-uncertain"
    ) {
      return listView;
    }
    return "";
  };

  const adoptVisibleSelection = () => { // Dropping out of whole-view mode falls back to the currently rendered checkbox state.
    allMatchingSelected = false;
    selectedIds.clear();
    listRows().forEach((row) => {
      const checkbox = row.querySelector(".email-select-checkbox");
      if (checkbox && checkbox.checked) {
        selectedIds.add(row.dataset.emailId);
      }
    });
  };

  const sanitizeSelection = () => { // Drop IDs that are no longer present after live refreshes.
    if (allMatchingSelected) {
      return;
    }
    const visibleIds = new Set(listRows().map((row) => row.dataset.emailId)); // Fast lookup of visible row IDs.
    Array.from(selectedIds).forEach((id) => { // Iterate snapshot so deletion is safe during traversal.
      if (!visibleIds.has(id)) {
        selectedIds.delete(id);
      }
    });
  };

  const syncCheckboxes = () => { // Mirror Set state into row checkboxes and visual row highlight.
    listRows().forEach((row) => {
      // Read selection state from the Set so row replacements from live polling stay consistent.
      const isSelected = isRowSelected(row);
      const checkbox = row.querySelector(".email-select-checkbox"); // Checkbox shown only in selection mode.
      if (checkbox) {
        checkbox.checked = isSelected; // Keep checkbox UI in sync with Set.
        checkbox.tabIndex = selectionMode ? 0 : -1; // Remove hidden checkboxes from keyboard tab order.
      }
      row.classList.toggle("selected", isSelected); // Row highlight for selected emails.
    });
  };

  const computeCapabilities = (rows) => { // Aggregate which actions are valid for selected rows.
    const state = {
      delete: false,
      archive: false,
      unarchive: false,
      markRead: false,
      markUnread: false,
      move: false,
    };
    rows.forEach((row) => { // Any row enabling an action enables that action globally.
      state.delete = state.delete || parseBool(row.dataset.canDelete);
      state.archive = state.archive || parseBool(row.dataset.canArchive);
      state.unarchive = state.unarchive || parseBool(row.dataset.canUnarchive);
      state.markRead = state.markRead || parseBool(row.dataset.canMarkRead);
      state.markUnread =
        state.markUnread || parseBool(row.dataset.canMarkUnread);
      state.move = state.move || parseBool(row.dataset.canMove);
    });
    return state;
  };

  const computeAllMatchingCapabilities = () => { // Whole-view select-all can infer capabilities from the active mailbox tab.
    const hasRows = totalMatchingCount() > 0;
    const state = {
      delete: hasRows,
      archive: false,
      unarchive: false,
      markRead: false,
      markUnread: false,
      move: false,
    };
    if (!hasRows) {
      return state;
    }
    if (listView === "sent" || listView === "draft") {
      return state;
    }
    state.move = true;
    state.markRead = true;
    state.markUnread = true;
    if (listView === "archived") {
      state.unarchive = true;
    } else {
      state.archive = true;
    }
    return state;
  };

  const refreshMenu = () => { // Recompute visible state whenever selection or rows change.
    sanitizeSelection(); // Remove stale IDs first.
    syncCheckboxes(); // Then update visual selection state.

    const count = currentSelectionCount(); // Number of selected rows drives visibility/labels.
    idsInput.value = allMatchingSelected ? "" : Array.from(selectedIds).join(","); // Whole-view selection lets the server resolve the matching IDs.
    scopeInput.value = allMatchingSelected ? "all" : "explicit"; // Tell the server whether it should expand the current view.
    listViewInput.value = listView; // Keep form metadata in sync even after live list refreshes.
    if (searchQueryInput) {
      searchQueryInput.value = pageSearchQuery;
    }
    selectedCount.textContent = String(count); // Update counter in side menu header.
    selectAllButton.classList.toggle("active", selectionMode && allMatchingSelected); // Show whether the whole view is selected.
    selectAllButton.textContent = allMatchingSelected ? "All selected" : "Select all"; // Give the user clear feedback about current scope.
    selectAllButton.setAttribute("aria-pressed", allMatchingSelected ? "true" : "false"); // Keep ARIA state aligned with the scope toggle.
    selectAllButton.disabled =
      bulkRequestInFlight || !selectionMode || totalMatchingCount() === 0; // Prevent empty or concurrent select-all requests.

    const menuVisible = selectionMode && count > 0; // Show menu only in mode + with active selection.
    sideMenu.classList.toggle("visible", menuVisible); // Toggle open/closed menu styles.
    sideMenu.setAttribute("aria-hidden", menuVisible ? "false" : "true"); // Keep ARIA visibility accurate.

    const rows = selectedRows(); // Selected row nodes used for capability checks.
    const capabilities = allMatchingSelected
      ? computeAllMatchingCapabilities()
      : computeCapabilities(rows); // Whole-view selection uses the mailbox view rules instead of the current page only.
    const movableRows = rows.filter((row) => parseBool(row.dataset.canMove)); // Rows that allow set-type moves.
    actionButtons.forEach((button) => {
      let enabled = false; // Default disabled until action-specific rules pass.
      const action = button.dataset.action; // Action code posted to backend.
      if (menuVisible) { // Never enable actions while menu is hidden.
        if (action === "delete") {
          enabled = capabilities.delete;
        } else if (action === "archive") {
          enabled = capabilities.archive;
        } else if (action === "unarchive") {
          enabled = capabilities.unarchive;
        } else if (action === "mark-read") {
          enabled = capabilities.markRead;
        } else if (action === "mark-unread") {
          enabled = capabilities.markUnread;
        } else if (action === "set-type") {
          const targetType = button.dataset.newType || ""; // Destination folder/type for move action.
          if (allMatchingSelected) {
            enabled =
              capabilities.move &&
              targetType !== fixedTypeView(); // Type-specific tabs should not offer a no-op move target.
          } else {
            enabled =
              capabilities.move && // At least one selected row can move.
              movableRows.some((row) => row.dataset.emailType !== targetType); // Only show move if it changes type.
          }
        }
      }
      button.disabled = bulkRequestInFlight || !enabled; // Disable invalid operations and while request is in flight.
      button.classList.toggle("is-hidden", !enabled); // Hide disabled actions to reduce menu clutter.
    });

    if (moveGroup) {
      moveGroup.classList.toggle(
        "is-hidden",
        !menuVisible || !capabilities.move // Hide move group when no selected row supports moving.
      );
    }
  };

  shell.addEventListener("change", (event) => { // Listen for checkbox toggles within list rows.
    const checkbox = event.target.closest(".email-select-checkbox"); // Ignore unrelated change events.
    if (!checkbox) {
      return;
    }
    if (!selectionMode) { // Prevent accidental state changes when mode is off.
      checkbox.checked = false;
      return;
    }
    const row = checkbox.closest(".email-row[data-email-id]"); // Resolve row metadata for this checkbox.
    if (!row) {
      return;
    }
    if (allMatchingSelected) { // Any per-row adjustment exits whole-view mode and keeps the visible page selection.
      adoptVisibleSelection();
      refreshMenu();
      return;
    }
    const emailId = row.dataset.emailId; // Stable ID used by backend bulk endpoint.
    if (checkbox.checked) {
      selectedIds.add(emailId);
    } else {
      selectedIds.delete(emailId);
    }
    refreshMenu(); // Recompute menu/button state after each selection change.
  });

  shell.addEventListener("click", (event) => { // Support row-click selection beyond tiny checkbox target.
    if (!selectionMode) { // Keep normal row navigation when mode is off.
      return;
    }
    if (event.target.closest(".email-select-wrap")) { // Checkbox wrapper already handled by change listener.
      return;
    }
    if (event.target.closest("button, a, form, .dropdown-menu, .row-actions")) { // Don't hijack interactive controls.
      return;
    }
    const row = event.target.closest(".email-row[data-email-id]"); // Toggle whichever row the user clicked.
    if (!row) {
      return;
    }
    const checkbox = row.querySelector(".email-select-checkbox"); // Use checkbox as canonical row toggle.
    if (!checkbox) {
      return;
    }
    checkbox.checked = !checkbox.checked; // Mirror row click into checkbox state.
    if (allMatchingSelected) { // Row tweaks after select-all revert to explicit page-local selection.
      adoptVisibleSelection();
      refreshMenu();
      return;
    }
    const emailId = row.dataset.emailId;
    if (checkbox.checked) {
      selectedIds.add(emailId);
    } else {
      selectedIds.delete(emailId);
    }
    refreshMenu();
  });

  clearButton.addEventListener("click", () => { // Manual reset from side menu header.
    allMatchingSelected = false;
    selectedIds.clear();
    refreshMenu();
  });

  selectAllButton.addEventListener("click", () => { // One-click helper for selecting every email in the current mailbox view.
    if (!selectionMode || totalMatchingCount() === 0) {
      return;
    }
    allMatchingSelected = true;
    selectedIds.clear();
    refreshMenu();
  });

  actionButtons.forEach((button) => {
    button.addEventListener("click", async () => { // Submit selected action and IDs to backend.
      if (button.disabled || currentSelectionCount() === 0) { // Guard against invalid/empty submissions.
        return;
      }
      if (bulkRequestInFlight) { // Ignore repeated clicks while a request is active.
        return;
      }
      actionInput.value = button.dataset.action || ""; // Backend action verb.
      typeInput.value = button.dataset.newType || ""; // Optional target type for set-type.
      if (!actionInput.value) {
        return;
      }
      bulkRequestInFlight = true;
      refreshMenu();
      let requestSucceeded = false;
      try {
        requestSucceeded = await submitBulkAction();
      } catch (error) {
        requestSucceeded = false;
      } finally {
        bulkRequestInFlight = false;
      }
      if (requestSucceeded) {
        allMatchingSelected = false;
        selectedIds.clear();
      }
      refreshMenu();
      if (requestSucceeded) {
        requestRefresh();
      }
    });
  });

  const setSelectionMode = (enabled) => { // Centralized mode switch keeps all related UI state consistent.
    selectionMode = enabled;
    shell.classList.toggle("selection-mode", enabled); // Shows/hides checkboxes via CSS.
    selectToggleButton.classList.toggle("active", enabled); // Visual state for the Select/Done button.
    selectToggleButton.textContent = enabled ? "Done" : "Select"; // Keep button label in sync with mode.
    selectToggleButton.setAttribute("aria-pressed", enabled ? "true" : "false"); // ARIA toggle semantics.
    selectAllButton.hidden = !enabled; // "Select all" should only appear after entering selection mode.
    if (!enabled) {
      allMatchingSelected = false; // Exiting mode also clears whole-view selection state.
      selectedIds.clear(); // Exiting mode clears selection so row navigation feels normal.
    }
    refreshMenu();
  };

  selectToggleButton.addEventListener("click", () => {
    setSelectionMode(!selectionMode); // Toggle between Select and Done states.
  });

  let refreshScheduled = false; // Debounce flag for refreshes triggered by DOM mutations.
  const scheduleRefresh = () => {
    if (refreshScheduled) { // Combine multiple mutation events into one refresh.
      return;
    }
    refreshScheduled = true;
    window.requestAnimationFrame(() => {
      refreshScheduled = false;
      refreshMenu(); // Refresh after DOM settles from live list replacement.
    });
  };

  const observer = new MutationObserver(() => { // Watch for row replacements from polling script.
    scheduleRefresh();
  });
  observer.observe(shell, { childList: true, subtree: true }); // Track direct and nested row changes.

  setSelectionMode(false); // Initialize in non-selection mode on first load.
})();
