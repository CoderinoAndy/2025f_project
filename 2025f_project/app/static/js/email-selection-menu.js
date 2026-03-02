(() => {
  const shell = document.querySelector(".list-shell");
  const sideMenu = document.querySelector("[data-bulk-side-menu]");
  if (!shell || !sideMenu) {
    return;
  }

  const listHeader = document.querySelector(".list-header");
  const sortForm = document.querySelector(".sort-form");
  let selectionMode = false;

  const selectToggleButton = document.createElement("button");
  selectToggleButton.type = "button";
  selectToggleButton.className = "list-select-toggle-btn";
  selectToggleButton.textContent = "Select";
  selectToggleButton.setAttribute("aria-pressed", "false");
  selectToggleButton.setAttribute("aria-label", "Toggle selection mode");
  if (sortForm) {
    sortForm.appendChild(selectToggleButton);
  } else if (listHeader) {
    listHeader.appendChild(selectToggleButton);
  } else {
    return;
  }

  const selectedCount = sideMenu.querySelector("[data-selected-count]");
  const clearButton = sideMenu.querySelector("[data-clear-selection]");
  const form = sideMenu.querySelector("[data-bulk-form]");
  const idsInput = sideMenu.querySelector("[data-selected-ids-input]");
  const actionInput = sideMenu.querySelector("[data-selected-action-input]");
  const typeInput = sideMenu.querySelector("[data-selected-type-input]");
  const actionButtons = Array.from(
    sideMenu.querySelectorAll("[data-bulk-action-btn]")
  );
  const moveGroup = sideMenu.querySelector(".bulk-move-group");

  if (
    !selectedCount ||
    !clearButton ||
    !form ||
    !idsInput ||
    !actionInput ||
    !typeInput
  ) {
    return;
  }

  const selectedIds = new Set();

  const parseBool = (value) => value === "1" || value === "true";
  const listRows = () =>
    Array.from(shell.querySelectorAll(".email-row[data-email-id]"));

  const selectedRows = () =>
    listRows().filter((row) => selectedIds.has(row.dataset.emailId));

  const sanitizeSelection = () => {
    const visibleIds = new Set(listRows().map((row) => row.dataset.emailId));
    Array.from(selectedIds).forEach((id) => {
      if (!visibleIds.has(id)) {
        selectedIds.delete(id);
      }
    });
  };

  const syncCheckboxes = () => {
    listRows().forEach((row) => {
      const isSelected = selectedIds.has(row.dataset.emailId);
      const checkbox = row.querySelector(".email-select-checkbox");
      if (checkbox) {
        checkbox.checked = isSelected;
        checkbox.tabIndex = selectionMode ? 0 : -1;
      }
      row.classList.toggle("selected", isSelected);
    });
  };

  const computeCapabilities = (rows) => {
    const state = {
      delete: false,
      archive: false,
      unarchive: false,
      markRead: false,
      markUnread: false,
      move: false,
    };
    rows.forEach((row) => {
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

  const refreshMenu = () => {
    sanitizeSelection();
    syncCheckboxes();

    const count = selectedIds.size;
    idsInput.value = Array.from(selectedIds).join(",");
    selectedCount.textContent = String(count);

    const menuVisible = selectionMode && count > 0;
    sideMenu.classList.toggle("visible", menuVisible);
    sideMenu.setAttribute("aria-hidden", menuVisible ? "false" : "true");

    const rows = selectedRows();
    const capabilities = computeCapabilities(rows);
    const movableRows = rows.filter((row) => parseBool(row.dataset.canMove));
    actionButtons.forEach((button) => {
      let enabled = false;
      const action = button.dataset.action;
      if (menuVisible) {
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
          const targetType = button.dataset.newType || "";
          enabled =
            capabilities.move &&
            movableRows.some((row) => row.dataset.emailType !== targetType);
        }
      }
      button.disabled = !enabled;
      button.classList.toggle("is-hidden", !enabled);
    });

    if (moveGroup) {
      moveGroup.classList.toggle(
        "is-hidden",
        !menuVisible || !capabilities.move
      );
    }
  };

  shell.addEventListener("change", (event) => {
    const checkbox = event.target.closest(".email-select-checkbox");
    if (!checkbox) {
      return;
    }
    if (!selectionMode) {
      checkbox.checked = false;
      return;
    }
    const row = checkbox.closest(".email-row[data-email-id]");
    if (!row) {
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

  shell.addEventListener("click", (event) => {
    if (!selectionMode) {
      return;
    }
    if (event.target.closest(".email-select-wrap")) {
      return;
    }
    if (event.target.closest("button, a, form, .dropdown-menu, .row-actions")) {
      return;
    }
    const row = event.target.closest(".email-row[data-email-id]");
    if (!row) {
      return;
    }
    const checkbox = row.querySelector(".email-select-checkbox");
    if (!checkbox) {
      return;
    }
    checkbox.checked = !checkbox.checked;
    const emailId = row.dataset.emailId;
    if (checkbox.checked) {
      selectedIds.add(emailId);
    } else {
      selectedIds.delete(emailId);
    }
    refreshMenu();
  });

  clearButton.addEventListener("click", () => {
    selectedIds.clear();
    refreshMenu();
  });

  actionButtons.forEach((button) => {
    button.addEventListener("click", () => {
      if (button.disabled || selectedIds.size === 0) {
        return;
      }
      actionInput.value = button.dataset.action || "";
      typeInput.value = button.dataset.newType || "";
      if (!actionInput.value) {
        return;
      }
      form.submit();
    });
  });

  const setSelectionMode = (enabled) => {
    selectionMode = enabled;
    shell.classList.toggle("selection-mode", enabled);
    selectToggleButton.classList.toggle("active", enabled);
    selectToggleButton.textContent = enabled ? "Done" : "Select";
    selectToggleButton.setAttribute("aria-pressed", enabled ? "true" : "false");
    if (!enabled) {
      selectedIds.clear();
    }
    refreshMenu();
  };

  selectToggleButton.addEventListener("click", () => {
    setSelectionMode(!selectionMode);
  });

  let refreshScheduled = false;
  const scheduleRefresh = () => {
    if (refreshScheduled) {
      return;
    }
    refreshScheduled = true;
    window.requestAnimationFrame(() => {
      refreshScheduled = false;
      refreshMenu();
    });
  };

  const observer = new MutationObserver(() => {
    scheduleRefresh();
  });
  observer.observe(shell, { childList: true, subtree: true });

  setSelectionMode(false);
})();
