// MVC: Controller
// Submits per-row mailbox actions with fetch and refreshes the list on success.
(() => { // Submit row action forms asynchronously to avoid full-page reloads.
  const shell = document.querySelector(".list-shell"); // List container where action forms live.
  if (!shell) { // Exit on pages that do not render mailbox rows.
    return;
  }

  const requestRefresh = () => { // Ask the live list module to rerender rows in place.
    if (typeof window.mailboxRefreshList === "function") {
      window.mailboxRefreshList();
      return;
    }
    window.dispatchEvent(new Event("mailbox:refresh-requested"));
  };

  const setFormBusyState = (form, isBusy) => { // Prevent double-submits while a request is in flight.
    const submitButtons = form.querySelectorAll(
      "button[type='submit'], input[type='submit']"
    );
    submitButtons.forEach((button) => {
      button.disabled = isBusy;
    });
  };

  const submitForm = async (form) => { // Send form data with fetch while keeping existing routes unchanged.
    const method = (form.getAttribute("method") || "post").toUpperCase();
    const response = await fetch(form.action, {
      method,
      body: new FormData(form),
      headers: {
        "X-Requested-With": "fetch",
      },
      credentials: "same-origin",
    });
    return response.ok;
  };

  shell.addEventListener("submit", async (event) => { // Intercept row action submits only.
    const form = event.target.closest("form");
    if (!form || !form.closest(".row-actions")) {
      return;
    }
    if (form.dataset.submitting === "1") {
      event.preventDefault();
      return;
    }

    event.preventDefault();
    form.dataset.submitting = "1";
    setFormBusyState(form, true);
    try {
      const ok = await submitForm(form);
      if (ok) {
        requestRefresh();
      }
    } catch (error) {
      return;
    } finally {
      setFormBusyState(form, false);
      form.dataset.submitting = "0";
    }
  });
})();
