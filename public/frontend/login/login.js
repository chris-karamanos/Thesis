function $(sel) {
  return document.querySelector(sel);
}

function setStatus(el, message = "", type = "info") {
  if (!el) return;
  el.textContent = message;
  el.className = `status ${type}`;
  el.style.display = message ? "block" : "none";
}

async function api(path, body) {
  const res = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    credentials: "same-origin",
    body: JSON.stringify(body),
  });

  let data = null;
  try { data = await res.json(); } catch (_) {}

  if (!res.ok) {
    const errMsg = data?.error ? data.error : `Request failed (${res.status})`;
    throw new Error(errMsg);
  }
  return data;
}

function resetLoginUI() {
  const pwd = document.querySelector('#loginForm input[name="password"]');
  if (pwd) pwd.value = "";

  const statusEl = $("#status");
  if (statusEl) {
    statusEl.textContent = "";
    statusEl.className = "status";
    statusEl.style.display = "none";
  }
}

function openModal(overlay) {
  overlay.classList.add("is-open");
  overlay.setAttribute("aria-hidden", "false");
}

function closeModal(overlay) {
  overlay.classList.remove("is-open");
  overlay.setAttribute("aria-hidden", "true");
}

// reset login form and status on page show 
window.addEventListener("pageshow", () => {
  resetLoginUI();
});

// if there's an active session, redirect to feed
(async () => {
  try {
    const res = await fetch("/auth/me", { credentials: "same-origin" });
    if (res.ok) {
      const data = await res.json().catch(() => null);
      if (data?.loggedIn) window.location.href = "/feed/";
    }
  } catch (_) {}
})();

document.addEventListener("DOMContentLoaded", () => {
  const form = $("#loginForm");
  const statusEl = $("#status");

  const signupBtn = $("#signupBtn");
  const overlay = $("#signupOverlay");
  const closeBtn = $("#closeSignup");
  const cancelBtn = $("#cancelSignup");

  const signupForm = $("#signupForm");
  const signupStatus = $("#signupStatus");

  resetLoginUI();

  // sign in
  form.addEventListener("submit", async (e) => {
    e.preventDefault();

    const fd = new FormData(form);
    const identifier = String(fd.get("identifier") || "").trim();
    const password = String(fd.get("password") || "");

    if (!identifier || !password) {
      setStatus(statusEl, "Please provide both username/email and password.", "error");
      return;
    }

    try {
      setStatus(statusEl, "Signing in…", "info");
      await api("/auth/login", { identifier, password });

      setStatus(statusEl, "Success! Redirecting…", "success");
      window.location.href = "/feed/";
    } catch (err) {
      setStatus(statusEl, err.message, "error");
      // clear password field on error for security
      const pwd = form.querySelector('input[name="password"]');
      if (pwd) pwd.value = "";
    }
  });

  // sign up modal open
  signupBtn.addEventListener("click", () => {
    signupForm.reset();
    setStatus(signupStatus, "", "info");
    openModal(overlay);
    signupForm.querySelector('input[name="identifier"]').focus();
  });

  // close modal
  closeBtn.addEventListener("click", () => closeModal(overlay));
  cancelBtn.addEventListener("click", () => closeModal(overlay));
  overlay.addEventListener("click", (e) => {
    if (e.target === overlay) closeModal(overlay);
  });
  window.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && overlay.classList.contains("is-open")) {
      closeModal(overlay);
    }
  });

  // submit signup
  signupForm.addEventListener("submit", async (e) => {
    e.preventDefault();

    const fd = new FormData(signupForm);
    const identifier = String(fd.get("identifier") || "").trim();
    const password = String(fd.get("password") || "");

    if (!identifier || !password) {
      setStatus(signupStatus, "Please fill in all fields.", "error");
      return;
    }

    try {
      setStatus(signupStatus, "Creating account…", "info");
      await api("/auth/signup", { identifier, password });

      setStatus(signupStatus, "Account created! Redirecting…", "success");
      setTimeout(() => {
        window.location.href = "/feed/";
      }, 350);
    } catch (err) {
      setStatus(signupStatus, err.message, "error");
      const pwd = signupForm.querySelector('input[name="password"]');
      if (pwd) pwd.value = "";
    }
  });
});
