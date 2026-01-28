const $ = (id) => document.getElementById(id);

const grid = $("grid");
const loadBtn = $("loadBtn");
const requestIdEl = $("requestId");
const countEl = $("count");
const statusEl = $("status");
const userIdInput = $("userId");
const toastRoot = $("toast");

let currentRequestId = null;
let currentUserId = 1;

function setStatus(s) {
  statusEl.textContent = s;
}

function toast(msg, ok = true) {
  const el = document.createElement("div");
  el.className = "toast-item " + (ok ? "toast-ok" : "toast-bad");
  el.textContent = msg;
  toastRoot.appendChild(el);

  setTimeout(() => {
    el.style.opacity = "0";
    el.style.transition = "opacity 200ms ease";
  }, 2200);

  setTimeout(() => el.remove(), 2600);
}

function escapeHTML(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatDate(iso) {
  if (!iso) return "—";
  try { return new Date(iso).toLocaleString(); } catch { return "—"; }
}

function renderItems(items) {
  grid.innerHTML = "";

  items.forEach((it, idx) => {
    const articleId = it.article_id;
    const title = escapeHTML(it.title || "(no title)");
    const source = escapeHTML(it.source || "—");
    const category = escapeHTML(it.category || "—");
    const published = escapeHTML(formatDate(it.published_at));
    const url = it.url || null;
    const imageUrl = it.image_url || "";

    const card = document.createElement("div");
    card.className = "card";

    const content = document.createElement("div");
    content.className = "content";

    const text = document.createElement("div");
    text.className = "text";

    const left = document.createElement("div");
    left.innerHTML = `
      <h2>${idx + 1}. ${title}</h2>
      <div class="line">
        <span class="badge">${category}</span>
        <span>Source: <strong>${source}</strong></span>
        <span>Published: <strong>${published}</strong></span>
        <span>Link: ${
          url
            ? `<a href="${escapeHTML(url)}" target="_blank" rel="noopener noreferrer">open</a>`
            : "<span style='opacity:.45'>—</span>"
        }</span>
      </div>
    `;

    text.appendChild(left);
    content.appendChild(text); 

    if (imageUrl) {
      const media = document.createElement("div");
      media.className = "thumb";

      const img = document.createElement("img");
      img.src = imageUrl;
      img.alt = title;
      img.loading = "lazy";
      img.referrerPolicy = "no-referrer";
      img.onerror = () => media.remove();

      media.appendChild(img);
      content.appendChild(media); // ✅ μετά η εικόνα -> δεξιά
    } else {
      content.classList.add("no-thumb");
    }

    const actions = document.createElement("div");
    actions.className = "actions";

    const likeBtn = document.createElement("button");
    likeBtn.className = "btn-like";
    likeBtn.textContent = "Like";

    const shareBtn = document.createElement("button");
    shareBtn.className = "btn-share";
    shareBtn.textContent = "Share";

    const dislikeBtn = document.createElement("button");
    dislikeBtn.className = "btn-dislike";
    dislikeBtn.textContent = "Dislike";

    actions.appendChild(likeBtn);
    actions.appendChild(shareBtn);
    actions.appendChild(dislikeBtn);

    card.appendChild(content);
    card.appendChild(actions);
    grid.appendChild(card);

    // Hook button → POST /feed/interact
    likeBtn.addEventListener("click", () => sendInteraction(articleId, "like"));
    shareBtn.addEventListener("click", () => sendInteraction(articleId, "share"));
    dislikeBtn.addEventListener("click", () => sendInteraction(articleId, "dislike"));
  });
}

async function loadFeed() {
  const userId = Number(userIdInput.value);
  if (!Number.isInteger(userId) || userId <= 0) {
    toast("Invalid user_id", false);
    return;
  }
  currentUserId = userId;

  loadBtn.disabled = true;
  setStatus("loading…");

  try {
    // Your router is mounted at /feed, so GET /feed?user_id=1&k=100 hits router.get("/")
    const resp = await fetch(`/feed?user_id=${encodeURIComponent(userId)}`);
    if (!resp.ok) {
      const t = await resp.text();
      throw new Error(`GET /feed failed: ${resp.status} ${t}`);
    }

    const data = await resp.json();
    currentRequestId = data.request_id;
    const items = data.items || [];

    requestIdEl.textContent = currentRequestId || "—";
    countEl.textContent = String(items.length);
    setStatus("loaded");

    renderItems(items);
    toast(`Loaded feed: ${items.length} items`, true);
  } catch (err) {
    console.error(err);
    setStatus("error");
    toast(err.message || String(err), false);
  } finally {
    loadBtn.disabled = false;
  }
}

async function sendInteraction(articleId, interactionType) {
  if (!currentRequestId) {
    toast("Load feed first (missing request_id).", false);
    return;
  }

  try {
    const payload = {
      user_id: currentUserId,
      request_id: currentRequestId,
      article_id: Number(articleId),
      interaction_type: interactionType,
      dwell_ms: null
    };

    // Mounted router: POST /feed/interact
    const resp = await fetch("/feed/interact", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const data = await resp.json().catch(() => ({}));

    if (!resp.ok) {
      throw new Error(data.error || `POST /feed/interact failed: ${resp.status}`);
    }

    toast(`Interaction logged: ${interactionType} (article_id=${articleId})`, true);
  } catch (err) {
    console.error(err);
    toast(err.message || String(err), false);
  }
}

loadBtn.addEventListener("click", loadFeed);
