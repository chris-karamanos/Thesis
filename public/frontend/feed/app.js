const $ = (id) => document.getElementById(id);

const grid = $("grid");
const toastRoot = $("toast");
const usernameEl = $("username");
const logoutBtn = $("logoutBtn");
const divSlider = $("divSlider");
const divValue = $("divValue");
const divApplyBtn = $("divApplyBtn");
const divStatus = $("divStatus");
const hintBox = document.getElementById("hintBox");
const diversityBar = document.getElementById("diversityBar");
const categorySelect = document.getElementById("categorySelect");

let currentRequestId = null;
let currentUserId = null;
let currentUsername = null;
let div=0.3;
let k=50;
let isLoadingFeed = false;
let isColdStart = false;
let selectedCategory = "__all__";
let fullFeedItems = [];



function showHint(text) {
  if (!hintBox) return;
  hintBox.textContent = text;
  hintBox.classList.remove("hidden");
}

function hideHint() {
  if (!hintBox) return;
  hintBox.classList.add("hidden");
}

function setDiversityControlsEnabled(enabled) {
  if (divSlider) divSlider.disabled = !enabled;
  if (divApplyBtn) divApplyBtn.disabled = !enabled;

  if (diversityBar) {
    diversityBar.classList.toggle("disabled", !enabled);
  }
}


function fmt2(x) {
  const v = Number(x);
  return Number.isFinite(v) ? v.toFixed(2) : "—";
}

function safeText(s) {
  return escapeHTML(String(s ?? ""));
}


function buildExplainHTML(it) {
  const rel = it.explain_relevance || null;
  const div = it.explain_diversity || null;

  // relevance bullets
  const relPos = Array.isArray(rel?.top_positive) ? rel.top_positive : [];
  const relNeg = Array.isArray(rel?.top_negative) ? rel.top_negative : [];

  const relBullets = relPos.length
    ? relPos.slice(0, 3).map((r) => `<li>${safeText(r.text)}</li>`).join("")
    : `<li>Μη διαθέσιμη επεξήγηση ομοιότητας.</li>`;

  const negText = relNeg.length ? safeText(relNeg[0].text) : "";

  // diversity bullets
  const msg = div?.message ? safeText(div.message) : "Μη διαθέσιμη επεξήγηση ποικιλίας.";
  const lam = div?.lambda_user;
  const maxSim = div?.max_sim_to_selected;

  const divBullets = `
    <li>${msg}</li>
    <li>λ = <strong>${fmt2(lam)}</strong>, Επανάληψη (μέγιστη ομοιότητα με ήδη επιλεγμένα άρθρα) = <strong>${fmt2(maxSim)}</strong></li>
  `;

  // optional numeric decomposition if present
  const comp = div?.mmr_components || null;
  const compLine = comp
    ? `<li>Συνιστώσες MMR: λ·Σχετικότητα=${fmt2(comp["lambda_rel"])}, (1-λ)·Επανάληψη=${fmt2(comp["(1-lambda)_redundancy"])}, Ποινές ποικιλίας=${fmt2(comp["penalties_total"])}</li>`
    : "";

  return `
    <div class="explain-section">
      <div class="explain-title">Επεξήγηση Ομοιότητας</div>
      <ul class="explain-list">
        ${relBullets}
        ${negText
          ? `<li class="explain-neg">
              <span class="explain-neg-title">Πιθανό μειονέκτημα:</span>
              <span class="explain-sub">${negText}</span>
            </li>`
          : ""
        }
      </ul>
    </div>

    <div class="explain-section">
      <div class="explain-title">Επεξήγηση Ποικιλίας</div>
      <ul class="explain-list">
        ${divBullets}
        ${compLine}
      </ul>
    </div>
  `;
}



async function loadSessionUser() {
  const resp = await fetch("/auth/me", { credentials: "same-origin" });

  // if not authenticated, redirect to login 
  if (resp.status === 401) {
    window.location.href = "/login/";
    return null;
  }

  if (!resp.ok) {
    const t = await resp.text().catch(() => "");
    throw new Error(`Auth check failed: ${resp.status} ${t}`);
  }

  const data = await resp.json().catch(() => null);

  if (!data || data.loggedIn === false) {
    window.location.href = "/login/";
    return null;
  }

  const u = data.user || null;
  currentUserId = u?.id ?? null;
  currentUsername = u?.username ?? null;

  if (usernameEl) usernameEl.textContent = currentUsername || "—";

  return data;
}


function setFeedLoadingUI(loading, message = "") {
  isLoadingFeed = loading;

  if (divApplyBtn) divApplyBtn.disabled = loading || isColdStart;
  if (divSlider) divSlider.disabled = loading || isColdStart;

  if (divStatus) divStatus.textContent = message || (loading ? "Updating…" : "");
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

function sliderToDiv() {
  // slider value is 0 to 10, div in 0.0 to 1.0 with step of 0.1
  const raw = Number(divSlider?.value ?? 3);
  return Math.round(raw) / 10;
}

function syncDivUiFromState() {
  if (divSlider) divSlider.value = String(Math.round(div * 10));
  if (divValue) divValue.textContent = div.toFixed(1);
}

function renderItems(items) {
  if (!grid) return;

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

    const titleBlock = url
      ? `
        <h2>
          ${idx + 1}.
          <a class="article-title-link"
             href="${escapeHTML(url)}"
             target="_blank"
             rel="noopener noreferrer"
             data-article-id="${articleId}"
             data-click-type="title">
            ${title}
          </a>
        </h2>
      `
      : `<h2>${idx + 1}. ${title}</h2>`;

    left.innerHTML = `
      ${titleBlock}
      <div class="line">
        <span class="badge">${category}</span>
        <span>Source: <strong>${source}</strong></span>
        <span>Published: <strong>${published}</strong></span>
      </div>
    `;

    text.appendChild(left);
    content.appendChild(text);

    // thumbnail
    if (imageUrl) {
      const media = document.createElement("div");
      media.className = "thumb";

      if (url) {
        const a = document.createElement("a");
        a.className = "article-image-link";
        a.href = url;
        a.target = "_blank";
        a.rel = "noopener noreferrer";
        a.dataset.articleId = String(articleId);
        a.dataset.clickType = "image";

        const img = document.createElement("img");
        img.src = imageUrl;
        img.alt = it.title || "(no title)";
        img.loading = "lazy";
        img.referrerPolicy = "no-referrer";
        img.onerror = () => media.remove();

        a.appendChild(img);
        media.appendChild(a);
      } else {
        const img = document.createElement("img");
        img.src = imageUrl;
        img.alt = it.title || "(no title)";
        img.loading = "lazy";
        img.referrerPolicy = "no-referrer";
        img.onerror = () => media.remove();
        media.appendChild(img);
      }

      content.appendChild(media);
    } else {
      content.classList.add("no-thumb");
    }

    // actions
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

    // explain only when NOT coldstart AND when explanations exist
    const hasExplain = !!(it.explain_relevance || it.explain_diversity);
    const showExplain = !isColdStart && hasExplain;

    if (showExplain) {
      const explainBtn = document.createElement("button");
      explainBtn.className = "btn-explain";
      explainBtn.type = "button";
      explainBtn.textContent = "Explain";
      actions.appendChild(explainBtn);

      const expl = document.createElement("div");
      expl.className = "explain-panel hidden";
      expl.innerHTML = buildExplainHTML(it);
      card.appendChild(expl);

      explainBtn.addEventListener("click", () => {
        const isHidden = expl.classList.contains("hidden");
        expl.classList.toggle("hidden", !isHidden);
        explainBtn.textContent = isHidden ? "Hide" : "Explain";
      });
    }

    grid.appendChild(card);

    // interactions
    likeBtn.addEventListener("click", () => sendInteraction(articleId, "like"));
    dislikeBtn.addEventListener("click", () => sendInteraction(articleId, "dislike"));

    shareBtn.addEventListener("click", async () => {
      const urlToCopy = it.url || "";
      if (!urlToCopy) {
        toast("No URL available for this article.", false);
        return;
      }
      try {
        if (navigator.clipboard && window.isSecureContext) {
          await navigator.clipboard.writeText(urlToCopy);
        } else {
          const ta = document.createElement("textarea");
          ta.value = urlToCopy;
          ta.style.position = "fixed";
          ta.style.left = "-9999px";
          ta.style.top = "-9999px";
          document.body.appendChild(ta);
          ta.focus();
          ta.select();
          document.execCommand("copy");
          ta.remove();
        }
        toast("Ο σύνδεσμος αντιγράφηκε ✅", true);
        sendInteraction(articleId, "share");
      } catch (e) {
        console.error("Clipboard copy failed:", e);
        toast("Could not copy link to clipboard.", false);
        sendInteraction(articleId, "share");
      }
    });
  });

  // bind once: click interactions for title/image links 
  if (!grid.dataset.clickBound) {
    grid.addEventListener("click", (e) => {
      const a = e.target.closest("a[data-article-id]");
      if (!a) return;

      const articleId = Number(a.dataset.articleId);
      if (Number.isInteger(articleId) && articleId > 0) {
        sendInteraction(articleId, "click");
      }
    });

    grid.dataset.clickBound = "1";
  }
}


async function loadFeed() {

  if (!currentUserId) {
    await loadSessionUser();
    if (!currentUserId) return;
  }

  if (isLoadingFeed) return; // prevent double-fire
  setFeedLoadingUI(true, "Updating…");

  try {

    const resp = await fetch(
      `/api/feed?user_id=${encodeURIComponent(currentUserId)}&diversity_level=${div}&k=${k}`,
      { credentials: "same-origin" }
    );

    if (!resp.ok) {
      const t = await resp.text();
      throw new Error(`GET /feed failed: ${resp.status} ${t}`);
    }

    const data = await resp.json();
    currentRequestId = data.request_id;
    const items = data.items || [];

    const mode = data.debug?.mode || "personalized";
    isColdStart = mode === "coldstart";

    if (isColdStart) {
      showHint(" Για την δημιουργία προσωποποιημένης ροής ειδήσεων, αλληλεπιδράστε με το σύστημα.");
    } else {
      hideHint();
    }

    setDiversityControlsEnabled(!isColdStart);

    fullFeedItems = items;

    const visibleItems =
      selectedCategory && selectedCategory !== "__all__"
        ? fullFeedItems.filter((x) => x.category === selectedCategory)
        : fullFeedItems;

    renderItems(visibleItems);
    setFeedLoadingUI(false, "");
  } catch (err) {
    setFeedLoadingUI(false, "Failed");
    console.error(err);
    setTimeout(() => { if (divStatus) divStatus.textContent = ""; }, 1500);
    toast(err.message || String(err), false);
  }
}


async function sendInteraction(articleId, interactionType) {
  if (!currentRequestId) {
    toast("Feed is still loading. Try again in a moment.", false);
    return;
  }

  try {
    const payload = {
      user_id: currentUserId,
      request_id: currentRequestId,
      article_id: Number(articleId),
      interaction_type: interactionType,
    };

    // mounted router: POST /feed/interact
    const resp = await fetch("/api/feed/interact", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      credentials: "same-origin",
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


async function logout() {
  try {
    const resp = await fetch("/auth/logout", {
      method: "POST",
      credentials: "same-origin",
    });

    const data = await resp.json().catch(() => ({}));

    if (!resp.ok) {
      throw new Error(data.error || `Logout failed: ${resp.status}`);
    }

    window.location.href = "/login/";
  } catch (err) {
    console.error(err);
    toast(err.message || String(err), false);
  }
}

logoutBtn.addEventListener("click", logout);


document.addEventListener("DOMContentLoaded", async () => {
  try {
      syncDivUiFromState();
      if (categorySelect) categorySelect.value = selectedCategory;

      if (divSlider) {
        divSlider.addEventListener("input", () => {
          const v = sliderToDiv();
          if (divValue) divValue.textContent = v.toFixed(1);
        });
      }

      if (divApplyBtn) {
        divApplyBtn.addEventListener("click", async () => {
          div = sliderToDiv();
          syncDivUiFromState();
          await loadFeed();
        });
      }

      if (categorySelect) {
        categorySelect.addEventListener("change", () => {
          selectedCategory = categorySelect.value || "__all__";

          const visibleItems =
            selectedCategory !== "__all__"
              ? fullFeedItems.filter((x) => x.category === selectedCategory)
              : fullFeedItems;

          renderItems(visibleItems);
        });
      }

      const data = await loadSessionUser();
      if (data) {
        await loadFeed();
      }
  } catch (e) {
    console.error("Startup error:", e);
    toast(e.message || String(e), false);
  }});
