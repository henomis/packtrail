// packtrail-ui: vanilla JS, no dependencies. Every value rendered as text
// (textContent / createElement), never as HTML.
"use strict";

const $ = (s) => document.querySelector(s);
let current = null;   // execution id
let selectedSeq = 0;  // timeline selection (0 = latest)
let watcher = null;
let ns = null;        // selected namespace

// nsPath prefixes an API path with the selected namespace.
const nsPath = (p) => `/api/ns/${encodeURIComponent(ns)}${p}`;

async function api(path, opts = {}) {
  const r = await fetch(path, opts);
  if (!r.ok) throw new Error(`${r.status}: ${(await r.text()).trim()}`);
  return r.headers.get("Content-Type")?.includes("json") ? r.json() : null;
}

function post(path, body) {
  return api(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body || {}) });
}

function el(tag, attrs = {}, text) {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  if (text !== undefined) e.textContent = text;
  return e;
}

// notify shows msg in the banner at the bottom of the page, in place of a
// blocking alert(). Errors stay until dismissed or cleared by their source
// (e.g. the list poll recovering); info notices fade after a few seconds.
let noticeSource = null, noticeTimer = null;
function notify(msg, { kind = "error", source = null } = {}) {
  const b = $("#banner");
  clearTimeout(noticeTimer);
  b.className = kind; b.hidden = false; noticeSource = source;
  $("#banner-msg").textContent = msg;
  if (kind === "info") noticeTimer = setTimeout(() => (b.hidden = true), 5000);
}
function clearNotice(source) { if (noticeSource === source) $("#banner").hidden = true; }
const fail = (e) => notify(e.message);

function badge(status) { return el("span", { class: "badge st-" + String(status) }, String(status)); }

async function loadList() {
  if (!ns) return;
  const q = new URLSearchParams();
  if ($("#f-status").value) q.set("status", $("#f-status").value);
  if ($("#f-flow").value) q.set("flow", $("#f-flow").value);
  if ($("#f-attr").value) q.set("attr", $("#f-attr").value);
  const tbody = $("#list tbody");
  tbody.replaceChildren();
  try {
    for (const s of (await api(nsPath("/executions?" + q))) || []) {
      const tr = el("tr");
      tr.append(el("td", {}, s.exec_id), el("td", {}, s.flow));
      const td = el("td"); td.append(badge(s.status)); tr.append(td);
      tr.append(el("td", {}, new Date(s.updated).toLocaleString()));
      tr.onclick = () => open(s.exec_id);
      if (s.exec_id === current) tr.classList.add("sel");
      tbody.append(tr);
    }
    clearNotice("list");
  } catch (e) { notify(`Cannot load executions: ${e.message}`, { source: "list" }); }
}

async function open(id) {
  current = id; selectedSeq = 0;
  $("#empty").hidden = true; $("#dlq").hidden = true; $("#detail").hidden = false;
  await render().catch(fail);
  if (watcher) watcher.close();
  watcher = new EventSource(nsPath(`/executions/${encodeURIComponent(id)}/watch?from=0`));
  let first = true;
  watcher.onmessage = () => { if (!first) render().catch(fail); first = false; };
  loadList();
}

async function render() {
  const id = current;
  const seq = selectedSeq ? `?seq=${selectedSeq}` : "";
  const [st, hist] = await Promise.all([
    api(nsPath(`/executions/${encodeURIComponent(id)}${seq}`)),
    api(nsPath(`/executions/${encodeURIComponent(id)}/history`)),
  ]);
  $("#d-title").textContent = id;
  const meta = $("#d-meta"); meta.replaceChildren(badge(st.status),
    document.createTextNode(` ${st.flow}@${st.flow_hash} · ${st.events} events` +
      (st.forked_from ? ` · forked from ${st.forked_from}@${st.fork_seq}` : "") +
      (st.archived ? " · archived" : "") + (st.error ? ` · ${st.reason}: ${st.error}` : "")));
  $("#d-at").textContent = selectedSeq ? `(state at seq ${selectedSeq})` : "(latest)";
  $("#state").textContent = JSON.stringify(st, null, 2);
  renderTimeline(hist);
  const def = await api(nsPath(`/flows/${encodeURIComponent(st.flow)}?version=${encodeURIComponent(st.flow_hash)}`));
  renderGraph(def.definition, def.start, st);
}

function summary(ev) {
  const d = ev.data || {};
  return [d.node, d.key && d.key !== d.node ? d.key : "", d.name, d.to ? "→ " + d.to : "",
    d.attempt ? "#" + d.attempt : "", d.error || d.reason || ""].filter(Boolean).join(" ");
}

function renderTimeline(hist) {
  const ol = $("#timeline"); ol.replaceChildren();
  for (const row of hist) {
    const li = el("li", {}, `${row.seq}  ${row.event.type}  ${summary(row.event)}`);
    if (row.decision_end) li.classList.add("decision-end");
    if (row.seq === selectedSeq) li.classList.add("sel");
    li.title = JSON.stringify(row.event.data);
    li.onclick = () => { selectedSeq = selectedSeq === row.seq ? 0 : row.seq; render().catch(fail); };
    ol.append(li);
  }
}

// Graph viewport: the SVG fills #graph-wrap and the viewBox decides what is
// shown. Until the user zooms or pans, the view follows the pane (fit); a
// resize of the pane re-fits it.
const view = { x: 0, y: 0, scale: 1, w: 0, h: 0, key: null, user: false };
const MAX_FIT_SCALE = 1.25;

function applyView() {
  const wrap = $("#graph-wrap"), cw = wrap.clientWidth, ch = wrap.clientHeight;
  if (!cw || !ch || !view.w) return;
  if (!view.user) {
    view.scale = Math.min(cw / view.w, ch / view.h, MAX_FIT_SCALE);
    view.x = (view.w - cw / view.scale) / 2; view.y = (view.h - ch / view.scale) / 2;
  }
  $("#graph").setAttribute("viewBox", `${view.x} ${view.y} ${cw / view.scale} ${ch / view.scale}`);
}

function fitGraph() { view.user = false; applyView(); }

function initGraphView() {
  const wrap = $("#graph-wrap"), svg = $("#graph");
  new ResizeObserver(applyView).observe(wrap);
  svg.addEventListener("wheel", (e) => {
    e.preventDefault();
    const r = svg.getBoundingClientRect();
    const px = view.x + (e.clientX - r.left) / view.scale, py = view.y + (e.clientY - r.top) / view.scale;
    view.scale = Math.min(4, Math.max(0.1, view.scale * (e.deltaY < 0 ? 1.15 : 1 / 1.15)));
    view.x = px - (e.clientX - r.left) / view.scale; view.y = py - (e.clientY - r.top) / view.scale;
    view.user = true; applyView();
  }, { passive: false });
  let drag = null;
  svg.addEventListener("pointerdown", (e) => { drag = { x: e.clientX, y: e.clientY }; svg.setPointerCapture(e.pointerId); });
  svg.addEventListener("pointermove", (e) => {
    if (!drag) return;
    view.x -= (e.clientX - drag.x) / view.scale; view.y -= (e.clientY - drag.y) / view.scale;
    drag = { x: e.clientX, y: e.clientY }; view.user = true; applyView();
  });
  svg.addEventListener("pointerup", () => { drag = null; });
  svg.addEventListener("dblclick", fitGraph);
}

// Layered layout: BFS depth from the start node, each layer centred.
// A fanout's next is its join, which runs after the branches: the layout
// routes fanout → branches → join (dashed), so the join sits below them
// instead of beside them. Edges back to the same or an earlier layer (loops)
// run around the right side of the boxes.
function renderGraph(def, start, st) {
  const svg = $("#graph"); svg.replaceChildren();
  const nodes = new Map(def.nodes.map((n) => [n.id, n]));
  const joinOf = new Map();
  for (const n of def.nodes) if (n.branches?.length && n.next) for (const b of n.branches) joinOf.set(b, n.next);
  const succ = (n) => [n.branches?.length ? null : n.next, ...(n.rules || []).map((r) => r.to), n.on_timeout, n.on_failure,
    ...(n.dynamic || []), ...(n.branches || []), joinOf.get(n.id)].filter(Boolean);
  const depth = new Map([[start, 0]]); const queue = [start];
  while (queue.length) {
    const id = queue.shift();
    for (const s of succ(nodes.get(id))) if (!depth.has(s)) { depth.set(s, depth.get(id) + 1); queue.push(s); }
  }
  const layers = [];
  for (const [id, d] of depth) (layers[d] ||= []).push(id);
  const label = (id) => `${id} · ${nodes.get(id).type}` + (st.visits?.[id] > 1 ? ` ×${st.visits[id]}` : "");
  // Boxes grow with the longest label (~7px per char at 12px) so text never spills.
  const W = Math.max(130, ...[...depth.keys()].map((id) => label(id).length * 7 + 16));
  const H = 34, GX = 40, GY = 40, PAD = 20, LOOP = 50, pos = new Map();
  const widest = Math.max(...layers.map((l) => l.length));
  layers.forEach((ids, d) => ids.forEach((id, i) => pos.set(id, { x: PAD + ((widest - ids.length) / 2 + i) * (W + GX), y: PAD + d * (H + GY) })));
  const ns = "http://www.w3.org/2000/svg";
  const mk = (t, a) => { const e = document.createElementNS(ns, t); for (const [k, v] of Object.entries(a)) e.setAttribute(k, v); return e; };
  const marker = mk("marker", { id: "arrow", viewBox: "0 0 8 8", refX: 8, refY: 4, markerWidth: 7, markerHeight: 7, orient: "auto-start-reverse" });
  marker.append(mk("path", { d: "M0,0 L8,4 L0,8 z", class: "arrow" }));
  const defs = mk("defs", {}); defs.append(marker); svg.append(defs);
  let loops = 0;
  for (const [id, n] of nodes) for (const s of new Set(succ(n))) {
    const a = pos.get(id), b = pos.get(s); if (!a || !b) continue;
    let d;
    if (b.y > a.y) {
      d = `M${a.x + W / 2},${a.y + H} C${a.x + W / 2},${a.y + H + GY / 2} ${b.x + W / 2},${b.y - GY / 2} ${b.x + W / 2},${b.y}`;
    } else if (id === s) {
      const x = a.x + W + LOOP;
      d = `M${a.x + W},${a.y + 8} C${x},${a.y - 10} ${x},${a.y + H + 10} ${a.x + W},${a.y + H - 8}`;
    } else if (b.y === a.y) {
      // Same layer: arc over the top.
      d = `M${a.x + W / 2},${a.y} C${a.x + W / 2},${a.y - GY * 0.7} ${b.x + W / 2},${b.y - GY * 0.7} ${b.x + W / 2},${b.y}`;
    } else {
      // Loop back up: leave a's top, enter b's right side so it never crosses
      // the forward edges into b's top; nested loops spread out.
      const sx = a.x + W * 0.75, x = b.x + W + LOOP / 2 + 12 * loops++;
      d = `M${sx},${a.y} C${sx},${a.y - GY * 0.8} ${x},${b.y + H / 2} ${b.x + W},${b.y + H / 2}`;
    }
    svg.append(mk("path", { class: joinOf.get(id) === s ? "edge join" : "edge", d, "marker-end": "url(#arrow)" }));
  }
  const active = new Set(Object.values(st.tasks || {}).map((t) => t.node).concat(Object.keys(st.awaits || {}), Object.keys(st.children || {}), Object.keys(st.maps || {})));
  for (const [id, p] of pos) {
    const g = mk("g", { transform: `translate(${p.x},${p.y})` });
    if (active.has(id)) g.classList.add("n-active");
    else if (st.failed_node === id) g.classList.add("n-failed");
    else if (st.results && id in st.results) g.classList.add("n-done");
    const r = mk("rect", { width: W, height: H, rx: 5 });
    const t = mk("text", { x: 8, y: 21 }); t.textContent = label(id);
    const tip = mk("title", {}); tip.textContent = label(id);
    g.append(r, t, tip); svg.append(g);
  }
  const key = `${current}|${st.flow_hash}`;
  view.w = widest * (W + GX) - GX + 2 * PAD + LOOP + 12 * loops;
  view.h = layers.length * (H + GY) - GY + 2 * PAD;
  if (view.key !== key) { view.key = key; view.user = false; }
  applyView();
}

async function action(kind) {
  const base = nsPath(`/executions/${encodeURIComponent(current)}`);
  try {
    if (kind === "signal") {
      const name = prompt("Signal name"); if (!name) return;
      const payload = prompt("Payload (JSON, optional)") || "null";
      await post(base + "/signal", { name, payload: JSON.parse(payload) });
    } else if (kind === "resume") {
      const node = prompt("Interrupted node"); if (!node) return;
      await post(base + "/resume", { node, value: JSON.parse(prompt("Value (JSON)") || "null") });
    } else if (kind === "cancel") {
      if (!confirm("Cancel this execution?")) return;
      await post(base + "/cancel", { reason: "cancelled from packtrail-ui" });
    } else if (kind === "fork") {
      if (!selectedSeq) { notify("Select an event in the timeline first.", { kind: "info" }); return; }
      const r = await post(base + "/fork", { seq: selectedSeq }); return open(r.exec_id);
    } else if (kind === "rerun") {
      const node = prompt("Node to rerun"); if (!node) return;
      const r = await post(base + "/rerun", { node }); return open(r.exec_id);
    }
    await render();
  } catch (e) { fail(e); }
}

async function showDLQ() {
  $("#detail").hidden = true; $("#empty").hidden = true; $("#dlq").hidden = false;
  const tbody = $("#dlq tbody"); tbody.replaceChildren();
  for (const d of (await api(nsPath("/deadletters"))) || []) {
    const tr = el("tr");
    tr.append(el("td", {}, d.seq), el("td", {}, d.kind), el("td", {}, d.key), el("td", {}, d.reason));
    const b = el("button", {}, "Redrive");
    b.onclick = async () => { try { await post(nsPath(`/deadletters/${d.seq}/redrive`)); showDLQ(); } catch (e) { fail(e); } };
    const td = el("td"); td.append(b); tr.append(td); tbody.append(tr);
  }
}

// selectNamespace switches every view to ns and records it in the URL hash,
// so a shared link opens the same namespace.
function selectNamespace(name) {
  ns = name; current = null; selectedSeq = 0;
  if (watcher) { watcher.close(); watcher = null; }
  $("#f-ns").value = name;
  history.replaceState(null, "", "#" + encodeURIComponent(name));
  $("#detail").hidden = true; $("#dlq").hidden = true; $("#empty").hidden = false;
  loadList();
}

async function loadNamespaces() {
  const sel = $("#f-ns");
  let res;
  try { res = await api("/api/namespaces"); } catch (e) { notify(`Cannot list namespaces: ${e.message}`); return; }
  const list = res.namespaces || [];
  sel.replaceChildren(...list.map((n) => el("option", { value: n }, n)));
  if (!list.length) { $("#empty").textContent = "No packtrail namespace found on this NATS account."; return; }
  const want = decodeURIComponent(location.hash.slice(1));
  selectNamespace([want, ns, res.default].find((n) => n && list.includes(n)) || list[0]);
}

document.querySelectorAll("[data-act]").forEach((b) => (b.onclick = () => action(b.dataset.act)));
$("#refresh").onclick = loadList;
$("#graph-fit").onclick = fitGraph;
$("#banner-close").onclick = () => ($("#banner").hidden = true);
initGraphView();
$("#show-dlq").onclick = showDLQ;
["#f-status", "#f-flow", "#f-attr"].forEach((s) => $(s).addEventListener("change", loadList));
$("#f-ns").addEventListener("change", () => selectNamespace($("#f-ns").value));
loadNamespaces();
setInterval(() => { if (ns) loadList(); }, 5000);
