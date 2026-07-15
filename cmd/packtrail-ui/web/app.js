"use strict";

const $ = (sel) => document.querySelector(sel);
const state = { selected: null, flowCache: {} };

async function getJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${url}: ${r.status}`);
  return r.json();
}

// ---- execution list ---------------------------------------------------------

async function loadFlows() {
  const flows = (await getJSON("/api/flows")) || [];
  const sel = $("#flow-filter");
  for (const f of flows) {
    const opt = document.createElement("option");
    opt.value = opt.textContent = f;
    sel.appendChild(opt);
  }
}

async function refreshList() {
  const flow = $("#flow-filter").value;
  const status = $("#status-filter").value;
  let url = "/api/executions";
  if (status) url += "?status=" + encodeURIComponent(status);
  else if (flow) url += "?flow=" + encodeURIComponent(flow);
  let execs = (await getJSON(url)) || [];
  if (flow && status) execs = execs.filter((e) => e.flow === flow);
  execs.sort((a, b) => new Date(b.updated_at) - new Date(a.updated_at));

  const list = $("#exec-list");
  list.innerHTML = "";
  for (const e of execs) {
    const li = document.createElement("li");
    if (e.id === state.selected) li.classList.add("active");
    li.innerHTML = `<div class="row1"><span class="flow">${esc(e.flow)}</span>
      <span class="badge ${e.status}">${e.status}</span></div>
      <div class="id">${esc(e.id)}</div>`;
    li.onclick = () => selectExec(e.id);
    list.appendChild(li);
  }
}

// ---- execution detail -------------------------------------------------------

async function selectExec(id) {
  state.selected = id;
  refreshList();
  await renderDetail(id);
}

async function renderDetail(id) {
  let ex;
  try {
    ex = await getJSON("/api/executions/" + encodeURIComponent(id));
  } catch {
    $("#detail").innerHTML = `<p class="empty">Execution not found.</p>`;
    return;
  }
  // The snapshot is control state only; payloads live in the data plane
  // (/results) and the transition trace in /history (empty unless the
  // deployment enables WithHistory).
  const eid = encodeURIComponent(id);
  const [results, history] = await Promise.all([
    getJSON(`/api/executions/${eid}/results`).catch(() => null),
    getJSON(`/api/executions/${eid}/history?limit=200`).catch(() => []),
  ]);
  const d = $("#detail");
  d.innerHTML = `
    <h2>${esc(ex.flow)} <span class="badge ${ex.status}">${ex.status}</span></h2>
    <div class="meta">${esc(ex.id)} · node: ${esc(ex.current_node || "—")}${generationSuffix(ex.node_generation)} · attempt ${ex.attempt || 0}
      ${ex.wait_signal ? `· waiting on signal <b>${esc(ex.wait_signal)}</b>` : ""}
      · updated ${new Date(ex.updated_at).toLocaleString()}</div>
    ${ex.error ? `<div class="err">⚠ ${esc(ex.error)}</div>` : ""}
    ${(ex.signals || []).length ? `<div class="chips">received signals: ${ex.signals.map((s) => `<span class="chip">${esc(s)}</span>`).join(" ")}</div>` : ""}
    <section><h3>flow</h3><div id="graph-wrap"></div></section>
    <section><h3>context <span class="hint">input · results · signals · branches · last_node</span></h3>
      ${results ? `<pre>${esc(pretty(results))}</pre>` : `<p class="empty">No context available (payloads archived or expired).</p>`}
    </section>
    ${outputsSection(ex.outputs, ex.output_versions)}
    ${branchSection(ex.branches)}
    ${historySection(history)}
  `;
  const g = await loadFlow(ex.flow);
  if (g) $("#graph-wrap").appendChild(renderGraph(g, ex));
}

function outputsSection(outputs, versions) {
  if (!outputs || !outputs.length) return "";
  const rows = outputs
    .map((node) => `<tr><td class="id">${esc(node)}</td><td class="id">${esc((versions || {})[node] || "legacy")}</td></tr>`)
    .join("");
  return `<section><h3>outputs <span class="hint">committed data-plane versions</span></h3>
    <table class="data-table"><thead><tr><th>node</th><th>version</th></tr></thead>
    <tbody>${rows}</tbody></table></section>`;
}

// branchSection renders the control state of fan-out branches (results live
// under context.results keyed by branch node id).
function branchSection(branches) {
  if (!branches || !Object.keys(branches).length) return "";
  const rows = Object.keys(branches)
    .sort()
    .map((node) => {
      const b = branches[node];
      return `<tr><td class="id">${esc(node)}</td>
        <td><span class="badge ${esc(b.status)}">${esc(b.status)}</span></td>
        <td>${esc(b.generation || "—")}</td>
        <td>${esc(b.attempt ?? "—")}</td>
        <td>${esc(b.error || "")}</td></tr>`;
    })
    .join("");
  return `<section><h3>branches</h3>
    <table class="data-table"><thead><tr><th>branch</th><th>status</th><th>generation</th><th>attempt</th><th>error</th></tr></thead>
    <tbody>${rows}</tbody></table></section>`;
}

// historySection renders the durable per-execution trace; omitted entirely
// when empty (WithHistory disabled, or records past retention).
function historySection(history) {
  if (!history || !history.length) return "";
  const rows = history
    .map(
      (ev) => `<tr><td>${ev.time ? new Date(ev.time).toLocaleString() : "—"}</td>
        <td><span class="badge ${esc(ev.status)}">${esc(ev.status)}</span></td>
        <td class="id">${esc(ev.node || "—")}</td>
        <td>${esc(ev.error || "")}</td></tr>`,
    )
    .join("");
  return `<section><h3>history <span class="hint">${history.length} transitions</span></h3>
    <table class="data-table"><thead><tr><th>time</th><th>status</th><th>node</th><th>error</th></tr></thead>
    <tbody>${rows}</tbody></table></section>`;
}

async function loadFlow(name) {
  if (state.flowCache[name]) return state.flowCache[name];
  try {
    const g = await getJSON("/api/flows/" + encodeURIComponent(name));
    state.flowCache[name] = g;
    return g;
  } catch {
    return null;
  }
}

// ---- flow graph (layered SVG) ----------------------------------------------

// derivedEdges expands routing implied by node type (choice rules, fanout
// branches, signal on_timeout) on top of explicit edges.
function derivedEdges(g) {
  const edges = (g.edges || []).map((e) => [e.from, e.to]);
  for (const n of g.nodes) {
    if (n.type === "choice") for (const r of n.rules || []) edges.push([n.id, r.to]);
    if (n.type === "fanout") for (const b of n.branches || []) edges.push([n.id, b]);
    if (n.type === "signal" && n.on_timeout) edges.push([n.id, n.on_timeout]);
  }
  return edges;
}

function layout(g) {
  const edges = derivedEdges(g);
  const indeg = {}, children = {};
  for (const n of g.nodes) { indeg[n.id] = 0; children[n.id] = []; }
  for (const [from, to] of edges) {
    if (!(to in indeg)) continue;
    indeg[to]++; if (children[from]) children[from].push(to);
  }
  // BFS depth from roots; cap iterations to tolerate cycles.
  const depth = {};
  let frontier = g.nodes.filter((n) => indeg[n.id] === 0).map((n) => n.id);
  if (frontier.length === 0 && g.nodes.length) frontier = [g.nodes[0].id];
  frontier.forEach((id) => (depth[id] = 0));
  let guard = 0;
  while (frontier.length && guard++ < 1000) {
    const next = [];
    for (const id of frontier)
      for (const c of children[id] || [])
        if (depth[c] === undefined || depth[c] < depth[id] + 1) { depth[c] = depth[id] + 1; next.push(c); }
    frontier = next;
  }
  g.nodes.forEach((n) => { if (depth[n.id] === undefined) depth[n.id] = 0; });

  const byDepth = {};
  for (const n of g.nodes) (byDepth[depth[n.id]] ||= []).push(n.id);
  const pos = {};
  const COLW = 200, ROWH = 80, NW = 150, NH = 46;
  for (const d of Object.keys(byDepth)) {
    byDepth[d].forEach((id, i) => { pos[id] = { x: 30 + d * COLW, y: 24 + i * ROWH }; });
  }
  const maxRows = Math.max(...Object.values(byDepth).map((a) => a.length), 1);
  const maxDepth = Math.max(...Object.keys(byDepth).map(Number), 0);
  return { edges, pos, NW, NH, COLW, width: 60 + (maxDepth + 1) * COLW, height: 48 + maxRows * ROWH };
}

function renderGraph(g, ex) {
  const L = layout(g);
  const svg = svgEl("svg", { class: "graph", viewBox: `0 0 ${L.width} ${L.height}`, height: Math.min(L.height, 520) });
  svg.appendChild(arrowDefs());

  for (const [from, to] of L.edges) {
    const a = L.pos[from], b = L.pos[to];
    if (!a || !b) continue;
    const x1 = a.x + L.NW, y1 = a.y + L.NH / 2, x2 = b.x, y2 = b.y + L.NH / 2;
    const mx = (x1 + x2) / 2;
    svg.appendChild(svgEl("path", {
      class: "gedge", "marker-end": "url(#arrow)",
      d: `M ${x1} ${y1} C ${mx} ${y1}, ${mx} ${y2}, ${x2} ${y2}`,
    }));
  }

  const settled = new Set(ex.outputs || []); // nodes with a stored output
  for (const n of g.nodes) {
    const p = L.pos[n.id];
    const current = n.id === ex.current_node;
    const cls = "gnode" + (settled.has(n.id) ? " done" : "") + (current ? " current " + ex.status : "");
    const grp = svgEl("g", { class: cls, transform: `translate(${p.x},${p.y})` });
    grp.appendChild(svgEl("rect", { width: L.NW, height: L.NH, rx: 8 }));
    grp.appendChild(text(10, 19, n.id, "nid"));
    grp.appendChild(text(10, 35, nodeLabel(n), "ntype"));
    svg.appendChild(grp);
  }
  return svg;
}

function nodeLabel(n) {
  if (n.type === "task") return "task" + (n.target ? " → " + n.target : "");
  if (n.type === "fanin") return "fanin (" + (n.join_policy || "all") + ")";
  if (n.type === "signal") return "signal: " + (n.signal_name || "");
  return n.type;
}

// ---- dead-letter tile -------------------------------------------------------

async function refreshDLQ() {
  let dlq;
  try {
    dlq = await getJSON("/api/deadletters");
  } catch {
    return;
  }
  const btn = $("#dlq");
  const count = dlq.count || 0;
  btn.hidden = count === 0;
  btn.textContent = `⚠ ${count} dead-lettered`;
  state.dlq = dlq.recent || [];
}

function showDLQ() {
  const rows = (state.dlq || [])
    .slice()
    .reverse()
    .map(
      (d) => `<tr><td><span class="badge failed">${esc(d.kind)}</span></td>
        <td class="id">${esc(d.key)}</td><td>${esc(d.reason)}</td>
        <td>${d.deliveries || 0}</td>
        <td>${d.time ? new Date(d.time).toLocaleString() : "—"}</td></tr>`,
    )
    .join("");
  $("#detail").innerHTML = `
    <h2>dead-lettered work</h2>
    <div class="meta">poisoned items dropped by the durable consumers (terminal error or exhausted retries)</div>
    ${rows
      ? `<table class="data-table"><thead><tr><th>kind</th><th>key</th><th>reason</th><th>deliveries</th><th>time</th></tr></thead><tbody>${rows}</tbody></table>`
      : `<p class="empty">No dead-letters retained.</p>`}`;
}

// ---- live updates -----------------------------------------------------------

function connectEvents() {
  const es = new EventSource("/api/events");
  es.onopen = () => $("#conn").classList.add("live");
  es.onerror = () => $("#conn").classList.remove("live");
  let pending = false;
  es.onmessage = (m) => {
    let ev; try { ev = JSON.parse(m.data); } catch { return; }
    if (!pending) { pending = true; setTimeout(() => { pending = false; refreshList(); refreshDLQ(); }, 300); }
    if (ev.exec_id === state.selected) renderDetail(state.selected);
  };
}

// ---- helpers ----------------------------------------------------------------

function svgEl(tag, attrs) {
  const el = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const k in attrs) el.setAttribute(k, attrs[k]);
  return el;
}
function text(x, y, s, cls) {
  const t = svgEl("text", { x, y, class: cls });
  t.textContent = s.length > 20 ? s.slice(0, 19) + "…" : s;
  return t;
}
function arrowDefs() {
  const defs = svgEl("defs", {});
  const m = svgEl("marker", { id: "arrow", viewBox: "0 0 10 10", refX: 9, refY: 5, markerWidth: 6, markerHeight: 6, orient: "auto-start-reverse" });
  const p = svgEl("path", { d: "M 0 0 L 10 5 L 0 10 z", fill: "#2a2f3a" });
  m.appendChild(p); defs.appendChild(m); return defs;
}
function pretty(v) { try { return JSON.stringify(v, null, 2); } catch { return String(v); } }
function esc(s) { return String(s ?? "").replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c])); }
function generationSuffix(g) { return g ? ` · gen ${esc(g)}` : ""; }

// ---- boot -------------------------------------------------------------------

$("#flow-filter").onchange = refreshList;
$("#status-filter").onchange = refreshList;
$("#dlq").onclick = showDLQ;
loadFlows().then(refreshList).then(refreshDLQ).then(connectEvents);
