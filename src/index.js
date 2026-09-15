// src/index.js
//
// RDPolyLIS sync backend -- Durable Object edition.
//
// WHY THIS VERSION EXISTS
// ------------------------
// The previous version stored each day's state directly in Workers KV and
// did a read-modify-write (GET existing -> merge -> PUT merged) inside a
// single request. That merge fixed the "two people editing at once loses
// everything" problem, but it didn't fully close a narrower race: if two
// saves for the same day arrive close enough together, both can read the
// same "existing" snapshot before either writes back, and whichever write
// lands second silently discards the other's merge. KV is also eventually
// consistent across edge locations, which can widen that window slightly.
//
// This version moves each day's LIVE, actively-edited state into its own
// Durable Object (class DayState below). A Durable Object is a single
// instance that handles all requests for a given key ONE AT A TIME, in
// order -- so the get-merge-put sequence can never race with itself for
// the same day, no matter how many devices hit it at once. The same
// Durable Object also holds short-lived per-record locks, which is what
// powers the "this record is open on another device" check: checking and
// setting a lock happens inside that same serialized instance, so two lock
// requests for the same record can never both "win".
//
// Archives (the weekly Sunday wipe) still write to Workers KV, since a
// write-once weekly snapshot has no concurrent-access problem and doesn't
// need a Durable Object's guarantees.
//
// WHAT CHANGED FROM THE PREVIOUS VERSION
// ----------------------------------------
// 1. Live state moved from KV to a Durable Object per weekday (see
//    wrangler.toml notes at the very bottom of this file -- deploying this
//    requires two additions there beyond just replacing this script).
// 2. Fixed a bug: the previous mergeState() didn't carry the `site` field
//    through at all, so it was silently dropped on every save after the
//    first. It's now merged like any other scalar field.
// 3. Added POST /lock and POST /unlock, backed by short-lived
//    (LOCK_TTL_MS) locks stored in the same Durable Object, so lock
//    checks are exactly as race-free as the state merge.
// 4. Student draft slots are still not accepted (unchanged from the
//    previous version) -- only mon/tue/wed/thu/fri are valid `day` values.
//
// ENDPOINTS (unchanged from a client's point of view, except the two new
// /lock and /unlock routes):
//   GET  /state?day=mon
//   POST /state?day=mon              body: full state object
//   POST /lock?day=mon               body: { ids: [...], owner }
//   POST /unlock?day=mon             body: { ids: [...], owner }
//   POST /clear?day=mon|all

var VALID_DAYS = ["mon", "tue", "wed", "thu", "fri"];

// How long a lock holds before it auto-expires. Long enough to comfortably
// finish accessioning/receiving/entering results/rejecting one record;
// short enough that a crashed tab or a forgotten open modal doesn't block
// that record for the rest of class.
var LOCK_TTL_MS = 2 * 60 * 1000;

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type"
  };
}

function jsonResponse(data, status) {
  return new Response(JSON.stringify(data), {
    status: status || 200,
    headers: {
      "Content-Type": "application/json",
      ...corsHeaders()
    }
  });
}

function isValidDay(day) {
  return VALID_DAYS.includes(day);
}

function todayDateString() {
  const now = new Date();
  const yyyy = now.getUTCFullYear();
  const mm = String(now.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(now.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

// --- Merge helpers ---
// Combines two saves of the same day's state so that concurrent edits from
// different devices are additive rather than last-write-wins. Same
// approach as the previous version, with the `site` fix noted above.

function isPlainObject(v) {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

// Merges two arrays of objects that share an id-like field. Items unique to
// either side are kept; items present in both are merged field-by-field
// (see mergeObjectPreferNonEmpty). This is a union, not a diff -- an item
// deleted on one device can reappear if the other device's save still has
// it. That trade-off is intentional: silently reviving a deleted row is a
// minor, visible annoyance a student can just delete again; silently
// dropping newly-entered specimen or result data is not.
function mergeArraysById(existingArr, incomingArr, idField) {
  const existing = Array.isArray(existingArr) ? existingArr : [];
  const incoming = Array.isArray(incomingArr) ? incomingArr : [];
  const map = new Map();
  const order = [];
  for (const item of existing) {
    if (item && item[idField] != null) {
      map.set(item[idField], item);
      order.push(item[idField]);
    }
  }
  for (const item of incoming) {
    if (!item || item[idField] == null) continue;
    const key = item[idField];
    const prev = map.get(key);
    if (!prev) {
      map.set(key, item);
      order.push(key);
    } else {
      map.set(key, mergeObjectPreferNonEmpty(prev, item));
    }
  }
  return order.map((k) => map.get(k));
}

// Shallow field merge: for each field on the incoming object, keep it unless
// it's empty/undefined/null, in which case fall back to the previous value.
// Nested arrays of objects with an "orderId" or "id" field (e.g. a batch's
// specimens list) are merged the same recursive way instead of replaced.
function mergeObjectPreferNonEmpty(prev, incoming) {
  const merged = { ...prev };
  for (const key of Object.keys(incoming)) {
    const val = incoming[key];
    const prevVal = prev ? prev[key] : undefined;
    if (val === undefined || val === null || val === "") {
      continue;
    }
    if (Array.isArray(val)) {
      const idField = val.length && val[0] && typeof val[0] === "object"
        ? (val[0].orderId !== undefined ? "orderId" : (val[0].id !== undefined ? "id" : null))
        : null;
      if (idField && Array.isArray(prevVal)) {
        merged[key] = mergeArraysById(prevVal, val, idField);
      } else {
        merged[key] = val;
      }
      continue;
    }
    if (isPlainObject(val) && isPlainObject(prevVal)) {
      merged[key] = mergeObjectPreferNonEmpty(prevVal, val);
      continue;
    }
    merged[key] = val;
  }
  return merged;
}

// testCounter is department -> integer, used to generate accession numbers.
// Always take the higher count per department so two devices accessioning
// concurrently never get told to reuse a number the other side already used.
function mergeTestCounter(prev, incoming) {
  const merged = { ...(prev || {}) };
  for (const key of Object.keys(incoming || {})) {
    const a = merged[key] || 0;
    const b = incoming[key] || 0;
    merged[key] = Math.max(a, b);
  }
  return merged;
}

function mergeState(existing, incoming) {
  if (!existing) return incoming;
  if (!incoming) return existing;
  return {
    patients: mergeArraysById(existing.patients, incoming.patients, "id"),
    orders: mergeArraysById(existing.orders, incoming.orders, "id"),
    results: mergeArraysById(existing.results, incoming.results, "id"),
    batches: mergeArraysById(existing.batches, incoming.batches, "id"),
    testCounter: mergeTestCounter(existing.testCounter, incoming.testCounter),
    // FIX: the previous version omitted `site` entirely here, so it was
    // silently discarded on every merge after the first save. It's a plain
    // scalar, so just prefer whichever side actually set a non-empty value.
    site: (incoming.site !== undefined && incoming.site !== null && incoming.site !== "")
      ? incoming.site
      : existing.site,
    checklist: isPlainObject(existing.checklist) && isPlainObject(incoming.checklist)
      ? mergeObjectPreferNonEmpty(existing.checklist, incoming.checklist)
      : (incoming.checklist || existing.checklist)
  };
}

// --- Durable Object: one instance per weekday ---
// Holds that day's live state and its record locks. Every request for a
// given day is routed to the same instance (see getDayStub below) and
// handled strictly one at a time, which is what makes both the state merge
// and the locks race-free -- there is no way for two requests to this same
// instance to interleave mid-operation.

export class DayState {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async getData() {
    const data = await this.state.storage.get("data");
    return data || null;
  }
  async getLocks() {
    const locks = await this.state.storage.get("locks");
    return locks || {};
  }

  async handleGetState() {
    const data = await this.getData();
    return jsonResponse(data || {});
  }

  async handlePostState(request) {
    let incoming;
    try {
      incoming = JSON.parse(await request.text());
    } catch (err) {
      return jsonResponse({ error: "Request body must be valid JSON." }, 400);
    }
    const existing = await this.getData();
    const merged = mergeState(existing, incoming);
    await this.state.storage.put("data", merged);
    // Returning the merged state lets the client immediately reflect
    // everyone's latest data after a save, rather than waiting for its next
    // poll to notice the difference.
    return jsonResponse({ success: true, merged: existing !== null, state: merged });
  }

  async handleLock(request) {
    let body;
    try {
      body = JSON.parse(await request.text());
    } catch (err) {
      return jsonResponse({ error: "Request body must be valid JSON." }, 400);
    }
    const ids = Array.isArray(body.ids) ? body.ids : [];
    const owner = typeof body.owner === "string" ? body.owner : "";
    if (!ids.length || !owner) {
      return jsonResponse({ error: "ids (non-empty array) and owner (string) are required." }, 400);
    }
    const now = Date.now();
    const locks = await this.getLocks();
    // Check every requested id BEFORE acquiring any of them, so a modal
    // covering several tests sharing one tube either locks all of them or
    // none of them -- never a partial lock.
    const conflicts = [];
    for (const id of ids) {
      const existingLock = locks[id];
      if (existingLock && existingLock.expiresAt > now && existingLock.owner !== owner) {
        conflicts.push({ id, owner: existingLock.owner });
      }
    }
    if (conflicts.length) {
      return jsonResponse({ locked: true, conflicts }, 409);
    }
    for (const id of ids) {
      locks[id] = { owner, expiresAt: now + LOCK_TTL_MS };
    }
    await this.state.storage.put("locks", locks);
    return jsonResponse({ locked: false, success: true });
  }

  async handleUnlock(request) {
    let body;
    try {
      body = JSON.parse(await request.text());
    } catch (err) {
      return jsonResponse({ error: "Request body must be valid JSON." }, 400);
    }
    const ids = Array.isArray(body.ids) ? body.ids : [];
    const owner = typeof body.owner === "string" ? body.owner : "";
    const locks = await this.getLocks();
    let changed = false;
    for (const id of ids) {
      // Only release a lock this same owner holds -- never release a lock
      // someone else currently holds, even if asked to.
      if (locks[id] && locks[id].owner === owner) {
        delete locks[id];
        changed = true;
      }
    }
    if (changed) await this.state.storage.put("locks", locks);
    return jsonResponse({ success: true });
  }

  async handleClear(day) {
    const data = await this.getData();
    if (data !== null) {
      const dateString = todayDateString();
      await this.env.RDPOLY_KV.put(`archive:${dateString}:${day}`, JSON.stringify(data));
    }
    await this.state.storage.delete("data");
    await this.state.storage.delete("locks");
    return jsonResponse({ success: true });
  }

  async fetch(request) {
    const url = new URL(request.url);
    const day = url.searchParams.get("day") || "";
    if (request.method === "GET" && url.pathname === "/state") {
      return this.handleGetState();
    }
    if (request.method === "POST" && url.pathname === "/state") {
      return this.handlePostState(request);
    }
    if (request.method === "POST" && url.pathname === "/lock") {
      return this.handleLock(request);
    }
    if (request.method === "POST" && url.pathname === "/unlock") {
      return this.handleUnlock(request);
    }
    if (request.method === "POST" && url.pathname === "/clear") {
      return this.handleClear(day);
    }
    return jsonResponse({ error: "Not found." }, 404);
  }
}

// --- Top-level Worker ---
// Routes each request to the right day's Durable Object instance (or fans
// a "clear all" request out across all five). This part has no state of
// its own and does no merging or locking -- all of that lives in DayState.

function getDayStub(env, day) {
  const id = env.DAY_STATE.idFromName(day);
  return env.DAY_STATE.get(id);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders() });
    }

    if (url.pathname === "/state" || url.pathname === "/lock" || url.pathname === "/unlock") {
      const day = url.searchParams.get("day");
      if (!isValidDay(day)) {
        return jsonResponse(
          { error: `Invalid or missing day. Use one of: ${VALID_DAYS.join(", ")}.` },
          400
        );
      }
      const stub = getDayStub(env, day);
      return stub.fetch(request);
    }

    if (url.pathname === "/clear" && request.method === "POST") {
      const day = url.searchParams.get("day");
      if (day !== "all" && !isValidDay(day)) {
        return jsonResponse(
          { error: `Invalid day. Use one of: ${VALID_DAYS.join(", ")}, or "all".` },
          400
        );
      }
      if (day === "all") {
        for (const d of VALID_DAYS) {
          const stub = getDayStub(env, d);
          await stub.fetch(new Request(`https://internal/clear?day=${d}`, { method: "POST" }));
        }
        return jsonResponse({ success: true, cleared: VALID_DAYS });
      }
      const stub = getDayStub(env, day);
      return stub.fetch(request);
    }

    return jsonResponse(
      {
        error: "Not found.",
        availableEndpoints: [
          "GET /state?day=mon",
          "POST /state?day=mon",
          "POST /lock?day=mon",
          "POST /unlock?day=mon",
          "POST /clear?day=mon",
          "POST /clear?day=all"
        ]
      },
      404
    );
  },

  // Runs whenever a Cron Trigger fires for this Worker. Only actually wipes
  // data if the trigger fired on a Sunday (UTC) -- so even if a Cron
  // Trigger is accidentally left set to run more often, no data gets
  // cleared except once a week.
  async scheduled(event, env, ctx) {
    const scheduledDate = new Date(event.scheduledTime);
    if (scheduledDate.getUTCDay() !== 0) {
      return;
    }
    for (const day of VALID_DAYS) {
      const stub = getDayStub(env, day);
      await stub.fetch(new Request(`https://internal/clear?day=${day}`, { method: "POST" }));
    }
  }
};

// ---------------------------------------------------------------------
// DEPLOYMENT NOTES -- wrangler.toml changes needed beyond this file
// ---------------------------------------------------------------------
// Your existing wrangler.toml should already have something like:
//
//   name = "rdpolylis-sync"
//   main = "src/index.js"
//   compatibility_date = "..."
//
//   [[kv_namespaces]]
//   binding = "RDPOLY_KV"
//   id = "..."
//
// Keep that KV binding exactly as-is (it's now used only for the weekly
// archive). Add these two new blocks:
//
//   [[durable_objects.bindings]]
//   name = "DAY_STATE"
//   class_name = "DayState"
//
//   [[migrations]]
//   tag = "v2-durable-objects"
//   new_classes = ["DayState"]
//
// The [[migrations]] block is required the first time a Durable Object
// class is introduced -- it tells Cloudflare to provision storage for it.
// After adding both blocks, deploy as usual (`wrangler deploy`).
// ---------------------------------------------------------------------
