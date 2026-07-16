(function (root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  else root.CryptoScopeLaunchDelivery = api;
})(typeof globalThis === "undefined" ? this : globalThis, function () {
  "use strict";

  const MAX_SNAPSHOT_BYTES = 2_000_000;
  const SAFE_ID = /^[A-Za-z0-9_-]{8,128}$/;
  const SAFE_NONCE = /^[A-Za-z0-9_-]{8,64}$/;
  const LOWER_HEX_64 = /^[0-9a-f]{64}$/;
  const BLOB_SUFFIX = ".public.blob.vercel-storage.com";
  const ENVELOPE_KEYS = [
    "assessment", "assessment_id", "auto_execution_allowed", "kind",
    "launch_generated_at", "ledger_assessment_sha256", "opportunity_id",
    "public_assessment_sha256", "schema_version", "verifier_version",
  ];
  const AWARE_ISO_CLOCK = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$/;
  const object = value => value !== null && typeof value === "object" && !Array.isArray(value);

  function failure(reason, details = {}) {
    return {ok: false, state: "fail", reason, ...details};
  }

  function deepSorted(value) {
    if (Array.isArray(value)) return value.map(deepSorted);
    if (!object(value)) return value;
    return Object.fromEntries(Object.keys(value).sort().map(key => [key, deepSorted(value[key])]));
  }

  function semanticJsonEqual(left, right) {
    try {
      return JSON.stringify(deepSorted(left)) === JSON.stringify(deepSorted(right));
    } catch (_error) {
      return false;
    }
  }

  function cloneJson(value) {
    try { return JSON.parse(JSON.stringify(value)); } catch (_error) { return null; }
  }

  function awareClock(value) {
    if (typeof value !== "string" || !AWARE_ISO_CLOCK.test(value)) return null;
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function deliverySubject(assessment) {
    if (!object(assessment)) return null;
    const subject = cloneJson(assessment);
    if (!object(subject)) return null;
    delete subject.delivery_readback;
    subject.delivery_sla_state = "unverified";
    const codes = Array.isArray(subject.action_reason_codes)
      ? subject.action_reason_codes.filter(code => String(code) !== "delivery_sla_unverified")
      : [];
    codes.push("delivery_sla_unverified");
    subject.action_reason_codes = codes;
    return subject;
  }

  function validatePublicUrl(value, snapshotPath) {
    if (typeof value !== "string" || typeof snapshotPath !== "string") {
      return failure("delivery_url_missing");
    }
    let parsed;
    try {
      parsed = new URL(value);
    } catch (_error) {
      return failure("delivery_url_invalid");
    }
    const hostname = parsed.hostname.toLowerCase();
    const canonical = `https://${hostname}/${snapshotPath}`;
    if (parsed.protocol !== "https:" || !hostname.endsWith(BLOB_SUFFIX)
      || hostname.length <= BLOB_SUFFIX.length || parsed.port
      || parsed.username || parsed.password || parsed.search || parsed.hash
      || parsed.pathname !== `/${snapshotPath}` || parsed.href !== canonical
      || value !== canonical) return failure("delivery_url_invalid");
    return {ok: true, publicUrl: canonical};
  }

  function inspect(row, launch) {
    const assessment = row?.current_assessment;
    const proof = assessment?.delivery_readback;
    if (!object(row) || !object(assessment) || !object(proof)) {
      return failure("delivery_proof_missing");
    }
    const assessmentId = assessment.assessment_id;
    const opportunityId = row.id;
    const assessmentHash = proof.public_assessment_sha256;
    const snapshotHash = proof.public_snapshot_sha256;
    if (row.action_level !== "A3_MANUAL_PROBE"
      || row.auto_execution_allowed !== false
      || assessment.kind !== "read_only_quote"
      || assessment.auto_execution_allowed !== false
      || assessment.is_real_fill !== false
      || assessment.delivery_sla_state !== "pass"
      || proof.version !== 1 || proof.verifier_version !== "public_launch_readback_v1"
      || proof.state !== "pass" || proof.assessment_id !== assessmentId
      || proof.opportunity_id !== opportunityId || assessment.opportunity_id !== opportunityId
      || !SAFE_ID.test(assessmentId || "") || !LOWER_HEX_64.test(assessmentHash || "")
      || !LOWER_HEX_64.test(snapshotHash || "")
      || !LOWER_HEX_64.test(proof.ledger_assessment_sha256 || "")) {
      return failure("delivery_identity_invalid");
    }
    const snapshotPath = proof.snapshot_path;
    const prefix = `launch-snapshots/v1/${assessmentId}-${assessmentHash.slice(0, 16)}-${snapshotHash.slice(0, 16)}-`;
    const tail = typeof snapshotPath === "string" && snapshotPath.startsWith(prefix)
      ? snapshotPath.slice(prefix.length) : "";
    if (!tail.endsWith(".json") || !SAFE_NONCE.test(tail.slice(0, -5))) {
      return failure("delivery_path_binding_invalid");
    }
    const safeUrl = validatePublicUrl(proof.public_url, snapshotPath);
    if (!safeUrl.ok) return safeUrl;
    const launchGenerated = awareClock(proof.launch_generated_at);
    const fetchedAt = awareClock(proof.fetched_at);
    const assessedAt = awareClock(assessment.assessed_at);
    const expiresAt = awareClock(assessment.expires_at);
    const latency = proof.delivery_latency_ms;
    if (![launchGenerated, fetchedAt, assessedAt, expiresAt].every(Number.isFinite)
      || launchGenerated < assessedAt || fetchedAt < launchGenerated || fetchedAt >= expiresAt
      || typeof latency !== "number" || !Number.isFinite(latency) || latency < 0
      || Math.abs(latency - (fetchedAt - launchGenerated)) > 1 || latency > 15_000) {
      return failure("delivery_clock_invalid");
    }
    if (launch && typeof launch.generated_at === "string") {
      const publicClock = awareClock(launch.generated_at);
      if (!Number.isFinite(publicClock) || publicClock < launchGenerated) {
        return failure("delivery_launch_clock_invalid");
      }
    }
    // Cache reuse must bind every proof field that affects delivery authority.
    // Assessment id + snapshot hash alone would let a later payload swap the
    // Blob store, ledger hash or SLA clock while inheriting an earlier pass.
    const cacheKey = JSON.stringify([
      assessmentId, opportunityId, assessmentHash, snapshotHash,
      proof.ledger_assessment_sha256, snapshotPath, safeUrl.publicUrl,
      proof.launch_generated_at, proof.fetched_at, latency,
    ]);
    return {
      ok: true,
      state: "pending",
      key: cacheKey,
      assessmentId,
      opportunityId,
      assessmentHash,
      snapshotHash,
      snapshotPath,
      publicUrl: safeUrl.publicUrl,
      launchGeneratedAt: proof.launch_generated_at,
      ledgerAssessmentHash: proof.ledger_assessment_sha256,
      subject: deliverySubject(assessment),
    };
  }

  async function sha256(bytes, cryptoImpl) {
    const subtle = cryptoImpl?.subtle;
    if (!subtle || typeof subtle.digest !== "function") throw new Error("web_crypto_unavailable");
    const digest = new Uint8Array(await subtle.digest("SHA-256", bytes));
    return Array.from(digest, byte => byte.toString(16).padStart(2, "0")).join("");
  }

  function uniqueAssessmentSlice(text) {
    // JSON.parse deliberately keeps the last duplicate object key. The delivery
    // hash cannot be allowed to bind one `assessment` while semantic checks see
    // another, so scan the complete JSON grammar and reject duplicate decoded
    // keys at every nesting level before using JSON.parse's value.
    let index = 0, assessment = null;
    const fail = () => { throw new Error("invalid_json_structure"); };
    const whitespace = () => {
      while (index < text.length && /[\u0009\u000a\u000d\u0020]/.test(text[index])) index += 1;
    };
    const stringToken = () => {
      if (text[index] !== '"') fail();
      const start = index++;
      let escaped = false;
      while (index < text.length) {
        const char = text[index++];
        if (escaped) escaped = false;
        else if (char === "\\") escaped = true;
        else if (char === '"') {
          try { return JSON.parse(text.slice(start, index)); } catch (_error) { fail(); }
        } else if (char.charCodeAt(0) < 0x20) fail();
      }
      fail();
    };
    const value = depth => {
      whitespace();
      if (text[index] === "{") return objectValue(depth);
      if (text[index] === "[") return arrayValue(depth);
      if (text[index] === '"') { stringToken(); return; }
      for (const literal of ["true", "false", "null"]) {
        if (text.startsWith(literal, index)) { index += literal.length; return; }
      }
      const number = text.slice(index).match(/^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/);
      if (!number) fail();
      index += number[0].length;
    };
    const arrayValue = depth => {
      index += 1; whitespace();
      if (text[index] === "]") { index += 1; return; }
      for (;;) {
        value(depth + 1); whitespace();
        if (text[index] === "]") { index += 1; return; }
        if (text[index] !== ",") fail();
        index += 1;
      }
    };
    const objectValue = depth => {
      index += 1; whitespace();
      const keys = new Set();
      if (text[index] === "}") { index += 1; return; }
      for (;;) {
        const key = stringToken();
        if (keys.has(key)) fail();
        keys.add(key); whitespace();
        if (text[index] !== ":") fail();
        index += 1; whitespace();
        const start = index;
        value(depth + 1);
        if (depth === 0 && key === "assessment") assessment = text.slice(start, index);
        whitespace();
        if (text[index] === "}") { index += 1; return; }
        if (text[index] !== ",") fail();
        index += 1; whitespace();
      }
    };
    try {
      whitespace();
      if (text[index] !== "{") return null;
      objectValue(0); whitespace();
      if (index !== text.length || assessment === null || assessment[0] !== "{") return null;
      return assessment;
    } catch (_error) {
      return null;
    }
  }

  async function verifySnapshotBytes(descriptor, bytes, cryptoImpl) {
    if (!descriptor?.ok || !(bytes instanceof Uint8Array)
      || bytes.byteLength === 0 || bytes.byteLength > MAX_SNAPSHOT_BYTES) {
      return failure("delivery_snapshot_size_invalid", {key: descriptor?.key});
    }
    if (await sha256(bytes, cryptoImpl) !== descriptor.snapshotHash) {
      return failure("delivery_snapshot_hash_mismatch", {key: descriptor.key});
    }
    let text, envelope;
    try {
      text = new TextDecoder("utf-8", {fatal: true}).decode(bytes);
      envelope = JSON.parse(text);
    } catch (_error) {
      return failure("delivery_snapshot_json_invalid", {key: descriptor.key});
    }
    if (!object(envelope) || !semanticJsonEqual(Object.keys(envelope), ENVELOPE_KEYS)
      || envelope.schema_version !== 1
      || envelope.kind !== "cryptoscope_launch_assessment_snapshot"
      || envelope.verifier_version !== "public_launch_readback_v1"
      || envelope.auto_execution_allowed !== false
      || envelope.assessment_id !== descriptor.assessmentId
      || envelope.opportunity_id !== descriptor.opportunityId
      || envelope.launch_generated_at !== descriptor.launchGeneratedAt
      || envelope.public_assessment_sha256 !== descriptor.assessmentHash
      || envelope.ledger_assessment_sha256 !== descriptor.ledgerAssessmentHash
      || !semanticJsonEqual(envelope.assessment, descriptor.subject)) {
      return failure("delivery_snapshot_binding_invalid", {key: descriptor.key});
    }
    const rawAssessment = uniqueAssessmentSlice(text);
    if (rawAssessment === null
      || await sha256(new TextEncoder().encode(rawAssessment), cryptoImpl) !== descriptor.assessmentHash) {
      return failure("delivery_assessment_hash_mismatch", {key: descriptor.key});
    }
    return {
      ok: true, state: "pass", key: descriptor.key,
      publicUrl: descriptor.publicUrl, snapshotHash: descriptor.snapshotHash,
      assessmentHash: descriptor.assessmentHash,
    };
  }

  async function readLimited(response) {
    const lengthHeader = response.headers?.get?.("content-length");
    const suppliedLength = typeof lengthHeader === "string" && /^\d+$/.test(lengthHeader)
      ? Number(lengthHeader) : null;
    if (suppliedLength !== null && suppliedLength > MAX_SNAPSHOT_BYTES) {
      throw new Error("delivery_snapshot_size_invalid");
    }
    if (response.body && typeof response.body.getReader === "function") {
      const reader = response.body.getReader(), chunks = [];
      let total = 0;
      try {
        for (;;) {
          const {done, value} = await reader.read();
          if (done) break;
          const chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
          total += chunk.byteLength;
          if (total > MAX_SNAPSHOT_BYTES) {
            Promise.resolve(reader.cancel("delivery_snapshot_size_invalid")).catch(() => {});
            throw new Error("delivery_snapshot_size_invalid");
          }
          chunks.push(chunk);
        }
      } finally {
        try { reader.releaseLock(); } catch (_error) {}
      }
      if (total === 0 || suppliedLength !== null && total !== suppliedLength) {
        throw new Error("delivery_snapshot_size_invalid");
      }
      const bytes = new Uint8Array(total);
      let offset = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
      }
      return bytes;
    }
    if (suppliedLength === null || suppliedLength === 0) {
      throw new Error("delivery_snapshot_size_invalid");
    }
    const buffer = await response.arrayBuffer();
    const bytes = new Uint8Array(buffer);
    if (bytes.byteLength !== suppliedLength || bytes.byteLength > MAX_SNAPSHOT_BYTES) {
      throw new Error("delivery_snapshot_size_invalid");
    }
    return bytes;
  }

  async function verify(row, launch, options = {}) {
    const descriptor = inspect(row, launch);
    if (!descriptor.ok) return descriptor;
    const fetchImpl = options.fetchImpl || (typeof fetch === "function" ? fetch.bind(globalThis) : null);
    const cryptoImpl = options.cryptoImpl || (typeof crypto === "object" ? crypto : null);
    const timeoutMs = Number.isFinite(options.timeoutMs) ? Math.max(1, options.timeoutMs) : 5_000;
    if (!fetchImpl) return failure("delivery_fetch_unavailable", {key: descriptor.key});
    const controller = typeof AbortController === "function" ? new AbortController() : null;
    let timer;
    try {
      const timeout = new Promise((_, reject) => {
        timer = setTimeout(() => {
          controller?.abort();
          reject(new Error("delivery_fetch_timeout"));
        }, timeoutMs);
      });
      const operation = (async () => {
        const response = await fetchImpl(descriptor.publicUrl, {
          method: "GET", cache: "no-store", credentials: "omit", redirect: "error",
          referrerPolicy: "no-referrer", headers: {Accept: "application/json"},
          ...(controller ? {signal: controller.signal} : {}),
        });
        if (!response || response.status !== 200 || response.redirected !== false
          || response.url !== descriptor.publicUrl) throw new Error("delivery_response_invalid");
        const contentType = String(response.headers?.get?.("content-type") || "").toLowerCase();
        if (!/^application\/json(?:\s*;|$)/.test(contentType)) {
          throw new Error("delivery_content_type_invalid");
        }
        const bytes = await readLimited(response);
        return await verifySnapshotBytes(descriptor, bytes, cryptoImpl);
      })();
      return await Promise.race([operation, timeout]);
    } catch (error) {
      return failure(String(error?.message || "delivery_fetch_failed"), {key: descriptor.key});
    } finally {
      clearTimeout(timer);
    }
  }

  return {
    MAX_SNAPSHOT_BYTES, cloneJson, deliverySubject, inspect, semanticJsonEqual,
    validatePublicUrl, verifySnapshotBytes, verify,
  };
});
