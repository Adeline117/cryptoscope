(function (root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  else root.CryptoScopeProtocolJoin = api;
})(typeof globalThis === "undefined" ? this : globalThis, function () {
  "use strict";

  const IDENTITY_FIELDS = ["protocol_id", "cohort_version", "protocol_start_at"];
  const SAFETY_FIELDS = [
    "state", "enrollment_open", "armed_at", "opened_at", "breached_at",
    "auto_execution_allowed",
  ];
  const STATES = new Set(["scheduled", "armed", "open", "breached"]);
  const RANK = {scheduled: 0, armed: 1, open: 2, breached: 3};
  const object = value => value !== null && typeof value === "object" && !Array.isArray(value);
  const same = (left, right) => JSON.stringify(left) === JSON.stringify(right);
  const clock = value => {
    const parsed = typeof value === "string" ? Date.parse(value) : NaN;
    return Number.isFinite(parsed) ? parsed : null;
  };

  function legalAdmissionTransition(older, newer) {
    if (RANK[newer.admission.state] >= RANK[older.admission.state]) return true;
    if (older.admission.state !== "armed" || newer.admission.state !== "scheduled") return false;
    const transitionAt = clock(newer.admission_updated_at);
    const protocolStartAt = clock(newer.identity.protocol_start_at);
    return transitionAt !== null && protocolStartAt !== null && transitionAt < protocolStartAt;
  }

  function projectMember(view, payload) {
    if (!object(payload)) return null;
    let identitySource, admission;
    if (view === "launch") {
      identitySource = payload.research_protocol;
      admission = payload.primary_sources?.solana?.protocol_admission;
    } else if (view === "stats") {
      const validation = payload.lanes?.launch?.edge_validation;
      identitySource = validation;
      admission = validation?.protocol_admission;
    } else return null;
    if (!object(identitySource) || !object(admission)) return null;
    const identity = Object.fromEntries(IDENTITY_FIELDS.map(field => [field, identitySource[field]]));
    const safety = Object.fromEntries(SAFETY_FIELDS.map(field => [field, admission[field]]));
    const generatedAt = clock(payload.generated_at), admissionUpdatedAt = clock(admission.updated_at);
    if (typeof identity.protocol_id !== "string" || !identity.protocol_id
      || !Number.isInteger(identity.cohort_version)
      || typeof identity.protocol_start_at !== "string" || clock(identity.protocol_start_at) === null
      || generatedAt === null || admissionUpdatedAt === null
      || !STATES.has(safety.state) || typeof safety.enrollment_open !== "boolean"
      || safety.enrollment_open !== (safety.state === "open")
      || safety.auto_execution_allowed !== false
      || IDENTITY_FIELDS.some(field => admission[field] !== identity[field])) return null;
    return {
      view,
      generated_at: payload.generated_at,
      identity,
      admission_updated_at: admission.updated_at,
      admission: safety,
    };
  }

  function compareMembers(launch, stats) {
    const members = {launch: projectMember("launch", launch), stats: projectMember("stats", stats)};
    const missing = Object.entries(members).filter(([, member]) => !member).map(([name]) => name);
    let state, reasonCodes;
    if (missing.length) {
      state = "incomplete";
      reasonCodes = missing.map(name => `${name}_protocol_projection_missing`);
    } else if (!same(members.launch.identity, members.stats.identity)) {
      state = "identity_mismatch";
      reasonCodes = ["launch_stats_protocol_identity_mismatch"];
    } else if (same(members.launch.admission, members.stats.admission)) {
      state = "consistent";
      reasonCodes = [];
    } else {
      const left = members.launch, right = members.stats;
      if (left.admission.state === right.admission.state) {
        state = "contradiction";
        reasonCodes = ["same_state_safety_projection_mismatch"];
      } else {
        const leftClock = clock(left.admission_updated_at), rightClock = clock(right.admission_updated_at);
        if (leftClock === null || rightClock === null || leftClock === rightClock) {
          state = "contradiction";
          reasonCodes = ["admission_state_clock_ambiguous"];
        } else {
          const older = leftClock < rightClock ? left : right;
          const newer = leftClock < rightClock ? right : left;
          if (!legalAdmissionTransition(older, newer)) {
            state = "contradiction";
            reasonCodes = ["admission_state_regressed"];
          } else {
            state = "sync_pending";
            reasonCodes = ["admission_state_not_yet_joined"];
          }
        }
      }
    }
    return {state, reasonCodes, members};
  }

  function actionSafety(members) {
    const launch = members.launch, stats = members.stats;
    if (!launch || !stats || same(launch.admission, stats.admission)) {
      return {block: false, reasonCodes: []};
    }
    const launchClock = clock(launch.admission_updated_at);
    const statsClock = clock(stats.admission_updated_at);
    if (launchClock === null || statsClock === null || launchClock === statsClock) {
      return {block: true, reasonCodes: ["conflicting_admission_clock_ambiguous"]};
    }
    const older = launchClock < statsClock ? launch : stats;
    const newer = launchClock < statsClock ? stats : launch;
    if (newer.admission.state === older.admission.state) {
      return {block: true, reasonCodes: [`newer_${newer.view}_same_state_safety_conflict`]};
    }
    if (newer.admission.state === "breached") {
      return {block: true, reasonCodes: [`newer_${newer.view}_admission_breached`]};
    }
    if (!legalAdmissionTransition(older, newer)) {
      return {block: true, reasonCodes: [`newer_${newer.view}_admission_regressed`]};
    }
    return {block: false, reasonCodes: []};
  }

  function certificateBinds(meta, actual) {
    const certificate = meta?.launch_protocol_join;
    if (!object(certificate) || certificate.version !== 1
      || certificate.state !== "consistent" || certificate.cross_view_edge_usable !== true
      || !object(certificate.members)) return false;
    return ["launch", "stats"].every(name => {
      const expected = actual.members[name], certified = certificate.members[name];
      return expected && object(certified)
        && certified.generated_at === expected.generated_at
        && certified.view === expected.view
        && certified.admission_updated_at === expected.admission_updated_at
        && same(certified.identity, expected.identity)
        && same(certified.admission, expected.admission);
    });
  }

  function evaluate(launch, stats, meta) {
    const actual = compareMembers(launch, stats);
    const bound = certificateBinds(meta, actual);
    const edgeUsable = actual.state === "consistent" && bound;
    const safety = actionSafety(actual.members);
    const reasonCodes = [...actual.reasonCodes];
    if (actual.state === "consistent" && !bound) reasonCodes.push("meta_protocol_join_not_bound");
    return {
      state: edgeUsable ? "consistent" : actual.state === "consistent" ? "meta_unbound" : actual.state,
      edgeUsable,
      reasonCodes,
      members: actual.members,
      actionBlock: safety.block,
      actionReasonCodes: safety.reasonCodes,
    };
  }

  return {projectMember, compareMembers, evaluate};
});
