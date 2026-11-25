(() => {
  const ROLE_KEYS = {
    admin: "dev-finance-admin-key-change-me",
    auditor: "dev-finance-auditor-key-change-me",
    reader: "dev-finance-reader-key-change-me",
  };

  let activePeriod = "";

  function getApiHeaders() {
    const role = document.getElementById("role-select").value;
    return {
      "Content-Type": "application/json",
      "X-API-Key": ROLE_KEYS[role] || ROLE_KEYS.reader,
    };
  }

  async function apiFetch(path, options = {}) {
    const res = await fetch(path, {
      ...options,
      headers: { ...getApiHeaders(), ...(options.headers || {}) },
    });
    if (!res.ok) {
      const errBody = await res.json().catch(() => ({ detail: res.statusText }));
      throw new Error(errBody.detail || `HTTP ${res.status}`);
    }
    return res.json();
  }

  function fmtMinorUsd(minor) {
    const sign = minor < 0 ? "-" : "";
    const abs = Math.abs(minor);
    const dollars = (abs / 100).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    return `${sign}$${dollars}`;
  }

  function clsColor(cls) {
    if (cls === "MATCHED") return "#10b981";
    if (cls === "DUPLICATE_FILE_DEDUPED") return "#14b8a6";
    if (cls === "TIMING_IN_TRANSIT") return "#38bdf8";
    if (cls === "FEE_VARIANCE" || cls === "FX_VARIANCE") return "#f59e0b";
    return "#f43f5e";
  }

  function severityPill(sev) {
    if (sev === "HIGH") return '<span class="pill pill-red">HIGH</span>';
    if (sev === "MEDIUM") return '<span class="pill pill-amber">MEDIUM</span>';
    return '<span class="pill pill-sky">LOW</span>';
  }

  function statusPill(st) {
    if (st === "RESOLVED_ADJUSTED" || st === "HARD_CLOSED") return `<span class="pill pill-green">${st}</span>`;
    if (st === "INVESTIGATING" || st === "SOFT_CLOSE") return `<span class="pill pill-amber">${st}</span>`;
    return `<span class="pill pill-red">${st}</span>`;
  }

  async function loadOverview() {
    const data = await apiFetch("/v1/overview");
    const k = data.kpis;
    const f = data.freshness;

    document.getElementById("freshness-pill").textContent =
      `Run ${String(f.run_id || "").slice(0, 8)} • Seed ${f.seed} • ${f.num_bookings} bookings • ${String(f.completed_at || "").slice(0, 19)}Z`;

    document.getElementById("kpi-imbalance").textContent = fmtMinorUsd(k.trial_balance_imbalance_minor);
    document.getElementById("kpi-debits-credits").textContent =
      `${k.total_reporting_debits_fmt} Dr = Cr`;

    document.getElementById("kpi-match-rate").textContent = `${(k.reconciliation_match_rate * 100).toFixed(2)}%`;
    document.getElementById("kpi-match-counts").textContent =
      `${k.reconciliation_total.toLocaleString()} transactions audited`;

    document.getElementById("kpi-rev-rec").textContent = k.recognized_revenue_usd_fmt;
    document.getElementById("kpi-rev-def").textContent = `Deferred: ${k.deferred_revenue_usd_fmt}`;

    document.getElementById("kpi-exposure").textContent = k.open_exception_exposure_usd_fmt;
    document.getElementById("kpi-exc-count").textContent =
      `${k.open_exception_count} open / ${k.resolved_exception_count} resolved`;

    document.getElementById("kpi-dq-rate").textContent = `${(k.dq_pass_rate * 100).toFixed(1)}%`;
    document.getElementById("kpi-dlq-count").textContent =
      `${k.dq_checks_passed}/${k.dq_checks_total} DQ • ${k.quarantined_dlq_count} DLQ quarantined`;

    const lockedCount = data.close_periods.filter((p) => p.status === "HARD_CLOSED").length;
    const softCount = data.close_periods.filter((p) => p.status === "SOFT_CLOSE").length;
    document.getElementById("kpi-close-status").textContent = `${lockedCount} Hard-Closed`;
    document.getElementById("kpi-restatements").textContent = `${softCount} soft-close • ${k.restatement_lines} restatements`;

    // Render recon breakdown bars
    const total = Math.max(1, k.reconciliation_total);
    const barsEl = document.getElementById("recon-bars");
    barsEl.innerHTML = Object.entries(data.classification_breakdown)
      .sort((a, b) => b[1] - a[1])
      .map(([cls, count]) => {
        const pct = ((count / total) * 100).toFixed(1);
        return `
          <div class="bar-item">
            <div class="bar-item-head">
              <span class="mono">${cls}</span>
              <span class="mono">${count} (${pct}%)</span>
            </div>
            <div class="bar-track">
              <div class="bar-fill" style="width:${Math.max(2, (count / total) * 100)}%; background:${clsColor(cls)};"></div>
            </div>
          </div>`;
      })
      .join("");

    // Render aging buckets
    const agingEl = document.getElementById("aging-grid");
    agingEl.innerHTML = Object.entries(data.aging_buckets)
      .map(([bucket, count]) => `
        <div class="aging-box">
          <div class="aging-label">${bucket}</div>
          <div class="aging-val">${count}</div>
        </div>`)
      .join("");

    // Render period close certificates
    const closeEl = document.getElementById("close-cards");
    closeEl.innerHTML = data.close_periods
      .map((c) => `
        <div class="close-card">
          <div class="close-card-top">
            <strong class="mono">${c.accounting_period}</strong>
            ${statusPill(c.status)}
          </div>
          <div class="close-card-meta mono">
            <span>Lines: ${c.entry_line_count.toLocaleString()} &bull; Restatements: ${c.restatement_line_count} &bull; Imbalance: ${fmtMinorUsd(c.trial_balance_imbalance_minor)}</span>
            <span>Cert SHA-256: ${String(c.certificate_sha256 || "").slice(0, 36)}&hellip;</span>
          </div>
        </div>`)
      .join("");
  }

  async function loadTrialBalance(period = activePeriod) {
    const query = period ? `?period=${encodeURIComponent(period)}` : "";
    const tb = await apiFetch(`/v1/ledger/trial-balance${query}`);
    const body = document.getElementById("trial-balance-body");
    body.innerHTML = tb.accounts
      .map((acc) => `
        <tr>
          <td class="mono">${acc.account_code}</td>
          <td><span class="pill pill-sky">${acc.account_category}</span></td>
          <td class="num">${acc.line_count.toLocaleString()}</td>
          <td class="num">${acc.reporting_debit_fmt}</td>
          <td class="num">${acc.reporting_credit_fmt}</td>
          <td class="num">${acc.net_reporting_fmt}</td>
        </tr>`)
      .join("");

    document.getElementById("trial-balance-foot").innerHTML = `
      <tr>
        <td colspan="2">TOTAL (${tb.period}) &bull; ZERO-SUM ${tb.is_balanced ? "VERIFIED" : "BREACHED"}</td>
        <td class="num">${tb.entry_line_count.toLocaleString()}</td>
        <td class="num">${tb.total_reporting_debits_fmt}</td>
        <td class="num">${tb.total_reporting_credits_fmt}</td>
        <td class="num">${fmtMinorUsd(tb.imbalance_minor)}</td>
      </tr>`;
  }

  async function loadExceptions() {
    const status = document.getElementById("exc-status-filter").value;
    const cls = document.getElementById("exc-class-filter").value;
    const params = new URLSearchParams({ limit: "10" });
    if (status) params.set("status", status);
    if (cls) params.set("classification", cls);

    const data = await apiFetch(`/v1/reconciliation/exceptions?${params.toString()}`);
    const body = document.getElementById("exceptions-body");
    if (!data.items.length) {
      body.innerHTML = '<tr><td colspan="8" class="empty">No matching exceptions for current filters.</td></tr>';
      return;
    }
    body.innerHTML = data.items
      .map((e) => {
        const actionHtml =
          e.status === "RESOLVED_ADJUSTED"
            ? `<span class="mono" style="color:#6ee7b7;">JE ${String(e.adjustment_journal_entry_id || "—").slice(0, 12)}</span>`
            : `<button class="btn btn-secondary btn-xs resolve-btn" data-id="${e.exception_id}" type="button">Resolve &amp; Adjust</button>`;
        return `
          <tr>
            <td class="mono">${e.exception_id}</td>
            <td><button class="link-btn inspect-booking-btn" data-booking="${e.booking_id}" type="button">${e.booking_id}</button></td>
            <td class="mono">${e.classification}</td>
            <td>${severityPill(e.severity)}</td>
            <td class="mono">${e.aging_bucket} (${e.age_days}d)</td>
            <td class="num">${e.variance_usd_fmt}</td>
            <td>${statusPill(e.status)}</td>
            <td>${actionHtml}</td>
          </tr>`;
      })
      .join("");

    document.querySelectorAll(".inspect-booking-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        const bid = btn.dataset.booking;
        document.getElementById("booking-id-input").value = bid;
        loadBookingTrace(bid);
      });
    });

    document.querySelectorAll(".resolve-btn").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const excId = btn.dataset.id;
        btn.disabled = true;
        btn.textContent = "Posting...";
        try {
          await apiFetch(`/v1/reconciliation/exceptions/${encodeURIComponent(excId)}/resolve`, {
            method: "POST",
            body: JSON.stringify({
              target_status: "RESOLVED_ADJUSTED",
              resolution_note: "Verified in Finance Control Tower and posted balanced adjustment entry",
              open_period: "2026-05",
            }),
          });
          await refreshAll();
        } catch (err) {
          alert(`Resolution blocked: ${err.message}`);
          btn.disabled = false;
          btn.textContent = "Resolve & Adjust";
        }
      });
    });
  }

  async function loadBookingTrace(bookingId) {
    const body = document.getElementById("booking-trace-body");
    const meta = document.getElementById("booking-trace-summary");
    try {
      const data = await apiFetch(`/v1/ledger/booking/${encodeURIComponent(bookingId)}`);
      const lastState = data.state_history[data.state_history.length - 1];
      const recon = data.reconciliation;
      meta.innerHTML = `
        <span class="trace-chip mono">Booking: <strong>${data.booking_id}</strong></span>
        <span class="trace-chip mono">Status: <strong>${lastState ? lastState.booking_status : "ORPHAN"}</strong></span>
        <span class="trace-chip mono">Transitions: <strong>${data.state_history.length}</strong></span>
        <span class="trace-chip mono">Recon: <strong>${recon ? recon.classification : "N/A"}</strong></span>
        <span class="trace-chip mono">Entry Balance: <strong>${data.ledger_balance_check.balanced ? "ZERO-SUM OK" : "IMBALANCED"}</strong></span>
      `;
      if (!data.journal_entries.length) {
        body.innerHTML = '<tr><td colspan="7" class="empty">No ledger journal lines (orphan settlement record).</td></tr>';
        return;
      }
      body.innerHTML = data.journal_entries
        .map((j) => {
          const origStr = j.debit_minor > 0
            ? `Dr ${j.debit_minor.toLocaleString()} ${j.original_currency}`
            : `Cr ${j.credit_minor.toLocaleString()} ${j.original_currency}`;
          return `
          <tr>
            <td class="mono" style="white-space:nowrap;">${j.accounting_period}${j.is_restatement ? ' <span class="pill pill-violet">RST</span>' : ""}</td>
            <td class="mono">${j.entry_type}</td>
            <td class="mono">${j.account_code}</td>
            <td class="num">${origStr}</td>
            <td class="num">${fmtMinorUsd(j.reporting_debit_minor)}</td>
            <td class="num">${fmtMinorUsd(j.reporting_credit_minor)}</td>
            <td class="mono">${j.fx_rate_version}</td>
          </tr>`;
        })
        .join("");
    } catch (err) {
      meta.innerHTML = `<span class="trace-chip" style="color:#fda4af;">${err.message}</span>`;
      body.innerHTML = '<tr><td colspan="7" class="empty">Booking trace unavailable.</td></tr>';
    }
  }

  async function loadCatalogAndDq() {
    const [cat, dq] = await Promise.all([apiFetch("/v1/iceberg/tables"), apiFetch("/v1/quality/report")]);
    document.getElementById("iceberg-tables-body").innerHTML = cat.tables
      .map((t) => `
        <tr>
          <td class="mono">${t.table}</td>
          <td class="num">${t.row_count.toLocaleString()}</td>
          <td class="num">${t.data_file_count}</td>
          <td class="num">${t.snapshot_count}</td>
        </tr>`)
      .join("");

    document.getElementById("dq-pills").innerHTML = dq.checks
      .map((c) => `<span class="dq-chip ${c.passed ? "" : "fail"}">${c.passed ? "PASS" : "FAIL"}: ${c.check_name}</span>`)
      .join("");
  }

  async function refreshAll() {
    await Promise.all([
      loadOverview(),
      loadTrialBalance(activePeriod),
      loadExceptions(),
      loadBookingTrace(document.getElementById("booking-id-input").value.trim() || "BKG-2026-00001"),
      loadCatalogAndDq(),
    ]);
  }

  document.querySelectorAll(".seg-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".seg-btn").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      activePeriod = btn.dataset.period || "";
      loadTrialBalance(activePeriod);
    });
  });

  document.getElementById("exc-status-filter").addEventListener("change", loadExceptions);
  document.getElementById("exc-class-filter").addEventListener("change", loadExceptions);
  document.getElementById("role-select").addEventListener("change", refreshAll);
  document.getElementById("refresh-btn").addEventListener("click", refreshAll);

  document.getElementById("booking-lookup-form").addEventListener("submit", (ev) => {
    ev.preventDefault();
    const bid = document.getElementById("booking-id-input").value.trim();
    if (bid) loadBookingTrace(bid);
  });

  document.getElementById("btn-post-restatement").addEventListener("click", async () => {
    const fb = document.getElementById("close-feedback");
    try {
      const res = await apiFetch("/v1/close/period", {
        method: "POST",
        body: JSON.stringify({
          period: "2026-04",
          target_state: "HARD_CLOSED",
          inject_late_restatement_booking_id: "BKG-2026-00002",
          restatement_amount_minor: 7500,
          restatement_reason: "Late post-close issuer fee dispute routed to open period",
        }),
      });
      fb.textContent = `Closed period 2026-04 remained immutable; routed ${res.restatement_entries.length} balanced restatement legs into open period ${res.routed_open_period}.`;
      fb.classList.remove("hidden");
      await refreshAll();
    } catch (err) {
      fb.textContent = `Action blocked: ${err.message}`;
      fb.classList.remove("hidden");
    }
  });

  refreshAll().catch((err) => {
    document.getElementById("freshness-pill").textContent = `API error: ${err.message}`;
  });
})();
