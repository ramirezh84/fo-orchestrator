/* SentinelFO Portal — Shared JavaScript */

// ── API Layer ───────────────────────────────────────────────────────────────

async function apiCall(url, method, body) {
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const resp = await fetch(url, opts);
  const data = await resp.json();
  if (!resp.ok) throw new Error(data.error || 'Request failed');
  return data;
}

// ── Button Loading States ───────────────────────────────────────────────────

function setButtonLoading(btn, loading) {
  if (loading) {
    btn.classList.add('loading');
    btn.disabled = true;
  } else {
    btn.classList.remove('loading');
    btn.disabled = false;
  }
}

async function btnAction(btn, fn) {
  setButtonLoading(btn, true);
  try {
    await fn();
  } catch (e) {
    // error already shown by showMessage
  } finally {
    setButtonLoading(btn, false);
  }
}

// ── Messages ────────────────────────────────────────────────────────────────

function showMessage(container, text, isError) {
  const el = typeof container === 'string' ? document.getElementById(container) : container;
  if (!el) return;
  el.textContent = text;
  el.className = 'message ' + (isError ? 'message-error' : 'message-ok');
  if (!isError) {
    setTimeout(() => { el.className = 'message hidden'; }, 3000);
  }
}

function clearMessage(container) {
  const el = typeof container === 'string' ? document.getElementById(container) : container;
  if (el) el.className = 'message hidden';
}

// ── Admin Page ──────────────────────────────────────────────────────────────

function initAdminPage(VERSIONS) {
  const selVersion = document.getElementById('sel-version');
  const selArch = document.getElementById('sel-architecture');
  const selBackend = document.getElementById('sel-backend');
  const selProvider = document.getElementById('provider-row-select');
  const msgEl = document.getElementById('admin-message');

  function onVersionChange() {
    const ver = selVersion.value;
    const config = VERSIONS[ver];
    const grid = document.querySelector('.config-grid');
    if (config.supports_provider) {
      grid.classList.add('provider-visible');
    } else {
      grid.classList.remove('provider-visible');
    }
    const featureList = document.getElementById('version-features');
    featureList.innerHTML = config.features
      .map(f => '<div class="feature-item">' + f + '</div>')
      .join('');
  }

  selVersion.addEventListener('change', onVersionChange);
  onVersionChange();

  // Start test
  document.getElementById('btn-start').addEventListener('click', function() {
    const btn = this;
    btnAction(btn, async () => {
      clearMessage(msgEl);
      const data = await apiCall('/api/start', 'POST', {
        version: selVersion.value,
        architecture: selArch.value,
        backend: selBackend.value,
        provider: selProvider.value,
      });
      showMessage(msgEl, data.message, false);
      refreshAdminStatus();
    });
  });

  // Stop test
  document.getElementById('btn-stop').addEventListener('click', function() {
    const btn = this;
    btnAction(btn, async () => {
      clearMessage(msgEl);
      const data = await apiCall('/api/stop', 'POST');
      showMessage(msgEl, data.message, false);
      refreshAdminStatus();
    });
  });

  // Aurora toggle
  document.getElementById('aurora-toggle').addEventListener('click', function() {
    const track = this;
    if (track.classList.contains('starting')) {
      showMessage(msgEl, 'Aurora is starting, please wait...', true);
      return;
    }
    const isOn = track.classList.contains('on');
    const url = isOn ? '/api/aurora/off' : '/api/aurora/on';

    // Create a proxy button for loading state
    track.style.opacity = '0.5';
    track.style.pointerEvents = 'none';

    apiCall(url, 'POST')
      .then(data => {
        showMessage(msgEl, data.message, false);
        refreshAdminStatus();
      })
      .catch(e => {
        showMessage(msgEl, e.message, true);
      })
      .finally(() => {
        track.style.opacity = '';
        track.style.pointerEvents = '';
      });
  });

  // Reset everything
  document.getElementById('btn-reset').addEventListener('click', function() {
    const btn = this;
    if (!confirm('Reset everything? This will stop the test, switchover Aurora to primary if needed, and reset all state.')) return;
    btnAction(btn, async () => {
      clearMessage(msgEl);
      const data = await apiCall('/api/reset', 'POST');
      showMessage(msgEl, data.message, false);
      refreshAdminStatus();
    });
  });

  // Auto-promote toggle
  document.getElementById('auto-promote-toggle').addEventListener('click', function() {
    const track = this;
    const isOn = track.classList.contains('on');
    const newValue = !isOn;

    track.style.opacity = '0.5';
    track.style.pointerEvents = 'none';

    apiCall('/api/auto-promote', 'POST', { enabled: newValue })
      .then(data => {
        track.classList.toggle('on', newValue);
        document.getElementById('auto-promote-text').textContent = newValue
          ? 'On — Aurora will auto-promote after failover'
          : 'Off — operator must promote Aurora manually';
        showMessage(msgEl, data.message, false);
      })
      .catch(e => showMessage(msgEl, e.message, true))
      .finally(() => {
        track.style.opacity = '';
        track.style.pointerEvents = '';
      });
  });

  // Status refresh
  async function refreshAdminStatus() {
    try {
      const data = await apiCall('/api/status', 'GET');
      const s = data.status;
      const l = data.lock;

      // ECS
      const ecsW1 = s.ecs['us-west-1'];
      const ecsW2 = s.ecs['us-west-2'];
      setText('ecs-w1', ecsW1.running !== null ? ecsW1.running + '/' + ecsW1.desired : 'N/A');
      setText('ecs-w2', ecsW2.running !== null ? ecsW2.running + '/' + ecsW2.desired : 'N/A');

      // Aurora
      const aw1 = s.aurora['us-west-1'];
      const aw2 = s.aurora['us-west-2'];
      const aw1Status = typeof aw1 === 'object' ? aw1.status : aw1;
      const aw2Status = typeof aw2 === 'object' ? aw2.status : aw2;
      const aw1Role = typeof aw1 === 'object' ? aw1.role : 'unknown';
      const aw2Role = typeof aw2 === 'object' ? aw2.role : 'unknown';
      setText('aurora-w1', aw1Status + (aw1Role !== 'unknown' ? ' (' + aw1Role + ')' : ''));
      setText('aurora-w2', aw2Status + (aw2Role !== 'unknown' ? ' (' + aw2Role + ')' : ''));

      const bothAvailable = aw1Status === 'available' && aw2Status === 'available';
      const anyRunning = aw1Status !== 'not-found' || aw2Status !== 'not-found';
      const creating = aw1Status === 'creating' || aw2Status === 'creating';

      const toggle = document.getElementById('aurora-toggle');
      const auroraText = document.getElementById('aurora-status-text');

      toggle.classList.remove('on', 'starting');
      if (bothAvailable) {
        toggle.classList.add('on');
        auroraText.textContent = 'Available in both regions';
      } else if (creating) {
        toggle.classList.add('starting');
        auroraText.textContent = 'Creating instances...';
      } else if (anyRunning) {
        auroraText.textContent = 'Partial (' + aw1 + ' / ' + aw2 + ')';
      } else {
        auroraText.textContent = 'Off';
      }

      // EventBridge
      setText('eb-w1', s.eventbridge['us-west-1']);
      setText('eb-w2', s.eventbridge['us-west-2']);

      // State
      const st = s.state;
      setText('fo-state', st.state || '--');
      setText('fo-active', st.active_region || '--');
      setText('fo-latch', st.latch_engaged !== undefined ? (st.latch_engaged ? 'YES' : 'no') : '--');
      setText('fo-failures', st.consecutive_failures !== undefined ? st.consecutive_failures : '--');

      // Lambda + Backend
      const verName = s.active_alias_name || ('v' + s.active_version);
      setText('lambda-version', verName + ' (v' + (s.active_version || '?') + ')');
      setText('active-backend', s.active_backend || 'dynamodb');

      // Lock
      const lockEl = document.getElementById('lock-status');
      if (l.locked) {
        lockEl.textContent = 'Held by ' + l.locked_by;
        lockEl.className = 'stat-value';
        lockEl.style.color = 'var(--warning)';
      } else {
        lockEl.textContent = 'Free';
        lockEl.className = 'stat-value';
        lockEl.style.color = 'var(--success)';
      }

      // Test status badge
      const isActive = ecsW1.desired > 0 || ecsW2.desired > 0;
      const badgeEl = document.getElementById('test-badge');
      const configEl = document.getElementById('test-config-display');
      if (isActive && l.locked) {
        badgeEl.textContent = 'ACTIVE';
        badgeEl.className = 'badge badge-active';
        try {
          const cfg = JSON.parse(l.test_config);
          configEl.textContent = cfg.version + ' / ' + cfg.architecture + ' / ' + cfg.backend;
        } catch(e) { configEl.textContent = ''; }
      } else {
        badgeEl.textContent = 'IDLE';
        badgeEl.className = 'badge badge-idle';
        configEl.textContent = '';
      }

      // Refresh dot
      const dot = document.getElementById('refresh-indicator');
      if (dot) {
        dot.classList.add('pulse');
        setTimeout(() => dot.classList.remove('pulse'), 500);
      }
    } catch (e) {
      // Silent refresh failure
    }
  }

  function setText(id, value) {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
  }

  refreshAdminStatus();
  setInterval(refreshAdminStatus, 15000);
}


// ── Demo Page ───────────────────────────────────────────────────────────────

function initDemoPage() {
  // Persistent state (survives page navigation)
  let events = JSON.parse(sessionStorage.getItem('sfo_events') || '[]');
  let lastState = sessionStorage.getItem('sfo_lastState') || null;
  let lastFailures = Number(sessionStorage.getItem('sfo_lastFailures') || '0');
  let failureAt = Number(sessionStorage.getItem('sfo_failureAt')) || 0;
  let failoverAt = Number(sessionStorage.getItem('sfo_failoverAt')) || 0;
  let auroraAt = Number(sessionStorage.getItem('sfo_auroraAt')) || 0;
  let resolvedAt = Number(sessionStorage.getItem('sfo_resolvedAt')) || 0;
  let timerInterval = null;

  const eventsEl = document.getElementById('timeline-events');
  const stateBadge = document.getElementById('state-badge');

  function save() {
    sessionStorage.setItem('sfo_events', JSON.stringify(events));
    sessionStorage.setItem('sfo_lastState', lastState || '');
    sessionStorage.setItem('sfo_lastFailures', lastFailures);
    sessionStorage.setItem('sfo_failureAt', failureAt);
    sessionStorage.setItem('sfo_failoverAt', failoverAt);
    sessionStorage.setItem('sfo_auroraAt', auroraAt);
    sessionStorage.setItem('sfo_resolvedAt', resolvedAt);
  }

  function fmt(s) {
    if (!s || s < 0) return '--';
    const m = Math.floor(s / 60), sec = s % 60;
    return String(m).padStart(2, '0') + ':' + String(sec).padStart(2, '0');
  }

  function addEvent(text, type) {
    const now = new Date();
    const time = now.toLocaleTimeString('en-US', { hour12: false });
    let elapsed = '';
    if (failureAt) elapsed = '+' + fmt(Math.floor((Date.now() - failureAt) / 1000));
    events.push({ time, text, type: type || 'info', elapsed });
    save();
    renderEvents();
  }

  function renderEvents() {
    if (!events.length) {
      eventsEl.innerHTML = '<div class="timeline-empty">Start a test from Admin, then click Inject Failure.</div>';
      return;
    }
    eventsEl.innerHTML = events.map(e =>
      '<div class="timeline-event">' +
      '<span class="event-time">' + e.time + '</span>' +
      '<span class="event-dot dot-' + e.type + '"></span>' +
      '<span class="event-text">' + e.text + '</span>' +
      (e.elapsed ? '<span class="event-elapsed">' + e.elapsed + '</span>' : '') +
      '</div>'
    ).join('');
    eventsEl.scrollTop = eventsEl.scrollHeight;
  }

  // Timers
  function updateTimers() {
    const now = Date.now();
    if (failureAt && failoverAt) {
      document.getElementById('timer-detection').textContent = fmt(Math.floor((failoverAt - failureAt) / 1000));
    }
    if (failoverAt && auroraAt) {
      document.getElementById('timer-db').textContent = fmt(Math.floor((auroraAt - failoverAt) / 1000));
    }
    if (failureAt) {
      const end = resolvedAt || now;
      document.getElementById('timer-impact').textContent = fmt(Math.floor((end - failureAt) / 1000));
    }
  }

  function startTimer() {
    stopTimer();
    timerInterval = setInterval(updateTimers, 1000);
  }

  function stopTimer() {
    if (timerInterval) { clearInterval(timerInterval); timerInterval = null; }
  }

  // Buttons
  document.getElementById('btn-inject').addEventListener('click', function() {
    btnAction(this, async () => {
      await apiCall('/api/trigger', 'POST');
      failureAt = Date.now(); failoverAt = 0; auroraAt = 0; resolvedAt = 0;
      addEvent('Failure injected — ECS scaling to 0 in primary', 'error');
      addEvent('Health checks will detect failure in ~60-120s (3 consecutive)', 'warn');
      startTimer();
    });
  });

  document.getElementById('btn-promote').addEventListener('click', function() {
    btnAction(this, async () => {
      const data = await apiCall('/api/aurora/promote', 'POST');
      addEvent('Aurora promotion initiated: ' + (data.message || ''), 'info');
    });
  });

  document.getElementById('btn-failback').addEventListener('click', function() {
    btnAction(this, async () => {
      const data = await apiCall('/api/failback', 'POST');
      addEvent('Failback initiated', 'info');
    });
  });

  document.getElementById('btn-clear').addEventListener('click', function() {
    events = []; lastState = null; lastFailures = 0;
    failureAt = 0; failoverAt = 0; auroraAt = 0; resolvedAt = 0;
    save(); stopTimer();
    document.getElementById('timer-detection').textContent = '--';
    document.getElementById('timer-db').textContent = '--';
    document.getElementById('timer-impact').textContent = '--';
    renderEvents();
  });

  // Update diagram + detect transitions
  function update(s) {
    const st = s.state;
    const state = st.state || 'UNKNOWN';
    const active = st.active_region || 'us-west-1';
    const failures = st.consecutive_failures || 0;
    const ecsW1 = s.ecs['us-west-1'];
    const ecsW2 = s.ecs['us-west-2'];
    const aw1 = typeof s.aurora['us-west-1'] === 'object' ? s.aurora['us-west-1'] : {status: s.aurora['us-west-1'], role: 'unknown'};
    const aw2 = typeof s.aurora['us-west-2'] === 'object' ? s.aurora['us-west-2'] : {status: s.aurora['us-west-2'], role: 'unknown'};

    // State badge
    const stateClasses = { PRIMARY_ACTIVE: 'state-primary-active', WAITING_AURORA_PROMOTION: 'state-waiting', SECONDARY_ACTIVE: 'state-secondary-active', FAILBACK_IN_PROGRESS: 'state-failback' };
    stateBadge.textContent = state.replace(/_/g, ' ');
    stateBadge.className = 'state-badge ' + (stateClasses[state] || 'state-unknown');

    // Service nodes
    updateNode('svc-ecs-w1', ecsW1.running > 0, ecsW1.running + '/' + ecsW1.desired);
    updateNode('svc-ecs-w2', ecsW2.running > 0, ecsW2.running + '/' + ecsW2.desired);
    updateNode('svc-alb-w1', ecsW1.running > 0, '');
    updateNode('svc-alb-w2', ecsW2.running > 0, '');
    updateNode('svc-aurora-w1', aw1.status === 'available', aw1.status);
    updateNode('svc-aurora-w2', aw2.status === 'available', aw2.status);

    // Aurora labels
    const lw1 = document.getElementById('aurora-label-w1');
    const lw2 = document.getElementById('aurora-label-w2');
    if (lw1) lw1.textContent = 'Aurora' + (aw1.role === 'writer' ? ' (Writer)' : aw1.role === 'reader' ? ' (Reader)' : '');
    if (lw2) lw2.textContent = 'Aurora' + (aw2.role === 'writer' ? ' (Writer)' : aw2.role === 'reader' ? ' (Reader)' : '');

    // Region boxes
    const w1 = document.getElementById('region-w1');
    const w2 = document.getElementById('region-w2');
    const r1 = document.getElementById('role-w1');
    const r2 = document.getElementById('role-w2');
    const fa = document.getElementById('flow-arrow');
    const fl = document.getElementById('flow-label');
    const tw1 = document.getElementById('traffic-w1');
    const tw2 = document.getElementById('traffic-w2');

    w1.className = 'region-box'; w2.className = 'region-box';

    if (active === 'us-west-1') {
      w1.classList.add('active'); w2.classList.add('standby');
      r1.textContent = 'Active'; r1.className = 'region-role role-active';
      r2.textContent = 'Standby'; r2.className = 'region-role role-secondary';
      fa.innerHTML = '&rarr;'; fl.textContent = 'standby'; fa.className = 'flow-arrow';
      // Traffic
      if (ecsW1.running > 0) {
        tw1.className = 'traffic-indicator traffic-active';
        tw1.querySelector('.traffic-label').textContent = 'Traffic flowing';
      } else {
        tw1.className = 'traffic-indicator traffic-errors';
        tw1.querySelector('.traffic-label').textContent = 'Requests failing (503)';
      }
      tw2.className = 'traffic-indicator traffic-none';
      tw2.querySelector('.traffic-label').textContent = 'No traffic';
    } else {
      w2.classList.add('active'); r2.textContent = 'Active'; r2.className = 'region-role role-active';
      fa.innerHTML = '&larr;'; fl.textContent = 'failover'; fa.className = 'flow-arrow active-flow';
      if (ecsW1.running === 0) {
        w1.classList.add('failed'); r1.textContent = 'Failed'; r1.className = 'region-role role-failed';
      } else {
        w1.classList.add('standby'); r1.textContent = 'Standby'; r1.className = 'region-role role-secondary';
      }
      tw1.className = 'traffic-indicator traffic-none';
      tw1.querySelector('.traffic-label').textContent = ecsW1.running === 0 ? 'Region down' : 'No traffic';
      if (state === 'WAITING_AURORA_PROMOTION' && aw2.role !== 'writer') {
        tw2.className = 'traffic-indicator traffic-errors';
        tw2.querySelector('.traffic-label').textContent = 'Writes failing (no Aurora writer)';
      } else {
        tw2.className = 'traffic-indicator traffic-active';
        tw2.querySelector('.traffic-label').textContent = 'Traffic flowing';
      }
    }

    // Detect consecutive failure changes
    if (state === 'PRIMARY_ACTIVE' && failureAt && failures > lastFailures && failures > 0) {
      addEvent('Orchestrator detected failure (' + failures + '/3)', 'warn');
    }
    lastFailures = failures;

    // State transitions
    if (lastState && lastState !== state) {
      if (lastState === 'PRIMARY_ACTIVE' && state === 'WAITING_AURORA_PROMOTION') {
        failoverAt = Date.now();
        const det = failureAt ? fmt(Math.floor((failoverAt - failureAt) / 1000)) : '?';
        addEvent('FAILOVER — DNS moved to us-west-2 (detection: ' + det + ')', 'error');
        addEvent('Waiting for Aurora promotion...', 'warn');
      } else if (state === 'SECONDARY_ACTIVE' && lastState === 'WAITING_AURORA_PROMOTION') {
        resolvedAt = Date.now();
        auroraAt = auroraAt || resolvedAt;
        const total = failureAt ? fmt(Math.floor((resolvedAt - failureAt) / 1000)) : '?';
        addEvent('App fully operational in us-west-2 (total impact: ' + total + ')', 'success');
        stopTimer();
      } else if (state === 'PRIMARY_ACTIVE' && lastState !== 'PRIMARY_ACTIVE') {
        addEvent('Failback complete — primary active', 'success');
        stopTimer();
      } else {
        addEvent('State: ' + lastState + ' → ' + state, 'info');
      }
    }

    // Detect Aurora promotion (w2 becomes writer while waiting)
    if (state === 'WAITING_AURORA_PROMOTION' && aw2.role === 'writer' && !auroraAt) {
      auroraAt = Date.now();
      const dbTime = failoverAt ? fmt(Math.floor((auroraAt - failoverAt) / 1000)) : '?';
      addEvent('Aurora promoted — us-west-2 is now writer (DB time: ' + dbTime + ')', 'success');
    }

    lastState = state;
    save();
    updateTimers();
  }

  function updateNode(id, healthy, detail) {
    const el = document.getElementById(id);
    if (!el) return;
    el.className = 'service-node ' + (healthy ? 'healthy' : 'unhealthy');
    const d = el.querySelector('.service-detail');
    if (d && detail !== undefined) d.textContent = detail;
  }

  async function poll() {
    try {
      const data = await apiCall('/api/status', 'GET');
      update(data.status);
    } catch(e) {}
  }

  // Init
  renderEvents();
  updateTimers();
  if (failureAt && !resolvedAt) startTimer();
  poll();
  setInterval(poll, 4000);
}
