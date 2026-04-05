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
  // Restore state from sessionStorage (survives page navigation)
  let events = JSON.parse(sessionStorage.getItem('sentinelfo_events') || '[]');
  let lastState = sessionStorage.getItem('sentinelfo_lastState') || null;
  let failureInjectedAt = Number(sessionStorage.getItem('sentinelfo_failureAt')) || null;
  let timerInterval = null;
  let failoverDetectedAt = Number(sessionStorage.getItem('sentinelfo_failoverAt')) || null;
  let impactResolvedAt = Number(sessionStorage.getItem('sentinelfo_impactResolved')) || null;

  const eventsContainer = document.getElementById('timeline-events');
  const timerEl = document.getElementById('elapsed-timer');
  const stateBadge = document.getElementById('state-badge');

  // Button handlers
  document.getElementById('btn-inject').addEventListener('click', function() {
    const btn = this;
    btnAction(btn, async () => {
      const data = await apiCall('/api/trigger', 'POST');
      addEvent('Failure injected — ECS scaled to 0 in primary', 'error');
      failureInjectedAt = Date.now();
      sessionStorage.setItem('sentinelfo_failureAt', failureInjectedAt);
      impactResolvedAt = null;
      sessionStorage.removeItem('sentinelfo_impactResolved');
      startTimer();
    });
  });

  document.getElementById('btn-promote').addEventListener('click', function() {
    const btn = this;
    btnAction(btn, async () => {
      const data = await apiCall('/api/aurora/promote', 'POST');
      addEvent('Aurora promotion initiated — ' + (data.message || ''), 'info');
    });
  });

  document.getElementById('btn-failback').addEventListener('click', function() {
    const btn = this;
    btnAction(btn, async () => {
      const data = await apiCall('/api/failback', 'POST');
      addEvent('Failback initiated — restoring primary', 'info');
    });
  });

  document.getElementById('btn-clear').addEventListener('click', function() {
    events = [];
    lastState = null;
    failureInjectedAt = null;
    failoverDetectedAt = null;
    impactResolvedAt = null;
    sessionStorage.removeItem('sentinelfo_events');
    sessionStorage.removeItem('sentinelfo_lastState');
    sessionStorage.removeItem('sentinelfo_failureAt');
    sessionStorage.removeItem('sentinelfo_failoverAt');
    sessionStorage.removeItem('sentinelfo_impactResolved');
    stopTimer();
    timerEl.textContent = '00:00';
    if (document.getElementById('impact-timer')) document.getElementById('impact-timer').textContent = '00:00';
    renderEvents();
  });

  function addEvent(text, type) {
    const now = new Date();
    const timeStr = now.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
    let elapsed = '';
    if (failureInjectedAt) {
      const diff = Math.floor((now.getTime() - failureInjectedAt) / 1000);
      elapsed = '+' + formatSeconds(diff);
    }
    events.push({ time: timeStr, text, type: type || 'info', elapsed });
    sessionStorage.setItem('sentinelfo_events', JSON.stringify(events));
    renderEvents();
  }

  function renderEvents() {
    if (events.length === 0) {
      eventsContainer.innerHTML = '<div class="timeline-empty">Waiting for events...</div>';
      return;
    }
    eventsContainer.innerHTML = events.map(e => {
      const dotClass = 'dot-' + (e.type || 'info');
      return '<div class="timeline-event">' +
        '<span class="event-time">' + e.time + '</span>' +
        '<span class="event-dot ' + dotClass + '"></span>' +
        '<span class="event-text">' + e.text + '</span>' +
        (e.elapsed ? '<span class="event-elapsed">' + e.elapsed + '</span>' : '') +
        '</div>';
    }).join('');
    eventsContainer.scrollTop = eventsContainer.scrollHeight;
  }

  function formatSeconds(s) {
    const m = Math.floor(s / 60);
    const sec = s % 60;
    return String(m).padStart(2, '0') + ':' + String(sec).padStart(2, '0');
  }

  function startTimer() {
    stopTimer();
    timerInterval = setInterval(() => {
      if (failureInjectedAt) {
        const endTime = impactResolvedAt || Date.now();
        const diff = Math.floor((endTime - failureInjectedAt) / 1000);
        timerEl.textContent = formatSeconds(diff);
        // Show detection time separately if we have it
        const impactEl = document.getElementById('impact-timer');
        if (impactEl && failoverDetectedAt) {
          const detDiff = Math.floor((failoverDetectedAt - failureInjectedAt) / 1000);
          impactEl.textContent = formatSeconds(detDiff);
        }
      }
    }, 1000);
  }

  function stopTimer() {
    if (timerInterval) {
      clearInterval(timerInterval);
      timerInterval = null;
    }
  }

  // Update diagram from status
  function updateDiagram(s, l) {
    const st = s.state;
    const currentState = st.state || 'UNKNOWN';
    const activeRegion = st.active_region || 'us-west-1';

    // State badge
    updateStateBadge(currentState);

    // Region boxes
    const w1Box = document.getElementById('region-w1');
    const w2Box = document.getElementById('region-w2');
    const w1Role = document.getElementById('role-w1');
    const w2Role = document.getElementById('role-w2');
    const flowArrow = document.getElementById('flow-arrow');
    const flowLabel = document.getElementById('flow-label');

    // ECS
    const ecsW1 = s.ecs['us-west-1'];
    const ecsW2 = s.ecs['us-west-2'];
    const auroraW1 = typeof s.aurora['us-west-1'] === 'object' ? s.aurora['us-west-1'] : {status: s.aurora['us-west-1'], role: 'unknown'};
    const auroraW2 = typeof s.aurora['us-west-2'] === 'object' ? s.aurora['us-west-2'] : {status: s.aurora['us-west-2'], role: 'unknown'};

    updateServiceNode('svc-r53-w1', 'R53', true, '');
    updateServiceNode('svc-alb-w1', 'ALB', ecsW1.running > 0, '');
    updateServiceNode('svc-ecs-w1', 'ECS', ecsW1.running > 0, ecsW1.running !== null ? ecsW1.running + '/' + ecsW1.desired : '--');
    updateServiceNode('svc-aurora-w1', 'RDS', auroraW1.status === 'available', auroraW1.status);

    updateServiceNode('svc-r53-w2', 'R53', true, '');
    updateServiceNode('svc-alb-w2', 'ALB', ecsW2.running > 0, '');
    updateServiceNode('svc-ecs-w2', 'ECS', ecsW2.running > 0, ecsW2.running !== null ? ecsW2.running + '/' + ecsW2.desired : '--');
    updateServiceNode('svc-aurora-w2', 'RDS', auroraW2.status === 'available', auroraW2.status);

    // Dynamic Aurora writer/reader labels
    const labelW1 = document.getElementById('aurora-label-w1');
    const labelW2 = document.getElementById('aurora-label-w2');
    if (labelW1) labelW1.textContent = 'Aurora' + (auroraW1.role === 'writer' ? ' (Writer)' : auroraW1.role === 'reader' ? ' (Reader)' : '');
    if (labelW2) labelW2.textContent = 'Aurora' + (auroraW2.role === 'writer' ? ' (Writer)' : auroraW2.role === 'reader' ? ' (Reader)' : '');

    // Region states
    w1Box.className = 'region-box';
    w2Box.className = 'region-box';

    if (activeRegion === 'us-west-1') {
      w1Box.classList.add('active');
      w2Box.classList.add('standby');
      w1Role.textContent = 'Active';
      w1Role.className = 'region-role role-active';
      w2Role.textContent = 'Standby';
      w2Role.className = 'region-role role-secondary';
      flowArrow.innerHTML = '&#8594;';
      flowLabel.textContent = 'standby';
      flowArrow.className = 'flow-arrow';
    } else {
      w2Box.classList.add('active');
      w2Role.textContent = 'Active';
      w2Role.className = 'region-role role-active';
      flowArrow.innerHTML = '&#8592;';
      flowLabel.textContent = 'failover';
      flowArrow.className = 'flow-arrow active-flow';

      if (ecsW1.running === 0 || ecsW1.running === null) {
        w1Box.classList.add('failed');
        w1Role.textContent = 'Failed';
        w1Role.className = 'region-role role-failed';
      } else {
        w1Box.classList.add('standby');
        w1Role.textContent = 'Recovering';
        w1Role.className = 'region-role role-secondary';
      }
    }

    if (currentState === 'WAITING_AURORA_PROMOTION') {
      flowArrow.className = 'flow-arrow active-flow';
    }

    // Detect state transitions
    if (lastState && lastState !== currentState) {
      onStateTransition(lastState, currentState, s);
    }

    // Detect consecutive failure changes
    if (lastState === currentState && currentState === 'PRIMARY_ACTIVE') {
      const failures = st.consecutive_failures || 0;
      if (failures > 0 && failureInjectedAt) {
        // Check if we already logged this failure count
        const failText = 'Detecting failure (' + failures + '/3)';
        const alreadyLogged = events.some(e => e.text === failText);
        if (!alreadyLogged) {
          addEvent(failText, 'warn');
        }
      }
    }

    lastState = currentState;
    sessionStorage.setItem('sentinelfo_lastState', currentState);
  }

  function onStateTransition(from, to, s) {
    if (from === 'PRIMARY_ACTIVE' && to === 'WAITING_AURORA_PROMOTION') {
      failoverDetectedAt = Date.now();
      sessionStorage.setItem('sentinelfo_failoverAt', failoverDetectedAt);
      let detectionTime = '';
      if (failureInjectedAt) {
        const diff = Math.floor((failoverDetectedAt - failureInjectedAt) / 1000);
        detectionTime = ' (detected in ' + formatSeconds(diff) + ')';
      }
      addEvent('FAILOVER TRIGGERED' + detectionTime + ' — DNS switched to us-west-2', 'error');
      addEvent('Waiting for Aurora promotion...', 'warn');
    } else if (from === 'WAITING_AURORA_PROMOTION' && to === 'SECONDARY_ACTIVE') {
      impactResolvedAt = Date.now();
      sessionStorage.setItem('sentinelfo_impactResolved', impactResolvedAt);
      let impactTime = '';
      if (failureInjectedAt) {
        const diff = Math.floor((impactResolvedAt - failureInjectedAt) / 1000);
        impactTime = ' (total impact: ' + formatSeconds(diff) + ')';
      }
      addEvent('Aurora promoted — app fully operational' + impactTime, 'success');
      stopTimer();
    } else if (to === 'PRIMARY_ACTIVE' && from !== 'PRIMARY_ACTIVE') {
      addEvent('Failback complete — primary is active', 'success');
      stopTimer();
    } else if (from === 'SECONDARY_ACTIVE' && to === 'FAILBACK_IN_PROGRESS') {
      addEvent('Failback in progress...', 'info');
    } else {
      addEvent('State changed: ' + from + ' -> ' + to, 'info');
    }
  }

  function updateStateBadge(state) {
    const classes = {
      'PRIMARY_ACTIVE': 'state-primary-active',
      'WAITING_AURORA_PROMOTION': 'state-waiting',
      'SECONDARY_ACTIVE': 'state-secondary-active',
      'FAILBACK_IN_PROGRESS': 'state-failback',
    };
    stateBadge.textContent = state.replace(/_/g, ' ');
    stateBadge.className = 'state-badge ' + (classes[state] || 'state-unknown');
  }

  function updateServiceNode(id, label, healthy, detail) {
    const el = document.getElementById(id);
    if (!el) return;
    el.className = 'service-node ' + (healthy ? 'healthy' : 'unhealthy');
    const detailEl = el.querySelector('.service-detail');
    if (detailEl && detail) detailEl.textContent = detail;
  }

  // Poll
  async function refreshDemoStatus() {
    try {
      const data = await apiCall('/api/status', 'GET');
      updateDiagram(data.status, data.lock);
    } catch (e) {
      // Silent
    }
  }

  renderEvents();
  // Restart timer if we had an active failure injection
  if (failureInjectedAt && !impactResolvedAt) {
    startTimer();
  } else if (failureInjectedAt && impactResolvedAt) {
    // Show final time
    const diff = Math.floor((impactResolvedAt - failureInjectedAt) / 1000);
    timerEl.textContent = formatSeconds(diff);
  }
  refreshDemoStatus();
  setInterval(refreshDemoStatus, 5000);
}
