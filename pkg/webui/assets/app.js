// get-torrent WebUI Client (Transmission RPC Compatible)
(() => {
  const RPC_URL = '/transmission/rpc';
  let sessionId = '';
  let torrents = [];
  let session = {};
  let currentFilter = 'all';
  let searchQuery = '';
  let activeTorrentToRemove = null;

  // Formatting utilities
  function formatBytes(bytes) {
    if (!bytes || bytes <= 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  function formatSpeed(bytesPerSec) {
    if (!bytesPerSec || bytesPerSec <= 0) return '0.0 KB/s';
    if (bytesPerSec < 1024 * 1024) {
      return (bytesPerSec / 1024).toFixed(1) + ' KB/s';
    }
    return (bytesPerSec / (1024 * 1024)).toFixed(2) + ' MB/s';
  }

  function formatETA(seconds) {
    if (seconds < 0 || seconds == null || seconds >= 3600000) return 'Unknown';
    if (seconds === 0) return 'Done';
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = seconds % 60;
    if (h > 0) return `${h}h ${m}m`;
    if (m > 0) return `${m}m ${s}s`;
    return `${s}s`;
  }

  function mapStatus(status) {
    // Status can be string or Transmission integer code
    if (typeof status === 'string') {
      return { key: status.toLowerCase(), label: status };
    }
    switch (status) {
      case 0: return { key: 'stopped', label: 'Paused' };
      case 1: return { key: 'queued', label: 'Queued Check' };
      case 2: return { key: 'checking', label: 'Checking' };
      case 3: return { key: 'queued', label: 'Queued Download' };
      case 4: return { key: 'downloading', label: 'Downloading' };
      case 5: return { key: 'queued', label: 'Queued Seed' };
      case 6: return { key: 'seeding', label: 'Seeding' };
      default: return { key: 'unknown', label: 'Unknown' };
    }
  }

  // Core Transmission RPC Client
  async function rpcCall(method, args = {}) {
    const body = JSON.stringify({ method, arguments: args, tag: Date.now() });

    const makeRequest = async (token) => {
      const headers = { 'Content-Type': 'application/json' };
      if (token) headers['X-Transmission-Session-Id'] = token;
      return fetch(RPC_URL, { method: 'POST', headers, body });
    };

    let response = await makeRequest(sessionId);

    // Handle CSRF 409 conflict transparently
    if (response.status === 409) {
      const newSession = response.headers.get('X-Transmission-Session-Id');
      if (newSession) {
        sessionId = newSession;
        response = await makeRequest(sessionId);
      }
    }

    if (!response.ok) {
      throw new Error(`RPC call failed with HTTP ${response.status}`);
    }

    const data = await response.json();
    if (data.result !== 'success') {
      throw new Error(`RPC Error (${method}): ${data.result}`);
    }
    return data.arguments || {};
  }

  // Polling data
  async function refreshData() {
    try {
      const [torrentData, sessionData] = await Promise.all([
        rpcCall('torrent-get', {
          fields: [
            'id', 'name', 'status', 'totalSize', 'percentDone',
            'rateDownload', 'rateUpload', 'leftUntilDone', 'peersConnected',
            'uploadedEver', 'downloadedEver', 'eta', 'metadataPercentComplete', 'magnetLink'
          ]
        }),
        rpcCall('session-get')
      ]);

      torrents = torrentData.torrents || [];
      session = sessionData || {};

      updateHeaderStats();
      renderTorrents();
      document.getElementById('connection-status').textContent = '● Connected to /transmission/rpc';
      document.getElementById('connection-status').className = 'status-online';
    } catch (err) {
      console.warn('Refresh error:', err);
      document.getElementById('connection-status').textContent = '○ Disconnected / Reconnecting...';
      document.getElementById('connection-status').className = 'status-offline';
    }
  }

  function updateHeaderStats() {
    let totalDownSpeed = 0;
    let totalUpSpeed = 0;
    torrents.forEach(t => {
      totalDownSpeed += (t.rateDownload || 0);
      totalUpSpeed += (t.rateUpload || 0);
    });

    document.getElementById('stat-down-speed').textContent = formatSpeed(totalDownSpeed);
    document.getElementById('stat-up-speed').textContent = formatSpeed(totalUpSpeed);

    if (session['download-dir-free-space'] !== undefined) {
      document.getElementById('stat-free-space').textContent = formatBytes(session['download-dir-free-space']);
    }
    if (session['download-dir']) {
      document.getElementById('session-download-dir').textContent = `Download Dir: ${session['download-dir']}`;
    }
  }

  function renderTorrents() {
    const tbody = document.getElementById('torrent-list');
    const emptyState = document.getElementById('empty-state');

    // Counts for filter pills
    let countAll = torrents.length;
    let countDownloading = 0;
    let countSeeding = 0;
    let countPaused = 0;

    torrents.forEach(t => {
      const st = mapStatus(t.status).key;
      if (st === 'downloading') countDownloading++;
      else if (st === 'seeding') countSeeding++;
      else if (st === 'stopped' || st === 'paused') countPaused++;
    });

    document.getElementById('count-all').textContent = countAll;
    document.getElementById('count-downloading').textContent = countDownloading;
    document.getElementById('count-seeding').textContent = countSeeding;
    document.getElementById('count-paused').textContent = countPaused;

    // Filtering
    const filtered = torrents.filter(t => {
      const st = mapStatus(t.status).key;
      if (currentFilter === 'downloading' && st !== 'downloading') return false;
      if (currentFilter === 'seeding' && st !== 'seeding') return false;
      if (currentFilter === 'paused' && st !== 'stopped' && st !== 'paused') return false;
      if (searchQuery && !t.name.toLowerCase().includes(searchQuery.toLowerCase())) return false;
      return true;
    });

    if (filtered.length === 0) {
      tbody.innerHTML = '';
      emptyState.style.display = 'flex';
      return;
    }
    emptyState.style.display = 'none';

    tbody.innerHTML = filtered.map(t => {
      const st = mapStatus(t.status);
      const isComplete = t.percentDone >= 1.0;
      const pctDisplay = (t.percentDone * 100).toFixed(1);
      const isPaused = st.key === 'stopped' || st.key === 'paused';

      return `
        <tr data-id="${t.id}">
          <td class="col-name">
            <div class="torrent-title" title="${escapeHtml(t.name)}">${escapeHtml(t.name)}</div>
            <div class="torrent-sub">${formatBytes(t.downloadedEver || 0)} of ${formatBytes(t.totalSize)}</div>
          </td>
          <td class="col-status">
            <span class="status-pill ${st.key}">${st.label}</span>
          </td>
          <td class="col-size">${formatBytes(t.totalSize)}</td>
          <td class="col-progress">
            <div class="progress-bar-container">
              <div class="progress-track">
                <div class="progress-fill ${isComplete ? 'seeding' : ''}" style="width: ${pctDisplay}%"></div>
              </div>
              <span class="progress-text">${pctDisplay}%</span>
            </div>
          </td>
          <td class="col-speed">
            <div><span class="stat-icon down">↓</span> ${formatSpeed(t.rateDownload)}</div>
            <div><span class="stat-icon up">↑</span> ${formatSpeed(t.rateUpload)}</div>
          </td>
          <td class="col-peers">${t.peersConnected || 0}</td>
          <td class="col-eta">${isComplete ? 'Complete' : formatETA(t.eta)}</td>
          <td class="col-actions">
            ${isPaused
              ? `<button class="btn btn-sm btn-secondary btn-action-start" data-id="${t.id}" title="Start">▶️</button>`
              : `<button class="btn btn-sm btn-secondary btn-action-stop" data-id="${t.id}" title="Pause">⏸️</button>`
            }
            <button class="btn btn-sm btn-danger btn-action-remove" data-id="${t.id}" data-name="${escapeHtml(t.name)}" title="Remove">🗑️</button>
          </td>
        </tr>
      `;
    }).join('');
  }

  function escapeHtml(str) {
    if (!str) return '';
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  // Event handlers
  function setupEvents() {
    // Filter tabs
    document.querySelectorAll('.filter-tab').forEach(tab => {
      tab.addEventListener('click', () => {
        document.querySelectorAll('.filter-tab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        currentFilter = tab.dataset.filter;
        renderTorrents();
      });
    });

    // Search input
    document.getElementById('search-input').addEventListener('input', (e) => {
      searchQuery = e.target.value.trim();
      renderTorrents();
    });

    // Start / Stop All
    document.getElementById('btn-start-all').addEventListener('click', async () => {
      try {
        await rpcCall('torrent-start');
        await refreshData();
      } catch (err) { alert(err.message); }
    });

    document.getElementById('btn-stop-all').addEventListener('click', async () => {
      try {
        await rpcCall('torrent-stop');
        await refreshData();
      } catch (err) { alert(err.message); }
    });

    // Delegated row action buttons
    document.getElementById('torrent-list').addEventListener('click', async (e) => {
      const target = e.target.closest('button');
      if (!target) return;
      const id = parseInt(target.dataset.id, 10);

      if (target.classList.contains('btn-action-start')) {
        await rpcCall('torrent-start', { ids: [id] });
        await refreshData();
      } else if (target.classList.contains('btn-action-stop')) {
        await rpcCall('torrent-stop', { ids: [id] });
        await refreshData();
      } else if (target.classList.contains('btn-action-remove')) {
        activeTorrentToRemove = { id, name: target.dataset.name };
        document.getElementById('remove-torrent-name').textContent = activeTorrentToRemove.name;
        document.getElementById('remove-delete-data').checked = false;
        openModal('modal-remove');
      }
    });

    // Confirm remove
    document.getElementById('btn-confirm-remove').addEventListener('click', async () => {
      if (!activeTorrentToRemove) return;
      const deleteData = document.getElementById('remove-delete-data').checked;
      try {
        await rpcCall('torrent-remove', {
          ids: [activeTorrentToRemove.id],
          'delete-local-data': deleteData
        });
        closeModal('modal-remove');
        await refreshData();
      } catch (err) { alert(err.message); }
    });

    // Add Torrent Modal
    document.getElementById('btn-add-torrent').addEventListener('click', () => {
      document.getElementById('magnet-input').value = '';
      document.getElementById('torrent-file-input').value = '';
      document.getElementById('add-download-dir').value = session['download-dir'] || '';
      openModal('modal-add');
    });

    document.getElementById('btn-submit-add').addEventListener('click', async () => {
      const magnet = document.getElementById('magnet-input').value.trim();
      const fileInput = document.getElementById('torrent-file-input');
      const downloadDir = document.getElementById('add-download-dir').value.trim();
      const paused = !document.getElementById('add-start-now').checked;

      try {
        if (fileInput.files.length > 0) {
          const file = fileInput.files[0];
          const reader = new FileReader();
          reader.onload = async () => {
            const base64 = reader.result.split(',')[1];
            await rpcCall('torrent-add', {
              metainfo: base64,
              paused,
              ...(downloadDir ? { 'download-dir': downloadDir } : {})
            });
            closeModal('modal-add');
            await refreshData();
          };
          reader.readAsDataURL(file);
        } else if (magnet) {
          await rpcCall('torrent-add', {
            filename: magnet,
            paused,
            ...(downloadDir ? { 'download-dir': downloadDir } : {})
          });
          closeModal('modal-add');
          await refreshData();
        } else {
          alert('Please enter a magnet link or select a .torrent file.');
        }
      } catch (err) {
        alert(err.message);
      }
    });

    // Settings Modal
    document.getElementById('btn-settings').addEventListener('click', () => {
      document.getElementById('setting-download-dir').value = session['download-dir'] || '';
      document.getElementById('setting-limit-down').value = session['speed-limit-down'] ? Math.round(session['speed-limit-down'] / 1024) : 0;
      document.getElementById('setting-limit-down-enabled').checked = !!session['speed-limit-down-enabled'];
      document.getElementById('setting-limit-up').value = session['speed-limit-up'] ? Math.round(session['speed-limit-up'] / 1024) : 0;
      document.getElementById('setting-limit-up-enabled').checked = !!session['speed-limit-up-enabled'];
      document.getElementById('setting-peer-port').textContent = `Peer Port: ${session['peer-port'] || '-'}`;
      document.getElementById('port-test-result').textContent = '';
      openModal('modal-settings');
    });

    document.getElementById('btn-test-port').addEventListener('click', async () => {
      const resSpan = document.getElementById('port-test-result');
      resSpan.textContent = 'Testing...';
      resSpan.className = 'port-result';
      try {
        const res = await rpcCall('port-test');
        if (res['port-is-open']) {
          resSpan.textContent = 'OPEN (Reachable)';
          resSpan.className = 'port-result open';
        } else {
          resSpan.textContent = 'CLOSED';
          resSpan.className = 'port-result closed';
        }
      } catch (err) {
        resSpan.textContent = 'Error testing port';
      }
    });

    document.getElementById('btn-submit-settings').addEventListener('click', async () => {
      const downloadDir = document.getElementById('setting-download-dir').value.trim();
      const downLimit = parseInt(document.getElementById('setting-limit-down').value, 10) * 1024;
      const downEnabled = document.getElementById('setting-limit-down-enabled').checked;
      const upLimit = parseInt(document.getElementById('setting-limit-up').value, 10) * 1024;
      const upEnabled = document.getElementById('setting-limit-up-enabled').checked;

      try {
        await rpcCall('session-set', {
          'download-dir': downloadDir,
          'speed-limit-down': downLimit,
          'speed-limit-down-enabled': downEnabled,
          'speed-limit-up': upLimit,
          'speed-limit-up-enabled': upEnabled
        });
        closeModal('modal-settings');
        await refreshData();
      } catch (err) {
        alert(err.message);
      }
    });

    // Search Modal & Torrent Finder
    const searchInput = document.getElementById('indexer-search-input');
    const searchSpinner = document.getElementById('search-spinner');
    const searchResultsList = document.getElementById('search-results-list');
    const searchEmptyState = document.getElementById('search-empty-state');

    document.getElementById('btn-search-torrent').addEventListener('click', () => {
      openModal('modal-search');
      searchInput.focus();
    });

    const executeSearch = async () => {
      const q = searchInput.value.trim();
      if (!q) return;

      searchSpinner.classList.remove('hidden');
      searchResultsList.innerHTML = '';
      searchEmptyState.style.display = 'none';

      try {
        const data = await rpcCall('torrent-search', { query: q });
        const results = data.results || [];
        searchSpinner.classList.add('hidden');

        if (results.length === 0) {
          searchEmptyState.innerHTML = '<p>No torrents found matching your query.</p>';
          searchEmptyState.style.display = 'flex';
          return;
        }

        searchResultsList.innerHTML = results.map((r, i) => `
          <tr>
            <td>
              <div class="torrent-title" title="${escapeHtml(r.name)}">${escapeHtml(r.name)}</div>
            </td>
            <td><span class="category-tag">${escapeHtml(r.category || 'General')}</span></td>
            <td>${formatBytes(r.size)}</td>
            <td class="seeders-count">${r.seeders || 0}</td>
            <td class="leechers-count">${r.leechers || 0}</td>
            <td style="text-align: right;">
              <button class="btn btn-sm btn-primary btn-add-search-result" data-index="${i}">➕ Download</button>
            </td>
          </tr>
        `).join('');

        // Wire 1-click download buttons
        searchResultsList.querySelectorAll('.btn-add-search-result').forEach(btn => {
          btn.addEventListener('click', async (e) => {
            const idx = parseInt(e.target.dataset.index, 10);
            const item = results[idx];
            if (!item || !item.magnetLink) return;

            e.target.textContent = 'Adding...';
            e.target.disabled = true;

            try {
              await rpcCall('torrent-add', { filename: item.magnetLink });
              e.target.textContent = '✓ Added';
              e.target.classList.remove('btn-primary');
              e.target.classList.add('btn-secondary');
              await refreshData();
            } catch (err) {
              alert('Error adding torrent: ' + err.message);
              e.target.textContent = '➕ Download';
              e.target.disabled = false;
            }
          });
        });
      } catch (err) {
        searchSpinner.classList.add('hidden');
        searchEmptyState.innerHTML = `<p style="color: var(--accent-danger);">Error querying indexer: ${escapeHtml(err.message)}</p>`;
        searchEmptyState.style.display = 'flex';
      }
    };

    document.getElementById('btn-execute-search').addEventListener('click', executeSearch);
    searchInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') executeSearch();
    });

    // Close modals
    document.querySelectorAll('[data-close]').forEach(btn => {
      btn.addEventListener('click', () => {
        closeModal(btn.dataset.close);
      });
    });
  }

  function openModal(id) {
    document.getElementById(id).classList.remove('hidden');
  }

  function closeModal(id) {
    document.getElementById(id).classList.add('hidden');
  }

  // Start polling
  setupEvents();
  refreshData();
  setInterval(refreshData, 2000);
})();
