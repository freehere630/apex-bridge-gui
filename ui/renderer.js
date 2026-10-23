const logContainer = document.getElementById('log-container');
const connectBtn = document.getElementById('connect-btn');
const ipInput = document.getElementById('ip-input');
const portInput = document.getElementById('port-input');
let isConnected = false;

window.api.onSetMode((mode) => {
  const title = mode === 'arrows' ? 'ARROWS KT-44' : 'MAGLUMI X3';
  document.getElementById('header-title').textContent = `APEX LIS BRIDGE — ${title}`;
  document.getElementById('header-subtitle').textContent = `Configure connection settings below`;
  
  // Set default ports
  if (mode === 'arrows') portInput.value = 5200;
  if (mode === 'maglumi') portInput.value = 5003;
});

connectBtn.addEventListener('click', async () => {
  if (isConnected) {
    // Disconnect
    connectBtn.textContent = 'Disconnecting...';
    connectBtn.disabled = true;
    const res = await window.api.stopBridge();
    if (res.success) {
      isConnected = false;
      connectBtn.textContent = 'Connect & Listen';
      connectBtn.classList.remove('disconnect');
      connectBtn.disabled = false;
      ipInput.disabled = false;
      portInput.disabled = false;
      updateTcpStatus('Offline', false);
    }
  } else {
    // Connect
    const ip = ipInput.value.trim();
    const port = parseInt(portInput.value);
    if (!ip || !port) return alert('Please enter a valid IP and Port');

    connectBtn.textContent = 'Connecting...';
    connectBtn.disabled = true;
    
    const res = await window.api.startBridge(ip, port);
    
    if (res.success) {
      isConnected = true;
      connectBtn.textContent = 'Disconnect';
      connectBtn.classList.add('disconnect');
      connectBtn.disabled = false;
      ipInput.disabled = true;
      portInput.disabled = true;
      updateTcpStatus('Listening', true);
    } else {
      alert('Failed to start: ' + res.message);
      connectBtn.textContent = 'Connect & Listen';
      connectBtn.disabled = false;
    }
  }
});

function updateTcpStatus(text, ok) {
  const el = document.getElementById('tcp-status');
  el.textContent = text;
  el.className = `value ${ok ? 'text-green' : 'text-red'}`;
}

window.api.onLog((data) => {
  const { tag, message, statusUpdate } = data;

  if (statusUpdate) {
    if (statusUpdate.type === 'db') {
      const el = document.getElementById('db-status');
      el.textContent = statusUpdate.text;
      el.className = `value ${statusUpdate.ok ? 'text-green' : 'text-red'}`;
    }
    if (statusUpdate.type === 'tcp') {
      updateTcpStatus(statusUpdate.text, statusUpdate.ok);
    }
  }

  if (tag && message) {
    const time = new Date().toLocaleTimeString('en-US', { hour12: false });
    const line = document.createElement('div');
    line.className = 'log-line';
    const cleanTag = tag.replace(/[\[\]]/g, '');
    
    line.innerHTML = `
      <span class="time">${time}</span>
      <span class="tag tag-${cleanTag}">${cleanTag}</span>
      <span class="msg">${message}</span>
    `;
    
    logContainer.appendChild(line);
    logContainer.scrollTop = logContainer.scrollHeight;
  }
});

document.getElementById('clear-logs').addEventListener('click', () => {
  logContainer.innerHTML = '';
});