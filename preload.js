const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('api', {
  onLog: (callback) => ipcRenderer.on('log', (event, data) => callback(data)),
  onSetMode: (callback) => ipcRenderer.on('set-mode', (event, mode) => callback(mode)),
  startBridge: (ip, port) => ipcRenderer.invoke('start-bridge', { ip, port }),
  stopBridge: () => ipcRenderer.invoke('stop-bridge'),
});