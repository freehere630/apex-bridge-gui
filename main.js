import { app, BrowserWindow, ipcMain } from 'electron';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// BULLETPROOF ENV LOADING
// If packaged, look in the same folder as the .exe. Otherwise look in the project root.
let envPath;
if (app.isPackaged) {
  envPath = path.join(path.dirname(process.execPath), '.env');
} else {
  envPath = path.join(__dirname, '.env');
}

dotenv.config({ path: envPath });

const exeName = path.basename(process.execPath).toLowerCase();
const isMaglumi = exeName.includes('maglumi') || process.argv.includes('--mode=maglumi');
const mode = isMaglumi ? 'maglumi' : 'arrows'; 

let mainWindow;
let activeServer = null;

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1000,
    height: 750,
    title: isMaglumi ? 'Maglumi X3 Bridge' : 'Arrows KT-44 Bridge',
    backgroundColor: '#f8fafc',
    autoHideMenuBar: true,
    show: false, // Prevents white flash
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      nodeIntegration: false,
      contextIsolation: true,
    },
  });

  mainWindow.loadFile(path.join(__dirname, 'ui/index.html'));
  
  mainWindow.once('ready-to-show', () => {
    mainWindow.show();
  });
}

app.whenReady().then(() => {
  createWindow();

  mainWindow.webContents.on('did-finish-load', () => {
    mainWindow.webContents.send('set-mode', mode);
    
    if (!process.env.SUPABASE_URL) {
      mainWindow.webContents.send('log', { 
        tag: 'ERR', 
        message: 'Missing .env file! Make sure it is in the same folder as the .exe' 
      });
    }
  });

  ipcMain.handle('start-bridge', async (event, { ip, port }) => {
    if (activeServer) return { success: false, message: 'Bridge already running' };
    if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
      return { success: false, message: 'Missing Supabase credentials in .env file.' };
    }

    try {
      const logCallback = (logData) => mainWindow.webContents.send('log', logData);
      if (isMaglumi) {
        const { startMaglumiBridge } = await import('./bridges/maglumi.js');
        activeServer = await startMaglumiBridge(ip, port, logCallback);
      } else {
        const { startArrowsBridge } = await import('./bridges/arrows.js');
        activeServer = await startArrowsBridge(ip, port, logCallback);
      }
      return { success: true };
    } catch (err) {
      return { success: false, message: err.message };
    }
  });

  ipcMain.handle('stop-bridge', async () => {
    if (activeServer) {
      activeServer.close();
      activeServer = null;
      return { success: true };
    }
    return { success: false, message: 'Bridge not running' };
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});