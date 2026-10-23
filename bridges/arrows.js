import WebSocket from 'ws';
import net from 'net';
import { createClient } from '@supabase/supabase-js';

// ASTM Framing Bytes
const ACK = 0x06, ENQ = 0x05, STX = 0x02, ETX = 0x03;

export function startArrowsBridge(ip, port, emitLog) {
  return new Promise((resolve, reject) => {
    const supabase = createClient(
  process.env.SUPABASE_URL, 
  process.env.SUPABASE_SERVICE_ROLE_KEY,
  {
    realtime: {
      transport: WebSocket,
    },
  }
);
    
    const dynamicMappings = new Map();
    const pendingSamples = new Map();
    const paramIdCache = new Map();
    const paramNameByIdCache = new Map();
    
    let currentBarcode = null;
    let rawBuffer = Buffer.alloc(0);
    let clients = 0;

    // Helper for modern UI logging
    const log = (tag, message) => emitLog({ tag, message });

    // Intermediates & Derived Keys
    const INTERMEDIATE_CODES = new Set(['10029', '10027', '_MID_PCT_', '_MID_ABS_', 'MID%', 'MID#', 'Mid%', 'Mid#']);
    const DERIVED_KEYS = {
      'Neutrophils': 'Neutrophils', 'Lymphocytes': 'Lymphocytes', 'Monocytes': 'Monocytes',
      'Eosinophils': 'Eosinophils', 'Basophils': 'Basophils', 'ANC': 'Absolute Neutrophil Count (ANC)',
      'ALC': 'Absolute Lymphocyte Count (ALC)', 'AMC': 'Absolute Monocyte Count (AMC)',
      'AEC': 'Absolute Eosinophil Count (AEC)', 'ABC': 'Absolute Basophil Count (ABC)', 'PCT': 'Plateletcrit (PCT)',
    };

    // ============================================================
    // DYNAMIC MAPPINGS
    // ============================================================
    async function loadMappings() {
      try {
        const { data, error } = await supabase
          .from('analyzer_mappings')
          .select('machine_code, lims_parameter_id')
          .eq('analyzer_name', 'Arrows KT-44');
          
        if (error) throw error;

        dynamicMappings.clear();
        data?.forEach(m => dynamicMappings.set(m.machine_code.toUpperCase(), m.lims_parameter_id));
        
        emitLog({ statusUpdate: { type: 'mapping', text: `${data?.length || 0} Loaded`, count: data?.length || 0 } });
        log('SYNC', `Loaded ${data?.length || 0} active mappings`);
      } catch (e) {
        log('ERR', `Sync Failed: ${e.message}`);
      }
    }

    // Connect to DB & Start Heartbeat
    (async () => {
      try {
        const { error } = await supabase.from('orders').select('id').limit(1);
        if (error) throw error;
        
        emitLog({ statusUpdate: { type: 'db', text: 'Live', ok: true } });
        await loadMappings(); 
        
        setInterval(loadMappings, 5 * 60 * 1000);
        setInterval(async () => {
          await supabase.from('bridge_heartbeats').upsert({ 
            analyzer_name: 'Arrows KT-44', 
            last_ping: new Date().toISOString() 
          });
        }, 60000);
        
      } catch (e) {
        emitLog({ statusUpdate: { type: 'db', text: 'Offline', ok: false } });
        log('ERR', `Database Connection Failed: ${e.message}`);
      }
    })();

    // ============================================================
    // DB HELPERS
    // ============================================================
    function normalizeValueForLims(limsName, rawValue) {
      const n = parseFloat(String(rawValue).replace(/,/g, ''));
      if (isNaN(n)) return String(rawValue).trim();
      const name = String(limsName || '').toUpperCase();

      if (name.includes('TOTAL LEUCOCYTE') || name.includes('WBC')) {
        if (n > 0 && n < 100) return String(Math.round(n * 1000));
        return String(Math.round(n));
      }
      if (name.includes('TOTAL PLATELET') || name.includes('PLATELET COUNT')) {
        if (n > 0 && n < 1000) return String(Math.round(n * 1000));
        return String(Math.round(n));
      }
      if (name.includes('PACKED CELL') || name.includes('PCV') || name.includes('HEMATOCRIT') || name.includes('HAEMATOCRIT')) {
        if (n > 0 && n < 1) return (n * 100).toFixed(1);
        return n.toFixed(1);
      }
      return String(rawValue).trim();
    }

    async function getOrderIdByBarcode(barcode) {
      const { data } = await supabase.from('orders').select('id').eq('barcode', barcode).maybeSingle();
      return data?.id;
    }

    async function loadParamMapForOrder(orderId) {
      if (paramIdCache.has(orderId)) return paramIdCache.get(orderId);
      const { data: orderTests } = await supabase.from('order_tests').select('test:tests(test_parameters(*))').eq('order_id', orderId);
      const map = {}, byId = {};
      if (orderTests) {
        for (const ot of orderTests) {
          const params = ot.test?.test_parameters || [];
          for (const p of params) {
            if (p.name) { map[p.name.trim().toUpperCase()] = p.id; byId[p.id] = p.name; }
          }
        }
      }
      paramIdCache.set(orderId, map);
      paramNameByIdCache.set(orderId, byId);
      return map;
    }

    async function resolveLimsParamId(orderId, code) {
      const codeUp = String(code || '').toUpperCase().trim();
      if (INTERMEDIATE_CODES.has(codeUp)) return '__INTERMEDIATE__';
      return dynamicMappings.get(codeUp) || null;
    }

    async function saveParamToSupabase(orderId, code, value) {
      const paramId = await resolveLimsParamId(orderId, code);
      if (paramId === '__INTERMEDIATE__') return false;
      if (!paramId) { log('UNMAPPED', `'${code}' not mapped`); return false; }

      const byId = paramNameByIdCache.get(orderId) || {};
      const limsName = byId[paramId] || code;
      const normalizedValue = normalizeValueForLims(limsName, value);

      const { error } = await supabase.from('results').upsert(
        { order_id: orderId, parameter_id: paramId, result_value: normalizedValue, status_flag: 'AUTOMATED', analyzer_source: 'ARROWS_KT44' },
        { onConflict: 'order_id,parameter_id' }
      );
      return !error;
    }

    async function saveDerivedToLims(orderId, limsName, value) {
      const paramMap = await loadParamMapForOrder(orderId);
      const paramId = paramMap[limsName.toUpperCase()];
      if (!paramId) return false;
      const { error } = await supabase.from('results').upsert(
        { order_id: orderId, parameter_id: paramId, result_value: String(value), status_flag: 'AUTOMATED', analyzer_source: 'ARROWS_KT44' },
        { onConflict: 'order_id,parameter_id' }
      );
      return !error;
    }

    // ============================================================
    // STAGING & DERIVATION
    // ============================================================
    function stageResult(barcode, parsed, orderId) {
      if (!pendingSamples.has(barcode)) {
        pendingSamples.set(barcode, { orderId, params: {}, timeout: null });
      }
      const entry = pendingSamples.get(barcode);
      entry.params[parsed.code || parsed.name] = parsed;
      if (entry.timeout) clearTimeout(entry.timeout);
      entry.timeout = setTimeout(() => { flushSample(barcode).catch(e => log('ERR', e.message)); }, 3000);
    }

    async function flushSample(barcode) {
      const entry = pendingSamples.get(barcode);
      if (!entry) return;
      pendingSamples.delete(barcode);
      if (entry.timeout) clearTimeout(entry.timeout);

      const { orderId, params } = entry;
      const keys = Object.keys(params);
      if (keys.length === 0) return;

      // Check Lock
      const { data: order } = await supabase.from('orders').select('is_locked, qc_status').eq('id', orderId).maybeSingle();
      if (order?.is_locked || order?.qc_status === 'Verified') {
        log('LOCKED', `[${barcode}] Order is verified. Data discarded.`);
        return;
      }

      let rawSaved = 0;
      for (const key of keys) {
        const p = params[key];
        const ok = await saveParamToSupabase(orderId, p.code, String(p.value));
        if (ok) rawSaved++;
      }

      const getVal = (codeOrName) => {
        for (const k of keys) {
          const p = params[k];
          if (p.code === codeOrName || p.name === codeOrName || k === codeOrName) {
            const n = parseFloat(String(p.value).replace(/,/g, ''));
            return isNaN(n) ? 0 : n;
          }
        }
        return 0;
      };

      const granPct = getVal('10030') || getVal('Gran%');
      const midPct  = getVal('10029') || getVal('Mid%');
      const granAbs = getVal('10028') || getVal('Gran#');
      const midAbs  = getVal('10027') || getVal('Mid#');

      let wbcRaw = getVal('6690-2') || getVal('WBC') || 0;
      if (wbcRaw > 0 && wbcRaw < 100) wbcRaw = wbcRaw * 1000;

      const derived = {};

      if (granPct > 0 || midPct > 0) {
        const neutPct = granPct;
        const monoPct = midPct * 0.70;
        const eosPct  = midPct * 0.25;
        const basPct  = midPct * 0.05;

        derived['Monocytes']   = monoPct.toFixed(1);
        derived['Eosinophils'] = eosPct.toFixed(1);
        derived['Basophils']   = basPct.toFixed(1);

        if (wbcRaw > 0) {
          const wbcK = wbcRaw / 1000;
          if (granAbs > 0) derived['ANC'] = granAbs.toFixed(2);
          else derived['ANC'] = ((wbcK * neutPct) / 100).toFixed(2);
          
          const lymPct = getVal('736-9') || getVal('Lymph%') || 0;
          if (lymPct > 0) {
            const alcVal = getVal('731-0');
            derived['ALC'] = alcVal > 0 ? alcVal.toFixed(2) : ((wbcK * lymPct) / 100).toFixed(2);
          }
          if (midAbs > 0) {
            derived['AMC'] = (midAbs * 0.70).toFixed(2);
            derived['AEC'] = String(Math.round(wbcRaw * eosPct / 100));
            derived['ABC'] = (midAbs * 0.05).toFixed(2);
          } else {
            derived['AMC'] = ((wbcK * monoPct) / 100).toFixed(2);
            derived['AEC'] = String(Math.round((wbcRaw * eosPct) / 100));
            derived['ABC'] = ((wbcK * basPct) / 100).toFixed(2);
          }
        }
      }

      const plt = getVal('777-3') || getVal('PLT') || 0;
      const mpv = getVal('32623-1') || getVal('MPV') || 0;
      if (!getVal('10002') && plt > 0 && mpv > 0) {
        derived['PCT'] = (plt * mpv / 10000).toFixed(2);
      }

      let derivedSaved = 0;
      for (const [k, v] of Object.entries(derived)) {
        const limsName = DERIVED_KEYS[k] || k;
        const ok = await saveDerivedToLims(orderId, limsName, v);
        if (ok) { derivedSaved++; log('DERIVED', `[${barcode}] ${limsName} = ${v}`); }
      }

      log('FLUSH', `[${barcode}] ${rawSaved} raw + ${derivedSaved} derived saved`);
      paramIdCache.delete(orderId);
      paramNameByIdCache.delete(orderId);
    }

    // ============================================================
    // TCP SERVER & ASTM PARSER
    // ============================================================
    const server = net.createServer((socket) => {
      clients++;
      emitLog({ statusUpdate: { type: 'tcp', text: 'Connected', ok: true } });
      log('LINK', `Arrows KT-44 connected`);
      
      rawBuffer = Buffer.alloc(0);
      currentBarcode = null;

      try { socket.write(Buffer.from([ENQ])); } catch (e) {}

      socket.on('data', async (chunk) => {
        try { socket.write(Buffer.from([ACK])); } catch (e) {}
        rawBuffer = Buffer.concat([rawBuffer, chunk]);

        while (true) {
          const stxIdx = rawBuffer.indexOf(STX);
          const etxIdx = rawBuffer.indexOf(ETX);

          if (stxIdx === -1 || etxIdx === -1 || etxIdx < stxIdx) {
            if (rawBuffer.length > 4096) rawBuffer = Buffer.alloc(0);
            break;
          }

          let frameStr = rawBuffer.subarray(stxIdx + 1, etxIdx).toString('ascii');
          frameStr = frameStr.replace(/^\d+/, '');
          rawBuffer = rawBuffer.subarray(etxIdx + 4);

          const records = frameStr.split(/\r\n|\n|\r/).filter(r => r.trim().length > 0);

          for (const rec of records) {
            const parts = rec.split('|');
            const recordType = parts[0];

            if (recordType === 'O') {
              const barcode = parts[2]?.split('^')[0];
              if (barcode) {
                currentBarcode = barcode;
                log('BARCODE', barcode);
              }
            } else if (recordType === 'R') {
              const testField = parts[2] || '';
              const codeParts = testField.split('^');
              const code = codeParts[codeParts.length - 1] || '';
              const value = parts[3] || '';

              if (code && value && currentBarcode) {
                const orderId = await getOrderIdByBarcode(currentBarcode);
                if (orderId) {
                  stageResult(currentBarcode, { code, name: code, value }, orderId);
                }
              }
            } else if (recordType === 'L') {
              if (currentBarcode) {
                await flushSample(currentBarcode);
                currentBarcode = null;
              }
            }
          }
        }
      });

      socket.on('close', () => {
        clients--;
        emitLog({ statusUpdate: { type: 'tcp', text: clients > 0 ? 'Connected' : 'Listening', ok: clients > 0 } });
        log('UNLINK', 'Arrows KT-44 disconnected');
      });

      socket.on('error', (err) => {
        log('ERR', `Socket error: ${err.message}`);
      });
    });

    // Start listening and resolve the promise
    server.listen(port, ip, () => {
      log('INIT', `Listening on TCP Port ${port}`);
      emitLog({ statusUpdate: { type: 'tcp', text: 'Listening', ok: true } });
      resolve(server);
    });

    server.on('error', (err) => {
      log('ERR', `Server Error: ${err.message}`);
      emitLog({ statusUpdate: { type: 'tcp', text: 'Error', ok: false } });
      reject(err);
    });
  });
}