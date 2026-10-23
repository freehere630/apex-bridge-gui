import WebSocket from 'ws';
import net from 'net';
import { createClient } from '@supabase/supabase-js';

// MLLP Framing Bytes
const VT = '\x0B';
const FS = '\x1C';
const CR = '\x0D';
const LF = '\x0A';

export function startMaglumiBridge(ip, port, emitLog) {
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
    
    const orderCache = new Map();
    const machineCodeToParamIds = new Map();
    const paramIdToMachineCode = new Map();
    
    let clients = 0;

    // Helper for modern UI logging
    const log = (tag, message) => emitLog({ tag, message });

    // 10-Minute In-Memory Cache
    const cacheOrder = (barcode, orderData) => {
      orderCache.set(String(barcode).trim(), { data: orderData, expiry: Date.now() + (10 * 60 * 1000) });
    };

    const getCachedOrder = (barcode) => {
      const cached = orderCache.get(String(barcode).trim());
      if (cached && Date.now() < cached.expiry) return cached.data;
      orderCache.delete(String(barcode).trim());
      return null;
    };

    const getLocalHl7Timestamp = () => {
      const d = new Date();
      const pad = (n) => String(n).padStart(2, '0');
      return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
    };

    // ============================================================
    // DYNAMIC MAPPINGS
    // ============================================================
    async function loadMappings() {
      try {
        const { data, error } = await supabase
          .from('analyzer_mappings')
          .select('machine_code, lims_parameter_id')
          .eq('analyzer_name', 'Maglumi X3');
          
        if (error) throw error;

        machineCodeToParamIds.clear();
        paramIdToMachineCode.clear();
        
        data?.forEach(m => {
          const code = m.machine_code.toUpperCase();
          if (!machineCodeToParamIds.has(code)) machineCodeToParamIds.set(code, []);
          machineCodeToParamIds.get(code).push(m.lims_parameter_id);
          paramIdToMachineCode.set(m.lims_parameter_id, m.machine_code);
        });
        
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
        
        // Reload mappings every 5 minutes
        setInterval(loadMappings, 5 * 60 * 1000);
        
        // Heartbeat every 60 seconds
        setInterval(async () => {
          await supabase.from('bridge_heartbeats').upsert({ 
            analyzer_name: 'Maglumi X3', 
            last_ping: new Date().toISOString() 
          });
        }, 60000);
        
      } catch (e) {
        emitLog({ statusUpdate: { type: 'db', text: 'Offline', ok: false } });
        log('ERR', `Database Connection Failed: ${e.message}`);
      }
    })();

    // ============================================================
    // HL7 HELPERS
    // ============================================================
    function wrapMllp(hl7Text) {
      return `${VT}${hl7Text}${FS}${CR}`;
    }

    function makeHl7Ack(mshSegment, ackCode = 'AA', textMessage = 'Message Accepted') {
      const fields = (mshSegment || '').split('|');
      const sendingApp = fields[4] || 'Lis';
      const receivingApp = fields[2] || 'Maglumi X3';
      const messageControlId = fields[9] || '1';
      const localNow = getLocalHl7Timestamp();

      const msh = `MSH|^~\\&|${sendingApp}||${receivingApp}||${localNow}||ACK|${Date.now()}|P|2.5|||NE|NE||UTF-8`;
      const msa = `MSA|${ackCode}|${messageControlId}|${textMessage}`;

      return `${msh}${CR}${msa}${CR}`;
    }

    function cleanBarcodeString(raw) {
      if (!raw) return '';
      const firstToken = String(raw).split('^')[0].trim();
      const match = firstToken.match(/^(202\d{6,8}|\d{6,14}|[A-Za-z0-9_-]+)/);
      return match ? match[1] : firstToken;
    }

    function extractTubeBarcode(hl7Segments = []) {
      const spmSeg = hl7Segments.find(s => s.startsWith('SPM'));
      if (spmSeg) {
        const parts = spmSeg.split('|');
        const val = cleanBarcodeString(parts[2]);
        if (val) return val;
      }

      const obrSeg = hl7Segments.find(s => s.startsWith('OBR'));
      if (obrSeg) {
        const parts = obrSeg.split('|');
        const val = cleanBarcodeString(parts[2] || parts[3]);
        if (val && val !== '1' && val !== '0') return val;
      }

      const pidSeg = hl7Segments.find(s => s.startsWith('PID'));
      if (pidSeg) {
        const parts = pidSeg.split('|');
        const val = cleanBarcodeString(parts[3] || parts[2]);
        if (val) return val;
      }

      return '';
    }

    // ============================================================
    // 1. HOST QUERY: EXPANDS PROFILES INTO INDIVIDUAL ASSAYS
    // ============================================================
    async function handleHl7Query(qSegment, mshSegment, socket, allSegments) {
      const localNow = getLocalHl7Timestamp();
      const mshParts = (mshSegment || '').split('|');
      const qpdParts = (qSegment || '').split('|');

      const incomingControlId = mshParts[9] || '1';
      const sendingApp = mshParts[4] || 'Lis';
      const receivingApp = mshParts[2] || 'Maglumi X3';

      const rawBarcodeField = (qpdParts[3] || qpdParts[8] || '').trim();
      const sampleBarcode = cleanBarcodeString(rawBarcodeField);

      log('QUERY', `Worklist request for barcode: ${sampleBarcode}`);

      let order = getCachedOrder(sampleBarcode);
      if (!order) {
        let { data: dbOrder } = await supabase
          .from('orders')
          .select(`*, patient:patients(*), order_tests(*, test:tests(*, test_parameters(*)))`)
          .eq('barcode', sampleBarcode)
          .maybeSingle();

        if (!dbOrder) {
          const { data: fallbackOrder } = await supabase
            .from('orders')
            .select(`*, patient:patients(*), order_tests(*, test:tests(*, test_parameters(*)))`)
            .or(`barcode.ilike.%${sampleBarcode}%,patient_id.eq.${sampleBarcode},id.eq.${sampleBarcode}`)
            .order('created_at', { ascending: false })
            .limit(1)
            .maybeSingle();
          dbOrder = fallbackOrder;
        }

        order = dbOrder;
        if (order) cacheOrder(sampleBarcode, order);
      }

      if (!order) {
        log('NO-ORDER', `Barcode ${sampleBarcode} not found in DB`);
        socket.write(wrapMllp(makeHl7Ack(mshSegment, 'AE', `Barcode ${sampleBarcode} Not Found`)));
        return;
      }

      const assayList = [];

      (order.order_tests || []).forEach(ot => {
        const test = ot.test || {};
        const params = test.test_parameters || test.parameters || [];

        if (params.length > 0) {
          params.forEach(p => {
            const machineCode = paramIdToMachineCode.get(p.id);
            if (machineCode && !assayList.includes(machineCode)) {
              assayList.push(machineCode);
            } else if (!machineCode && p.name) {
              log('WARN', `Param ${p.name} has no machine mapping!`);
              if (!assayList.includes(p.name)) assayList.push(p.name);
            }
          });
        } else if (test.id) {
          const machineCode = paramIdToMachineCode.get(test.id);
          if (machineCode && !assayList.includes(machineCode)) {
             assayList.push(machineCode);
          } else if (test.code || test.name) {
             const fallbackCode = test.code || test.name;
             if (!assayList.includes(fallbackCode)) assayList.push(fallbackCode);
          }
        }
      });

      if (assayList.length === 0) {
        log('NO-TESTS', `No assays prescribed for ${sampleBarcode}`);
        socket.write(wrapMllp(makeHl7Ack(mshSegment, 'AA', 'No Tests Prescribed')));
        return;
      }

      const patient = order.patient || {};
      const gender = (patient.gender || '').toUpperCase().startsWith('F') ? 'Female' : 'Male';
      const patientName = (patient.name || 'Patient').replace(/[|^\\]/g, '');
      const patientId = String(patient.id || sampleBarcode);

      const respMsh = `MSH|^~\\&|${sendingApp}||${receivingApp}||${localNow}||OML^O33|${incomingControlId}|P|2.5|||NE|NE||UTF-8`;
      const respPid = `PID|1||${patientId}||${patientName}|||${gender}||||||||||||||||||||||||||||||^Years Old`;
      const respSpm = `SPM|1|${sampleBarcode}^^|||||||||P`;
      const respOrc = `ORC|NW||||||N`;

      const obrSegments = assayList.map((testCode, idx) => `OBR|${idx + 1}|||${testCode}^`).join(CR);
      const fullResponse = `${respMsh}${CR}${respPid}${CR}${respSpm}${CR}${respOrc}${CR}${obrSegments}${CR}`;
      
      socket.write(wrapMllp(fullResponse));
      log('DISPATCH', `Sent ${assayList.length} assays to machine for ${sampleBarcode}`);
    }

    // ============================================================
    // 2. RECEIVE RESULTS: SMART MAPPING BACK TO PROFILE PARAMETERS
    // ============================================================
    async function handleHl7Results(hl7Segments, mshSegment, socket) {
      socket.write(wrapMllp(makeHl7Ack(mshSegment, 'AA', 'Results Accepted')));

      const sampleBarcode = extractTubeBarcode(hl7Segments) || 'UNKNOWN';
      const parsedResults = [];

      for (const seg of hl7Segments) {
        const parts = seg.split('|');
        if (parts[0]?.trim() === 'OBX') {
          const testIdentifier = parts[3] || '';
          const subParts = testIdentifier.split('^').filter(Boolean);
          const testCode = subParts[0] || testIdentifier.trim();
          const testName = subParts[1] || testCode;
          const resultValue = (parts[5] || '').trim();
          const unit = (parts[6] || '').trim();

          if (testCode && resultValue !== '') {
            parsedResults.push({ testCode, testName, resultValue, unit });
          }
        }
      }

      for (const res of parsedResults) {
        try {
          let { data: order } = await supabase
            .from('orders')
            .select(`id, qc_status, is_locked, order_tests(*, test:tests(*, test_parameters(*)))`)
            .eq('barcode', sampleBarcode)
            .maybeSingle();

          if (!order) {
            const { data: patientOrder } = await supabase
              .from('orders')
              .select(`id, qc_status, is_locked, order_tests(*, test:tests(*, test_parameters(*)))`)
              .eq('patient_id', sampleBarcode)
              .order('created_at', { ascending: false })
              .limit(1)
              .maybeSingle();
            order = patientOrder;
          }

          if (!order) {
            log('NOT-SAVED', `Order ${sampleBarcode} not in Supabase`);
            continue;
          }

          if (order.is_locked === true || order.qc_status === 'Verified') {
            log('LOCKED', `Order ${order.id} is verified. Data discarded.`);
            continue;
          }

          let targetParamId = null;
          const possibleParamIds = machineCodeToParamIds.get(res.testCode.toUpperCase()) || [];
          
          if (possibleParamIds.length > 0) {
            for (const ot of (order.order_tests || [])) {
              const params = ot.test?.test_parameters || ot.test?.parameters || [];
              for (const p of params) {
                if (possibleParamIds.includes(p.id)) {
                  targetParamId = p.id;
                  break;
                }
              }
              if (targetParamId) break;
            }
          }

          // Exact name fallback
          if (!targetParamId) {
            const cleanMachineCode = res.testCode.toUpperCase().replace(/\s+/g, '');
            for (const ot of (order.order_tests || [])) {
              const params = ot.test?.test_parameters || ot.test?.parameters || [];
              for (const p of params) {
                const pName = (p.name || '').toUpperCase().replace(/\s+/g, '');
                if (pName === cleanMachineCode) {
                  targetParamId = p.id;
                  break;
                }
              }
              if (targetParamId) break;
            }
          }

          if (!targetParamId) {
            log('UNMAPPED', `Assay '${res.testCode}' not mapped for this order`);
            continue;
          }

          await supabase.from('results').upsert({
            order_id: order.id,
            parameter_id: targetParamId,
            result_value: String(res.resultValue),
            status_flag: 'AUTOMATED',
            analyzer_source: 'MAGLUMI_X3'
          }, { onConflict: 'order_id,parameter_id' });

          // ADDED BARCODE TO THE LOG LINE HERE
          log('RESULT', `[${sampleBarcode}] ${res.testCode} = ${res.resultValue} ${res.unit} (Saved)`);
          
        } catch (dbErr) {
          log('DB-ERR', `Error saving result: ${dbErr.message}`);
        }
      }
    }

    // ============================================================
    // 3. TCP SERVER
    // ============================================================
    const server = net.createServer((socket) => {
      clients++;
      emitLog({ statusUpdate: { type: 'tcp', text: 'Connected', ok: true } });
      log('LINK', `Maglumi X3 connected`);
      
      socket.setKeepAlive(true, 10000);
      let rawBuffer = '';

      socket.on('data', async (chunk) => {
        const chunkStr = chunk.toString('ascii');
        rawBuffer += chunkStr;

        while (rawBuffer.includes(FS) || rawBuffer.includes(CR)) {
          let hl7Message = '';
          if (rawBuffer.includes(FS)) {
            const startIndex = rawBuffer.indexOf(VT);
            const endIndex = rawBuffer.indexOf(FS);
            if (startIndex !== -1 && startIndex < endIndex) {
              hl7Message = rawBuffer.substring(startIndex + 1, endIndex);
            } else {
              hl7Message = rawBuffer.substring(0, endIndex);
            }
            rawBuffer = rawBuffer.substring(endIndex + 1);
            if (rawBuffer.startsWith(CR)) rawBuffer = rawBuffer.substring(1);
            if (rawBuffer.startsWith(LF)) rawBuffer = rawBuffer.substring(1);
          } else {
            const lineEnd = rawBuffer.indexOf(CR);
            if (lineEnd === -1) break;
            hl7Message = rawBuffer.substring(0, lineEnd);
            rawBuffer = rawBuffer.substring(lineEnd + 1);
          }

          const segments = hl7Message.split(/[\r\n]+/).map(s => s.trim()).filter(Boolean);
          if (segments.length === 0) continue;

          const mshSegment = segments.find(s => s.startsWith('MSH')) || segments[0];
          const mshParts = mshSegment.split('|');
          const messageType = mshParts[8] || '';

          try {
            if (messageType.includes('TSREQ') || messageType.includes('QRY') || segments.some(s => s.startsWith('QPD'))) {
              const qSeg = segments.find(s => s.startsWith('QPD')) || '';
              await handleHl7Query(qSeg, mshSegment, socket, segments);
            } else if (messageType.includes('OUL') || messageType.includes('ORU') || segments.some(s => s.startsWith('OBX'))) {
              await handleHl7Results(segments, mshSegment, socket);
            } else {
              socket.write(wrapMllp(makeHl7Ack(mshSegment, 'AA', 'Heartbeat OK')));
            }
          } catch (handlerErr) {
            log('ERR', handlerErr.message);
          }
        }
      });

      socket.on('close', () => {
        clients--;
        emitLog({ statusUpdate: { type: 'tcp', text: clients > 0 ? 'Connected' : 'Listening', ok: clients > 0 } });
        log('UNLINK', 'Maglumi X3 disconnected');
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