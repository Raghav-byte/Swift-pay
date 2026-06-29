const fs = require('fs');
const readline = require('readline');

async function summarize(file) {
  const durations = [];
  const waiting = [];
  let httpReqs = 0;
  let httpFailed = 0;
  let checksPass = 0;
  let dropped = 0;
  let maxVus = 0;
  let start = null;
  let end = null;

  const rl = readline.createInterface({ input: fs.createReadStream(file), crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line.trim()) continue;
    let obj;
    try {
      obj = JSON.parse(line);
    } catch {
      continue;
    }
    const t = obj.data?.time;
    if (t) {
      if (!start || t < start) start = t;
      if (!end || t > end) end = t;
    }
    if (obj.type !== 'Point') continue;
    const m = obj.metric;
    const v = obj.data?.value;
    if (m === 'http_req_duration') durations.push(v);
    else if (m === 'http_req_waiting') waiting.push(v);
    else if (m === 'http_reqs') httpReqs += v;
    else if (m === 'http_req_failed') httpFailed += v;
    else if (m === 'checks' && v === 1) checksPass += v;
    else if (m === 'dropped_iterations') dropped += v;
    else if (m === 'vus') maxVus = Math.max(maxVus, v);
  }

  const pct = (arr, p) => {
    if (!arr.length) return 0;
    const s = [...arr].sort((a, b) => a - b);
    const idx = Math.ceil((p / 100) * s.length) - 1;
    return s[Math.max(0, idx)];
  };
  const avg = (arr) => (arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0);

  const durationSec = start && end ? (new Date(end) - new Date(start)) / 1000 : null;
  return {
    file,
    durationSec: durationSec ? durationSec.toFixed(1) : null,
    httpReqs,
    actualRps: durationSec ? (httpReqs / durationSec).toFixed(2) : null,
    httpFailedPct: httpReqs ? ((httpFailed / httpReqs) * 100).toFixed(4) : '0',
    droppedIterations: dropped,
    maxVus,
    checksPass,
    http_req_duration_ms: {
      avg: avg(durations).toFixed(2),
      med: pct(durations, 50).toFixed(2),
      p90: pct(durations, 90).toFixed(2),
      p95: pct(durations, 95).toFixed(2),
      p99: pct(durations, 99).toFixed(2),
      max: durations.length ? Math.max(...durations).toFixed(2) : '0',
    },
    http_req_waiting_ms: {
      avg: avg(waiting).toFixed(2),
      p95: pct(waiting, 95).toFixed(2),
    },
  };
}

(async () => {
  for (const f of process.argv.slice(2)) {
    console.log(JSON.stringify(await summarize(f), null, 2));
  }
})();
