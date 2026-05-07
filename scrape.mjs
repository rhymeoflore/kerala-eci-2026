// One-time scraper: pulls ECI Kerala 2026 results via r.jina.ai with strict
// rate-limit pacing (Jina free tier ≈ 20 crawls/minute) and writes
// data/results.json. Resumable — re-running picks up where it left off.

import fs from "node:fs/promises";

const PARTYWISE_URL = "https://results.eci.gov.in/ResultAcGenMay2026/partywiseresult-S11.htm";
const STATEWISE_PAGES = Array.from({length:7}, (_,i)=>`https://results.eci.gov.in/ResultAcGenMay2026/statewiseS11${i+1}.htm`);
const acDetailURL = (no) => `https://results.eci.gov.in/ResultAcGenMay2026/ConstituencywiseS11${no}.htm`;

const PROXY = u => `https://r.jina.ai/${u}`;
const HEADERS = {
  "Accept": "text/plain, text/html",
  "x-respond-with": "markdown",
  "x-with-images-summary": "false",
  "x-with-links-summary": "false",
};

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const CONCURRENCY = 1; // sequential — jina anon free tier rate-limits hard
async function pool(items, fn){
  const results = new Array(items.length);
  let cursor = 0;
  const workers = Array.from({length: Math.min(CONCURRENCY, items.length)}, async () => {
    while(true){
      const i = cursor++;
      if (i >= items.length) return;
      try { results[i] = await fn(items[i], i); }
      catch(e){ console.warn("  worker err", items[i], e.message); results[i] = null; }
    }
  });
  await Promise.all(workers);
  return results;
}

async function fetchMD(url, attempts=8){
  let lastErr;
  for (let i=0;i<attempts;i++){
    try{
      const ctl = new AbortController();
      const tid = setTimeout(() => ctl.abort(), 60000); // 60s hard cap
      try {
        const r = await fetch(PROXY(url), {headers: HEADERS, signal: ctl.signal});
        const body = await r.text();
        if (r.status === 429 || /RateLimitTriggeredError|Per IP rate limit/i.test(body)){
          let retry = 30;
          try { const j = JSON.parse(body); if (j.retryAfter) retry = +j.retryAfter; } catch {}
          throw new Error(`429 retryAfter=${retry}`);
        }
        if (!r.ok) throw new Error("HTTP "+r.status);
        if (!body || body.length < 500) throw new Error("empty body");
        return body;
      } finally { clearTimeout(tid); }
    }catch(e){
      lastErr = e;
      const m = /retryAfter=(\d+)/.exec(e.message);
      const base = m ? (parseInt(m[1],10)*1000 + 3000) : (4000 * (i+1));
      const wait = base + Math.floor(Math.random()*4000);
      console.log(`  retry ${i+1}/${attempts} (${e.message}, wait ${(wait/1000).toFixed(0)}s)`);
      await sleep(wait);
    }
  }
  throw lastErr || new Error("fetch failed: "+url);
}

function splitPipeRow(line){
  const s = line.trim().replace(/^\|/, "").replace(/\|$/, "");
  return s.split("|").map(c => c.trim());
}
function cleanParty(s){
  if (!s) return "";
  return s
    .replace(/\s*###[\s\S]*$/, "")
    .replace(/\s+i\s*$/i, "")
    .replace(/\s*i$/, "")
    .replace(/\[(.*?)\]\([^)]*\)/g, "$1")
    .replace(/\s*-\s*[A-Z()]+\s*$/, "")
    .trim();
}
function parsePartyWise(md){
  const rows = [];
  md.split("\n").forEach(line => {
    if (!line.includes("|")) return;
    const cells = splitPipeRow(line);
    if (cells.length < 4) return;
    const partyRaw = cleanParty(cells[0]);
    const num = s => { const m = String(s).match(/-?\d+/); return m ? parseInt(m[0],10) : NaN; };
    const won = num(cells[1]); const lead = num(cells[2]); const total = num(cells[3]);
    if (!partyRaw || isNaN(won) || isNaN(lead) || isNaN(total)) return;
    if (/^party$/i.test(partyRaw)) return;
    if (/^total$/i.test(partyRaw)) return;
    const party = partyRaw.replace(/\s*-\s*[A-Z()]+\s*$/, "").trim();
    if (!party) return;
    rows.push({party, won, lead, total});
  });
  const lu = (md.match(/Last Updated at[^\n]+/) || [""])[0].trim();
  return {rows, lastUpdated: lu};
}
function parseStatewise(md){
  const out = [];
  md.split("\n").forEach(line => {
    if (!line.includes("|")) return;
    const cells = splitPipeRow(line);
    if (cells.length < 9) return;
    const [ac, no, leadCand, leadPartyRaw, trailCand, trailPartyRaw, margin, round, status] = cells;
    if (!/^\d+$/.test(no)) return;
    out.push({
      ac: ac.trim(), no: parseInt(no,10),
      leadCand: leadCand.trim(), leadParty: cleanParty(leadPartyRaw),
      trailCand: trailCand.trim(), trailParty: cleanParty(trailPartyRaw),
      margin: parseInt(String(margin).replace(/[^\d-]/g,""),10) || 0,
      round: round.trim(), status: status.trim(),
    });
  });
  return out;
}
function parseCandidates(md){
  const out = [];
  md.split("\n").forEach(line => {
    if (!line.includes("|")) return;
    const cells = splitPipeRow(line);
    if (cells.length < 7) return;
    const sn = cells[0];
    if (!/^\d+$/.test(sn)) return;
    const name = cleanParty(cells[1]);
    const party = cleanParty(cells[2]);
    const evm = parseInt(String(cells[3]).replace(/[^\d]/g,""),10) || 0;
    const postal = parseInt(String(cells[4]).replace(/[^\d]/g,""),10) || 0;
    const total = parseInt(String(cells[5]).replace(/[^\d]/g,""),10) || 0;
    const pct = parseFloat(String(cells[6]).replace(/[^\d.]/g,"")) || 0;
    if (!name) return;
    out.push({sn:parseInt(sn,10), name, party, evm, postal, total, pct});
  });
  return out;
}

async function checkpoint(state){
  await fs.writeFile("data/results.json", JSON.stringify(state));
}

async function main(){
  await fs.mkdir("data", {recursive:true});
  let prev = {};
  try { prev = JSON.parse(await fs.readFile("data/results.json","utf8")); } catch {}

  let partywise = prev.partywise;
  if (!partywise || !partywise.rows || !partywise.rows.length){
    console.log("[1/3] Partywise…");
    const pwMd = await fetchMD(PARTYWISE_URL);
    partywise = parsePartyWise(pwMd);
    console.log(`     rows=${partywise.rows.length} · ${partywise.lastUpdated}`);
  } else {
    console.log("[1/3] Partywise (cached)");
  }

  let acs = prev.acs;
  if (!acs || !acs.length || acs.length < 140){
    console.log("[2/3] Statewise (7 pages)…");
    const seen = new Map();
    (acs || []).forEach(r => seen.set(r.no, r));
    for (let i=0;i<STATEWISE_PAGES.length;i++){
      const md = await fetchMD(STATEWISE_PAGES[i]);
      parseStatewise(md).forEach(r => seen.set(r.no, r));
      console.log(`     page ${i+1}/7 · cumulative ACs=${seen.size}`);
    }
    acs = [...seen.values()].sort((a,b)=>a.no-b.no);
    if (acs.length < 140) console.warn(`     ⚠ expected 140 ACs, got ${acs.length}`);
  } else {
    console.log(`[2/3] Statewise (cached, ${acs.length} ACs)`);
  }

  const candidatesByAC = prev.candidatesByAC || {};
  const todo = acs.filter(a => !(candidatesByAC[a.no] && candidatesByAC[a.no].length));
  console.log(`[3/3] Candidate detail · ${todo.length} pending of ${acs.length} (concurrency=${CONCURRENCY})`);
  let done = acs.length - todo.length;
  let sinceCheckpoint = 0;
  await pool(todo, async (ac) => {
    const md = await fetchMD(acDetailURL(ac.no));
    candidatesByAC[ac.no] = parseCandidates(md);
    done++;
    sinceCheckpoint++;
    console.log(`     ${done}/${acs.length} · AC ${ac.no} ${ac.ac} (${candidatesByAC[ac.no].length} cands)`);
    if (sinceCheckpoint >= 8){
      sinceCheckpoint = 0;
      await checkpoint({generatedAt:new Date().toISOString(), partywise, acs, candidatesByAC});
    }
  });

  const out = {generatedAt:new Date().toISOString(), partywise, acs, candidatesByAC};
  await checkpoint(out);
  const sz = (JSON.stringify(out).length/1024).toFixed(1);
  console.log(`✔ Wrote data/results.json (${sz} KB)`);
}
main().catch(e => { console.error("FATAL", e); process.exit(1); });
