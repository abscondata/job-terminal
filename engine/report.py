from __future__ import annotations

import html as htmlmod
import json
from datetime import datetime
from pathlib import Path


def _e(value):
    return htmlmod.escape(str(value) if value is not None else "")


def write_report(run_dir, run_id, rows, meta=None):
    Path(run_dir).mkdir(parents=True, exist_ok=True)
    out = Path(run_dir) / "report.html"
    payload = (
        TPL.replace("__RID__", _e(run_id[:8]))
        .replace("__NOW__", _e(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        .replace("__META__", json.dumps(meta or {}, ensure_ascii=False))
        .replace("__DATA__", json.dumps(rows, ensure_ascii=False))
    )
    out.write_text(payload, encoding="utf-8")
    return str(out)


TPL = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>JobEngine Direction Review</title>
<style>
:root{--bg:#f1ecdf;--panel:#fffaf0;--ink:#191611;--muted:#6b6256;--line:#dbcdb5;--accent:#8d3a12;--olive:#365314;--blue:#1d4ed8;--teal:#0f766e;--warn:#9a6700;--bad:#991b1b}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at top left,#f7f1e7 0,#eee5d5 48%,#e3d8c7 100%);color:var(--ink);font:14px/1.45 Georgia,'Times New Roman',serif}a{color:var(--accent);text-decoration:none}
.top{position:sticky;top:0;z-index:5;background:rgba(255,250,240,.96);backdrop-filter:blur(12px);border-bottom:1px solid var(--line);padding:14px 18px}
.brandRow{max-width:1540px;margin:0 auto;display:flex;gap:12px;align-items:baseline;flex-wrap:wrap}.brand{font-weight:700;letter-spacing:.04em}.rid{font:12px Consolas,monospace;color:var(--muted)}.tabs{display:flex;gap:8px;margin-left:10px}.tab{padding:6px 10px;border:1px solid var(--line);border-radius:999px;color:var(--muted);cursor:pointer}.tab.on{background:#efdeca;color:var(--ink);border-color:#d2b48c}
.stats{max-width:1540px;margin:8px auto 0;color:var(--muted);font:12px Consolas,monospace}
.page{display:none}.page.on{display:block}
.grid{max-width:1540px;margin:0 auto;padding:14px 18px;display:grid;grid-template-columns:310px 1fr 390px;gap:12px;height:calc(100vh - 92px)}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:18px;box-shadow:0 12px 34px rgba(91,64,37,.06);overflow:hidden}
.side,.detail{padding:14px;overflow:auto}.main{overflow:auto}
.label{font:700 11px/1.2 'Segoe UI',sans-serif;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);margin:10px 0 6px}.label:first-child{margin-top:0}
.text,.sel,.area{width:100%;padding:10px 11px;border:1px solid var(--line);border-radius:12px;background:#fff;color:var(--ink);font:inherit}.area{min-height:180px;resize:vertical}
.small{font-size:12px;color:var(--muted)}
.actions,.saveRow{display:flex;gap:6px;flex-wrap:wrap}.btn{padding:7px 10px;border-radius:12px;border:1px solid var(--line);background:#fff;color:var(--ink);cursor:pointer}.btn.primary{background:#f7e3cd;border-color:#d9b48b}.btn.good{background:#e0f3e7;border-color:#9ed0ae}.btn.warn{background:#fcf0d2;border-color:#e7ca77}.btn.bad{background:#fde7e7;border-color:#e5a2a2}
.note{padding:10px 12px;border:1px solid var(--line);border-radius:14px;background:#fff;color:#443d32}
table{width:100%;border-collapse:collapse;min-width:1220px}th,td{padding:8px 6px;border-bottom:1px solid #efe4d5;vertical-align:top}th{position:sticky;top:0;background:#fff6eb;color:var(--muted);font:700 11px sans-serif;text-transform:uppercase;letter-spacing:.08em;text-align:left}tr:hover td{background:#fff4e7}tr.sel td{background:#fcefdc}
.tag{display:inline-block;padding:2px 8px;border-radius:999px;font:600 11px/1.6 'Segoe UI',sans-serif}.lane{background:#ede9fe;color:#5b21b6}.apply{background:#dcfce7;color:#166534}.maybe{background:#fef3c7;color:#92400e}.skip{background:#fee2e2;color:#991b1b}.world{background:#dbeafe;color:#1d4ed8}.city{background:#cffafe;color:#155e75}.neutral{background:#ece7df;color:#5f584d}.risk{background:#fff7ed;color:#9a3412}
.box{padding:10px;border:1px solid var(--line);border-radius:14px;background:#fff;white-space:pre-wrap}
.metric{display:grid;grid-template-columns:1fr auto;gap:8px;padding:6px 0;border-bottom:1px solid #efe4d5}.metric:last-child{border-bottom:0}
.triage{max-width:1180px;margin:0 auto;padding:18px;display:grid;grid-template-columns:1fr 1fr;gap:14px}.triage .panel{padding:14px}
.savedWrap{max-width:1540px;margin:0 auto;padding:14px 18px;display:grid;grid-template-columns:1fr 360px;gap:12px}
.savedDetail{padding:14px}
</style></head><body>
<div class="top"><div class="brandRow"><span class="brand">JobEngine Direction Review</span><span class="rid">__RID__</span><span class="rid">__NOW__</span><div class="tabs"><span class="tab on" data-page="discover">Discovery</span><span class="tab" data-page="triage">Manual Triage</span><span class="tab" data-page="saved">Saved Review</span></div></div><div class="stats" id="stats"></div></div>

<div class="page on" id="page-discover">
<div class="grid">
<div class="panel side">
<div class="label">Search</div><input id="q" class="text" placeholder="Company, lane, city, world..." />
<div class="label">Decision</div><select id="fDecision" class="sel"></select>
<div class="label">City Lane</div><select id="fCity" class="sel"></select>
<div class="label">Path Lane</div><select id="fLane" class="sel"></select>
<div class="label">World</div><select id="fWorld" class="sel"></select>
<div class="label">Function</div><select id="fFunction" class="sel"></select>
<div class="label">Work Type</div><select id="fWorkType" class="sel"></select>
<div class="label">French</div><select id="fFrench" class="sel"></select>
<div class="label">Bridge Or Slop</div><select id="fSlop" class="sel"></select>
<div class="label">Read</div><div class="note" id="activePath"></div>
<div class="label">Saved Review</div><div class="small">Manual review buckets stay simple: `Dream Bridge`, `Strong Bridge`, `Practical Paris Entry`, or `Skip`. Discovery itself stays lane-based and explanation-first.</div>
</div>
<div class="panel main">
<table><thead><tr><th>City</th><th>Primary Lane</th><th>Decision</th><th>World</th><th>Function</th><th>Saved</th><th>Company</th><th>Title</th><th>Work Type</th><th>French</th><th>Bridge Or Slop</th><th>Source</th></tr></thead><tbody id="tb"></tbody></table>
</div>
<div class="panel detail">
<div class="label">Selected Role</div><div id="detailTitle" style="font-weight:700;font-size:18px;margin-bottom:10px">Select a role.</div>
<div id="detailSummary" class="note">This panel explains the path logic, not just whether the role matched keywords.</div>
<div class="label">Quick Read</div><div id="detailMetrics" class="box"></div>
<div class="label">Why It Surfaced</div><div id="detailSurfaced" class="box"></div>
<div class="label">Why It Could Matter</div><div id="detailMatter" class="box"></div>
<div class="label">Next Path Logic</div><div id="detailPath" class="box"></div>
<div class="label">Main Risk</div><div id="detailRisk" class="box"></div>
<div class="label">Lane Tags</div><div id="detailLanes" class="box"></div>
<div class="label">Save To Review</div><div class="saveRow"><button class="btn good" onclick="saveSelected('Dream Bridge')">Dream Bridge</button><button class="btn good" onclick="saveSelected('Strong Bridge')">Strong Bridge</button><button class="btn warn" onclick="saveSelected('Practical Paris Entry')">Practical Paris Entry</button><button class="btn bad" onclick="saveSelected('Skip')">Skip</button></div>
<div class="actions" style="margin-top:10px"><button class="btn primary" id="openBtn">Open</button><button class="btn" id="copyBtn">Copy URL</button></div>
</div>
</div>
</div>

<div class="page" id="page-triage">
<div class="triage">
<div class="panel">
<div class="label">Paste A Role</div>
<input id="tjUrl" class="text" placeholder="Paste a LinkedIn, JobTeaser, FashionJobs, or company URL (optional)" />
<div style="height:8px"></div>
<input id="tjTitle" class="text" placeholder="Job title (optional if URL resolves)" />
<div style="height:8px"></div>
<input id="tjCompany" class="text" placeholder="Company (optional)" />
<div style="height:8px"></div>
<input id="tjLocation" class="text" placeholder="Location (optional)" />
<div style="height:8px"></div>
<textarea id="tjDesc" class="area" placeholder="Paste the job text here. This is still the best input, especially for blocked sources like JobTeaser and FashionJobs."></textarea>
<div class="actions"><button class="btn primary" id="triageBtn">Evaluate</button></div>
<div class="small" id="triageStatus">Best input: pasted job text plus title/company/location if available.</div>
</div>
<div class="panel">
<div class="label">Triage Result</div>
<div id="triageResult" class="note">No role evaluated yet.</div>
<div class="label">Save To Review</div><div class="saveRow"><button class="btn good" onclick="saveTriage('Dream Bridge')">Dream Bridge</button><button class="btn good" onclick="saveTriage('Strong Bridge')">Strong Bridge</button><button class="btn warn" onclick="saveTriage('Practical Paris Entry')">Practical Paris Entry</button><button class="btn bad" onclick="saveTriage('Skip')">Skip</button></div>
</div>
</div>
</div>

<div class="page" id="page-saved">
<div class="savedWrap">
<div class="panel main">
<table><thead><tr><th>Review Bucket</th><th>City</th><th>Primary Lane</th><th>Decision</th><th>World</th><th>Company</th><th>Title</th></tr></thead><tbody id="savedTb"></tbody></table>
</div>
<div class="panel savedDetail">
<div class="label">Saved Role</div><div id="savedTitle" style="font-weight:700;font-size:18px;margin-bottom:10px">Select a saved role.</div>
<div id="savedSummary" class="note">Saved review is where you compare the actual leap candidates.</div>
<div class="label">Why It Surfaced</div><div id="savedSurfaced" class="box"></div>
<div class="label">Why It Could Matter</div><div id="savedMatter" class="box"></div>
<div class="label">Next Path Logic</div><div id="savedPath" class="box"></div>
<div class="label">Main Risk</div><div id="savedRisk" class="box"></div>
<div class="label">Rebucket</div><div class="saveRow"><button class="btn good" onclick="rebucketSaved('Dream Bridge')">Dream Bridge</button><button class="btn good" onclick="rebucketSaved('Strong Bridge')">Strong Bridge</button><button class="btn warn" onclick="rebucketSaved('Practical Paris Entry')">Practical Paris Entry</button><button class="btn bad" onclick="rebucketSaved('Skip')">Skip</button></div>
<div class="actions" style="margin-top:10px"><button class="btn primary" onclick="openSaved()">Open</button><button class="btn bad" onclick="removeSaved()">Remove</button></div>
</div>
</div>
</div>

<script>
var META=__META__,DATA=__DATA__,SRV="http://127.0.0.1:8765",sel=null,SAVED=[],SAVED_INDEX={},savedSel=null,TRIAGE=null;
var DECISION_ORDER={"apply":0,"maybe":1,"skip":2},CITY_ORDER={"Paris":0,"Paris Region":1,"NYC":2,"Miami":3,"France Outside Paris":4,"US Other":5,"Remote":6,"Off-Lane":7,"Unknown":8},REVIEW_ORDER={"Dream Bridge":0,"Strong Bridge":1,"Practical Paris Entry":2,"Skip":3};
function esc(s){return String(s==null?"":s).replace(/[&<>\"']/g,m=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[m]||m))}
function ord(map,key,defv){return Object.prototype.hasOwnProperty.call(map,key)?map[key]:defv}
function tagClass(kind,val){if(kind==="decision"){if(val==="apply")return"apply";if(val==="maybe")return"maybe";return"skip"}if(kind==="world")return"world";if(kind==="city")return"city";if(kind==="risk")return"risk";if(kind==="review"){if(val==="Dream Bridge"||val==="Strong Bridge")return"apply";if(val==="Practical Paris Entry")return"maybe";return"skip"}return"lane"}
function savedBucket(role){var hit=SAVED_INDEX[role.fingerprint||""];return hit?hit.review_bucket:""}
function statLine(){var cc=META.classification_counts||{},rc=META.recommendation_counts||{},lc=META.lane_counts||{},gc=META.city_lane_counts||{};document.getElementById("stats").innerHTML="pulled <b>"+(META.pulled_total||0)+"</b> · new <b>"+(META.new_count||0)+"</b> · backfill <b>"+(META.backfill_count||0)+"</b> · Paris lane <b>"+(gc[\"Paris\"]||0)+"</b> · NYC lane <b>"+(gc[\"NYC\"]||0)+"</b> · Apply <b>"+(rc[\"apply\"]||0)+"</b> · Maybe <b>"+(rc[\"maybe\"]||0)+"</b> · Skip <b>"+(rc[\"skip\"]||0)+"</b> · Paris Direction <b>"+(cc[\"Paris Direction\"]||0)+"</b> · Money / Platform <b>"+(lc[\"Money / Platform Leap\"]||0)+"</b>"} 
function activePath(){document.getElementById("activePath").textContent=META.active_path_note||"Active path: discovery, manual triage, and saved review."}
function uniqueValues(getter){var seen={},out=[];for(var i=0;i<DATA.length;i++){var v=getter(DATA[i]);if(!v)continue;if(Array.isArray(v)){for(var j=0;j<v.length;j++){var item=v[j];if(item&&!seen[item]){seen[item]=1;out.push(item)}}}else if(!seen[v]){seen[v]=1;out.push(v)}}return out.sort()}
function fillSelect(id,values,label){var el=document.getElementById(id),html='<option value="all">All '+label+'</option>';for(var i=0;i<values.length;i++)html+='<option value="'+esc(values[i])+'">'+esc(values[i])+'</option>';el.innerHTML=html}
function setupFilters(){fillSelect("fDecision",["apply","maybe","skip"],"Decisions");fillSelect("fCity",uniqueValues(r=>r.city_lane),"Cities");fillSelect("fLane",uniqueValues(r=>r.opportunity_lanes||[]),"Lanes");fillSelect("fWorld",uniqueValues(r=>r.world_tier),"Worlds");fillSelect("fFunction",uniqueValues(r=>r.function_family),"Functions");fillSelect("fWorkType",uniqueValues(r=>r.work_type_label),"Work Types");fillSelect("fFrench",uniqueValues(r=>r.french_risk_label),"French Burdens");fillSelect("fSlop",uniqueValues(r=>r.slop_verdict),"Bridge/Slop Labels")}
function defaultWorkType(){var counts=META.work_type_counts||{},total=0,intern=0;for(var k in counts){var v=counts[k]||0;total+=v;if(k==="Internship"||k==="Traineeship / Apprenticeship")intern+=v}if(!total)return;var heavy=intern/total>=0.5;var sel=document.getElementById("fWorkType");if(heavy&&sel){for(var i=0;i<sel.options.length;i++){if(sel.options[i].value==="Full-time"){sel.value="Full-time";break}}}}
function matchesFilter(role,id,getter){var want=document.getElementById(id).value;if(want==="all")return true;var value=getter(role);if(Array.isArray(value))return value.indexOf(want)>=0;return String(value||"")===want}
function visible(role){var q=(document.getElementById("q").value||"").toLowerCase().trim();if(q){var hay=((role.company||"")+" "+(role.title||"")+" "+(role.classification||"")+" "+(role.city_lane||"")+" "+(role.world_tier||"")+" "+(role.function_family||"")+" "+((role.opportunity_lanes||[]).join(" "))+" "+(savedBucket(role)||"")).toLowerCase();if(hay.indexOf(q)<0)return false}return matchesFilter(role,"fDecision",r=>r.recommendation)&&matchesFilter(role,"fCity",r=>r.city_lane)&&matchesFilter(role,"fLane",r=>r.opportunity_lanes||[])&&matchesFilter(role,"fWorld",r=>r.world_tier)&&matchesFilter(role,"fFunction",r=>r.function_family)&&matchesFilter(role,"fWorkType",r=>r.work_type_label)&&matchesFilter(role,"fFrench",r=>r.french_risk_label)&&matchesFilter(role,"fSlop",r=>r.slop_verdict)}
function sortRows(rows){rows.sort(function(a,b){var d=ord(DECISION_ORDER,a.recommendation||"skip",9)-ord(DECISION_ORDER,b.recommendation||"skip",9);if(d!==0)return d;var c=ord(CITY_ORDER,a.city_lane||"Unknown",9)-ord(CITY_ORDER,b.city_lane||"Unknown",9);if(c!==0)return c;return String(a.company||"").localeCompare(String(b.company||""))})}
function render(){var rows=[];for(var i=0;i<DATA.length;i++){if(visible(DATA[i]))rows.push(DATA[i])}sortRows(rows);if(rows.length&&(!sel||!rows.find(function(r){return r.job_id===sel})))sel=rows[0].job_id;var html="";for(var i=0;i<rows.length;i++){var r=rows[i],saved=savedBucket(r),savedHtml=saved?'<span class="tag '+tagClass("review",saved)+'">'+esc(saved)+'</span>':'<span class="tag neutral">Not saved</span>',selCls=r.job_id===sel?' class="sel"':'',sourceTag=esc(r.source||"")+(r.backfill?' <span class=\"tag neutral\">Backfill</span>':'');html+='<tr'+selCls+' onclick="pick(\\''+esc(r.job_id)+'\\')"><td><span class="tag city">'+esc(r.city_lane||"")+'</span></td><td><span class="tag lane">'+esc(r.classification||"")+'</span></td><td><span class="tag '+tagClass("decision",r.recommendation)+'">'+esc((r.recommendation||"").toUpperCase())+'</span></td><td><span class="tag world">'+esc(r.world_tier||"")+'</span></td><td>'+esc(r.function_family||"")+'</td><td>'+savedHtml+'</td><td>'+esc(r.company||"")+'</td><td>'+esc(r.title||"")+'</td><td>'+esc(r.work_type_label||"")+'</td><td><span class="tag '+tagClass("risk",r.french_risk_label)+'">'+esc(r.french_risk_label||"")+'</span></td><td><span class="tag '+tagClass("risk",r.slop_verdict)+'">'+esc(r.slop_verdict||"")+'</span></td><td>'+sourceTag+'</td></tr>'}document.getElementById("tb").innerHTML=html||'<tr><td colspan="12" class="small">No roles match the current filters.</td></tr>';renderDetail()}
function selectedRole(){for(var i=0;i<DATA.length;i++)if(DATA[i].job_id===sel)return DATA[i];return null}
function metricRows(role){return ['<div class="metric"><span>Bucket</span><strong>'+esc(role.classification||"")+'</strong></div>','<div class="metric"><span>Decision</span><strong>'+esc((role.recommendation||"").toUpperCase())+'</strong></div>','<div class="metric"><span>City lane</span><strong>'+esc(role.city_lane||"")+'</strong></div>','<div class="metric"><span>World</span><strong>'+esc(role.world_tier||"")+'</strong></div>','<div class="metric"><span>Function</span><strong>'+esc(role.function_family||"")+'</strong></div>','<div class="metric"><span>Work type</span><strong>'+esc(role.work_type_label||"")+'</strong></div>','<div class="metric"><span>French</span><strong>'+esc(role.french_risk_label||"")+'</strong></div>','<div class="metric"><span>Bridge or slop</span><strong>'+esc(role.slop_verdict||"")+'</strong></div>','<div class="metric"><span>Biggest gap</span><strong>'+esc(role.biggest_resume_gap||"")+'</strong></div>'].join("")}
function renderDetail(){var role=selectedRole();if(!role){document.getElementById("detailTitle").textContent="Select a role.";return}document.getElementById("detailTitle").textContent=(role.company?role.company+" - ":"")+(role.title||"");document.getElementById("detailSummary").textContent=role.one_line_recommendation||role.explanation||"";document.getElementById("detailMetrics").innerHTML=metricRows(role);document.getElementById("detailSurfaced").textContent=role.why_surfaced||"";document.getElementById("detailMatter").textContent=role.why_could_matter||role.why_fit||"";document.getElementById("detailPath").textContent=role.path_logic||role.bridge_story||"";document.getElementById("detailRisk").textContent=role.main_risk||role.why_fail||"";document.getElementById("detailLanes").textContent=(role.opportunity_lanes&&role.opportunity_lanes.length?role.opportunity_lanes.join("\\n"):"No secondary lanes tagged.");var url=role.url||role.apply_url||"";document.getElementById("openBtn").onclick=function(){if(url)window.open(url,"_blank")};document.getElementById("copyBtn").onclick=function(){navigator.clipboard.writeText(url)}}
function pick(id){sel=id;render()}
async function loadSaved(){try{var r=await fetch(SRV+"/saved/list"),d=await r.json();SAVED=d.items||[];SAVED_INDEX={};for(var i=0;i<SAVED.length;i++)SAVED_INDEX[SAVED[i].fingerprint]=SAVED[i];render();renderSaved()}catch(e){SAVED=[];SAVED_INDEX={};render();renderSaved()}}
async function saveRole(role,bucket){if(!role)return;try{var r=await fetch(SRV+"/saved/save",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({review_bucket:bucket,role:role})}),d=await r.json();if(r.ok){SAVED=d.items||[];SAVED_INDEX={};for(var i=0;i<SAVED.length;i++)SAVED_INDEX[SAVED[i].fingerprint]=SAVED[i];render();renderSaved();return}alert(d.error||"Save failed")}catch(e){alert(e.message||"Save failed")}}
function saveSelected(bucket){saveRole(selectedRole(),bucket)}
function triageHtml(score){return metricRows(score)+'<div class="label">Why It Surfaced</div><div class="box">'+esc(score.why_surfaced||"")+'</div><div class="label">Why It Could Matter</div><div class="box">'+esc(score.why_could_matter||score.why_fit||"")+'</div><div class="label">Next Path Logic</div><div class="box">'+esc(score.path_logic||score.bridge_story||"")+'</div><div class="label">Main Risk</div><div class="box">'+esc(score.main_risk||score.why_fail||"")+'</div><div class="label">Lanes</div><div class="box">'+esc((score.opportunity_lanes||[]).join("\\n"))+'</div><div class="label">Recommendation</div><div class="box">'+esc(score.one_line_recommendation||"")+'</div>'}
async function evaluateTriage(){var payload={url:document.getElementById("tjUrl").value.trim(),title:document.getElementById("tjTitle").value.trim(),company:document.getElementById("tjCompany").value.trim(),location_text:document.getElementById("tjLocation").value.trim(),description_text:document.getElementById("tjDesc").value.trim()};document.getElementById("triageStatus").textContent="Evaluating...";try{var r=await fetch(SRV+"/triage/evaluate",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)}),d=await r.json();if(!r.ok){document.getElementById("triageStatus").textContent=d.error||"Triage failed.";document.getElementById("triageResult").textContent=d.error||"Triage failed.";return}TRIAGE={fingerprint:d.fingerprint,job:d.job,score:d.score};var score=d.score||{};score.classification=d.classification;score.recommendation=d.recommendation;score.fingerprint=d.fingerprint;score.source=d.job.source||"manual_input";score.company=d.job.company||"";score.title=d.job.title||"";score.location_text=d.job.location_text||"";score.url=d.job.url||d.job.apply_url||"";score.apply_url=d.job.apply_url||d.job.url||"";score.description_text=d.job.description_text||"";document.getElementById("triageResult").innerHTML=triageHtml(score);document.getElementById("triageStatus").textContent="Triage complete."}catch(e){document.getElementById("triageStatus").textContent=e.message||"Triage failed.";document.getElementById("triageResult").textContent=e.message||"Triage failed."}}
function saveTriage(bucket){if(!TRIAGE){alert("Evaluate a role first.");return}var role=Object.assign({},TRIAGE.score,{fingerprint:TRIAGE.fingerprint,source:TRIAGE.job.source||"manual_input",company:TRIAGE.job.company||"",title:TRIAGE.job.title||"",location_text:TRIAGE.job.location_text||"",url:TRIAGE.job.url||TRIAGE.job.apply_url||"",apply_url:TRIAGE.job.apply_url||TRIAGE.job.url||"",description_text:TRIAGE.job.description_text||""});saveRole(role,bucket)}
function sortSaved(rows){rows.sort(function(a,b){var ra=ord(REVIEW_ORDER,a.review_bucket,9)-ord(REVIEW_ORDER,b.review_bucket,9);if(ra!==0)return ra;var ca=ord(CITY_ORDER,a.city_lane||"Unknown",9)-ord(CITY_ORDER,b.city_lane||"Unknown",9);if(ca!==0)return ca;return String(a.company||"").localeCompare(String(b.company||""))})}
function renderSaved(){var rows=SAVED.slice();sortSaved(rows);if(rows.length&&(!savedSel||!SAVED_INDEX[savedSel]))savedSel=rows[0].fingerprint;var html="";for(var i=0;i<rows.length;i++){var r=rows[i],selCls=r.fingerprint===savedSel?' class="sel"':'';html+='<tr'+selCls+' onclick="pickSaved(\\''+esc(r.fingerprint)+'\\')"><td><span class="tag '+tagClass("review",r.review_bucket)+'">'+esc(r.review_bucket||"")+'</span></td><td><span class="tag city">'+esc(r.city_lane||"")+'</span></td><td><span class="tag lane">'+esc(r.classification||"")+'</span></td><td><span class="tag '+tagClass("decision",r.recommendation)+'">'+esc((r.recommendation||"").toUpperCase())+'</span></td><td><span class="tag world">'+esc(r.world_tier||"")+'</span></td><td>'+esc(r.company||"")+'</td><td>'+esc(r.title||"")+'</td></tr>'}document.getElementById("savedTb").innerHTML=html||'<tr><td colspan="7" class="small">No saved roles yet.</td></tr>';renderSavedDetail()}
function currentSaved(){return SAVED_INDEX[savedSel]||null}
function pickSaved(fingerprint){savedSel=fingerprint;renderSaved()}
function renderSavedDetail(){var role=currentSaved();if(!role){document.getElementById("savedTitle").textContent="Select a saved role.";return}document.getElementById("savedTitle").textContent=(role.company?role.company+" - ":"")+(role.title||"");document.getElementById("savedSummary").textContent=role.one_line_recommendation||role.explanation||"";document.getElementById("savedSurfaced").textContent=role.why_surfaced||"";document.getElementById("savedMatter").textContent=role.why_could_matter||role.why_fit||"";document.getElementById("savedPath").textContent=role.path_logic||role.bridge_story||"";document.getElementById("savedRisk").textContent=role.main_risk||role.why_fail||""}
function rebucketSaved(bucket){var role=currentSaved();if(role)saveRole(role,bucket)}
async function removeSaved(){var role=currentSaved();if(!role)return;try{var r=await fetch(SRV+"/saved/delete",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({fingerprint:role.fingerprint})}),d=await r.json();if(r.ok){SAVED=d.items||[];SAVED_INDEX={};for(var i=0;i<SAVED.length;i++)SAVED_INDEX[SAVED[i].fingerprint]=SAVED[i];savedSel=null;render();renderSaved();return}alert(d.error||"Remove failed")}catch(e){alert(e.message||"Remove failed")}}
function openSaved(){var role=currentSaved();if(role&&(role.url||role.apply_url))window.open(role.url||role.apply_url,"_blank")}
["q","fDecision","fCity","fLane","fWorld","fFunction","fWorkType","fFrench","fSlop"].forEach(function(id){document.getElementById(id).addEventListener("input",render);document.getElementById(id).addEventListener("change",render)});
document.querySelectorAll(".tab").forEach(function(tab){tab.onclick=function(){document.querySelectorAll(".tab").forEach(function(x){x.classList.remove("on")});document.querySelectorAll(".page").forEach(function(x){x.classList.remove("on")});tab.classList.add("on");document.getElementById("page-"+tab.dataset.page).classList.add("on")}});
document.getElementById("triageBtn").addEventListener("click",evaluateTriage);
(async function(){statLine();activePath();setupFilters();defaultWorkType();await loadSaved();render();renderSaved()})()
</script></body></html>"""
