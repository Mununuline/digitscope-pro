const $=id=>document.getElementById(id);
const PUBLIC_WS="wss://api.derivws.com/trading/v1/options/ws/public";
const MARKET_NAMES={"1HZ10V":"Volatility 10 (1s)","1HZ25V":"Volatility 25 (1s)","1HZ50V":"Volatility 50 (1s)","1HZ75V":"Volatility 75 (1s)","1HZ100V":"Volatility 100 (1s)","R_10":"Volatility 10","R_25":"Volatility 25","R_50":"Volatility 50","R_75":"Volatility 75","R_100":"Volatility 100"};
let publicWs=null, tradeWs=null, ticks=[], accounts=[], selectedAccount=null, proposal=null, openContracts=new Map(), tradeHistory=[], req=0, pending=new Map(), realAck=false, marketPipSize=2;
const rid=()=>++req;
function toast(s){const el=$("toast");el.textContent=s;el.classList.remove("hidden");setTimeout(()=>el.classList.add("hidden"),3600)}
function status(s,live=false){$("feedStatus").textContent=s;$("feedDot").classList.toggle("live",live)}
function send(ws,obj){if(ws&&ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(obj))}
function rpc(ws,obj){return new Promise((resolve,reject)=>{const id=rid();pending.set(id,{resolve,reject,timer:setTimeout(()=>{pending.delete(id);reject(new Error("Deriv request timed out"))},12000)});send(ws,{...obj,req_id:id})})}
function decimalsFromPrice(v){const s=String(v);return s.includes(".")?s.split(".")[1].length:2}
function lastDigit(v){const s=Number(v).toFixed(Math.min(10,decimalsFromPrice(v)));return Number(s.replace(/\D/g,"").slice(-1))}
function connectMarket(){
 if(publicWs)try{publicWs.close()}catch{}
 ticks=[];renderStats();const sym=$("marketSelect").value;
 status("Connecting market…");
 publicWs=new WebSocket(PUBLIC_WS);
 publicWs.onopen=()=>{status("Live market feed",true);send(publicWs,{ticks_history:sym,count:1000,end:"latest",style:"ticks",req_id:rid()});send(publicWs,{ticks:sym,subscribe:1,req_id:rid()})};
 publicWs.onmessage=e=>{let d;try{d=JSON.parse(e.data)}catch{return}
  if(d.error){status("Feed error");return}
  if(d.msg_type==="history"&&d.history?.prices){if(d.pip_size!==undefined)marketPipSize=Number(d.pip_size);ticks=d.history.prices.map((p,i)=>({quote:Number(p),epoch:d.history.times?.[i]||0,digit:lastDigit(p,marketPipSize)}));renderStats()}
  if(d.msg_type==="tick"&&d.tick){if(d.tick.pip_size!==undefined)marketPipSize=Number(d.tick.pip_size);ticks.push({quote:Number(d.tick.quote),epoch:d.tick.epoch,digit:lastDigit(d.tick.quote,marketPipSize)});if(ticks.length>1000)ticks.shift();renderStats()}
 };
 publicWs.onclose=()=>status("Market feed disconnected");
 publicWs.onerror=()=>status("Market feed error");
}
function renderStats(){
 const sym=$("marketSelect").value, n=ticks.length, c=Array(10).fill(0);
 ticks.forEach(t=>c[t.digit]++);
 const hi=Math.max(...c,0),lo=n?Math.min(...c):0;
 $("marketName").textContent=MARKET_NAMES[sym];$("symbolCode").textContent=sym;$("sampleSize").textContent=n;
 if(n){const t=ticks.at(-1);$("quote").textContent=t.quote.toFixed(decimalsFromPrice(t.quote));$("lastDigit").textContent=t.digit;$("lastTick").textContent=new Date(t.epoch*1000).toLocaleTimeString("en-GB",{timeZone:"Africa/Kampala"})+" EAT"}
 else{$("quote").textContent="—";$("lastDigit").textContent="—";$("lastTick").textContent="Waiting for ticks"}
 $("digitGrid").innerHTML=c.map((v,i)=>`<div class="digit ${n&&v===hi?"hot":n&&v===lo?"cold":""}"><b>${i}</b><small>${n?(v/n*100).toFixed(1):"0.0"}%</small></div>`).join("");
 $("recent").innerHTML=ticks.slice(-24).map(t=>`<span class="tick">${t.digit}</span>`).join("");
 const over=n?ticks.filter(t=>t.digit>4).length/n*100:0,under=100-over,even=n?ticks.filter(t=>t.digit%2===0).length/n*100:0,odd=100-even;
 const metrics=[["OVER 4",over],["UNDER 5",under],["EVEN",even],["ODD",odd]];
 $("meters").innerHTML=metrics.map(([k,v])=>`<div class="meter"><b>${k}</b><div class="track"><div class="fill" style="width:${v}%"></div></div><strong>${n?v.toFixed(1)+"%":"—"}</strong></div>`).join("");
 if(n<100){$("signalTitle").textContent="Collecting sample";$("signalCopy").textContent=`${n}/100 ticks. No reading yet.`}
 else{const expected=n/10,rank=c.map((v,i)=>({i,v})).sort((a,b)=>b.v-a.v),delta=Math.max(...c.map(v=>Math.abs(v-expected)/expected));$("signalTitle").textContent=delta<.22?"No notable imbalance":"Observed frequency imbalance";$("signalCopy").textContent=`Most frequent: ${rank[0].i} (${(rank[0].v/n*100).toFixed(1)}%). Least frequent: ${rank[9].i} (${(rank[9].v/n*100).toFixed(1)}%). Descriptive only.`}
}
async function api(path,body){const r=await fetch(path,{method:body?"POST":"GET",headers:body?{"Content-Type":"application/json"}:{},body:body?JSON.stringify(body):undefined});const d=await r.json();if(!r.ok)throw new Error(d.error||"Request failed");return d}
async function connectDeriv(){
 try{
  const d=await api("/api/config");if(!d.connected){location.href="/auth/login";return}
  await loadAccounts();
 }catch(e){toast(e.message)}
}
async function loadAccounts(){
 const d=await api("/api/accounts");accounts=d.accounts||[];
 if(!accounts.length){toast("No Deriv trading accounts returned.");return}
 $("accountSelect").innerHTML=accounts.map((a,i)=>`<option value="${i}">${a.display_name||a.account_id} · ${String(a.account_type).toUpperCase()} · ${a.currency}</option>`).join("");
 $("accountSelect").disabled=false;$("loginBtn").classList.add("hidden");$("logoutBtn").classList.remove("hidden");$("quoteBtn").disabled=false;$("refreshPortfolio").disabled=false;
 await selectAccount(0);
}
async function selectAccount(i){
 selectedAccount=accounts[Number(i)];if(!selectedAccount)return;
 const type=String(selectedAccount.account_type).toLowerCase().includes("demo")||String(selectedAccount.account_id).toUpperCase().startsWith("VRTC")?"demo":"real";
 selectedAccount.mode=type;$("realConfirm").checked=false;$("balance").textContent="Connecting…";
 try{
  const d=await api("/api/otp",{accountId:selectedAccount.account_id,accountType:type,realAcknowledged:type==="real"});
  if(tradeWs)try{tradeWs.close()}catch{}
  tradeWs=new WebSocket(d.url);
  tradeWs.onopen=()=>{send(tradeWs,{balance:1,subscribe:1,req_id:rid()});send(tradeWs,{portfolio:1,req_id:rid()});toast(type.toUpperCase()+" account connected")};
  tradeWs.onmessage=e=>{let m;try{m=JSON.parse(e.data)}catch{return}
   if(m.req_id&&pending.has(m.req_id)){const p=pending.get(m.req_id);clearTimeout(p.timer);pending.delete(m.req_id);m.error?p.reject(new Error(m.error.message)):p.resolve(m);return}
   if(m.msg_type==="balance"&&m.balance){$("balance").textContent=`${m.balance.currency} ${Number(m.balance.balance).toFixed(2)}`}
   if(m.msg_type==="proposal_open_contract"&&m.proposal_open_contract){const c=m.proposal_open_contract;updateContract(c)}
   if(m.msg_type==="portfolio")renderPortfolio(m.portfolio?.contracts||[]);
   if(m.msg_type==="buy"&&m.buy){toast("Contract purchased: "+m.buy.contract_id);tradeHistory.unshift({id:m.buy.contract_id,stake:m.buy.buy_price,status:"OPEN",type:$("contractType").value});renderHistory();send(tradeWs,{proposal_open_contract:1,contract_id:m.buy.contract_id,subscribe:1,req_id:rid()});send(tradeWs,{balance:1,subscribe:1,req_id:rid()})}
   if(m.msg_type==="sell"&&m.sell){toast("Sell request accepted");send(tradeWs,{portfolio:1,req_id:rid()})}
  };
  tradeWs.onerror=()=>toast("Authenticated Deriv connection error");
  tradeWs.onclose=()=>{$("balance").textContent="Disconnected"};
  $("quoteBtn").disabled=false;
 }catch(e){$("balance").textContent="—";toast(e.message)}
}
function updateContract(c){
 const row=tradeHistory.find(x=>String(x.id)===String(c.contract_id));if(row){row.status=c.status||row.status;row.profit=c.profit;renderHistory()}
 renderPortfolio([...openContracts.values(),c].filter((v,i,a)=>a.findIndex(x=>x.contract_id===v.contract_id)===i));
}
function renderPortfolio(list){
 const live=list.filter(c=>c.is_valid_to_sell||c.status==="open");
 $("positions").innerHTML=live.length?live.map(c=>`<div class="position"><b>#${c.contract_id}</b> ${c.contract_type||""}<span>${c.status||"OPEN"}</span><small>Buy ${c.buy_price} · P/L ${c.profit??"—"}</small>${c.is_valid_to_sell?`<button class="btn ghost sell-btn" data-id="${c.contract_id}">Sell early</button>`:""}</div>`).join(""):"<div class='empty'>No open positions.</div>";
 document.querySelectorAll(".sell-btn").forEach(b=>b.onclick=()=>sellContract(b.dataset.id));
}
function renderHistory(){$("history").innerHTML=tradeHistory.slice(0,8).map(t=>`<div class="history-row"><b>#${t.id}</b><span>${t.type}</span><small>${t.status}${t.profit!==undefined?" · P/L "+t.profit:""}</small></div>`).join("")||"No trades in this session."}
async function getProposal(){
 if(!tradeWs||tradeWs.readyState!==WebSocket.OPEN||!selectedAccount){toast("Connect an account first.");return}
 const stake=Number($("stake").value),duration=Number($("duration").value),barrier=$("barrier").value;
 if(!Number.isFinite(stake)||stake<=0||stake>1000||!Number.isInteger(duration)||duration<1||duration>10){toast("Check stake and duration limits.");return}
 if(selectedAccount.mode==="real"&&!$("realConfirm").checked){toast("Tick the real-account authorization box first.");return}
 const reqData={proposal:1,amount:stake,basis:"stake",contract_type:$("contractType").value,currency:selectedAccount.currency||"USD",duration,duration_unit:"t",underlying_symbol:$("marketSelect").value,barrier,req_id:rid()};
 try{const d=await rpc(tradeWs,reqData);proposal=d.proposal;$("proposalBox").classList.remove("hidden");$("proposalBox").innerHTML=`<label>DERIV QUOTE</label><br><strong>${proposal.ask_price} ${proposal.currency}</strong><div>Potential payout: ${proposal.payout} ${proposal.currency} · Spot: ${proposal.spot}</div><small>Quote is time-sensitive. Request a fresh quote before purchase if it expires.</small>`;$("buyBtn").disabled=false;toast("Quote received. Review before buying.")}
 catch(e){toast("Quote failed: "+e.message)}
}
function requestBuy(){
 if(!proposal){toast("Get a fresh quote first.");return}
 if(selectedAccount.mode==="real"&&!$("realConfirm").checked){toast("Real-account authorization is required.");return}
 $("confirmDetails").textContent=`${selectedAccount.mode.toUpperCase()} · ${MARKET_NAMES[$("marketSelect").value]} · ${$("contractType").value} · barrier ${$("barrier").value} · stake ${proposal.ask_price} ${proposal.currency} · ${$("duration").value} tick(s).`;
 $("confirmModal").classList.remove("hidden");
}
async function confirmBuy(){
 $("confirmModal").classList.add("hidden");
 if(!proposal||!tradeWs||tradeWs.readyState!==WebSocket.OPEN)return;
 try{
  const d=await rpc(tradeWs,{buy:proposal.id,price:Number(proposal.ask_price),req_id:rid()});
  if(d.buy){toast("Order accepted by Deriv.");tradeHistory.unshift({id:d.buy.contract_id,stake:d.buy.buy_price,status:"OPEN",type:$("contractType").value});renderHistory();send(tradeWs,{proposal_open_contract:1,contract_id:d.buy.contract_id,subscribe:1,req_id:rid()});send(tradeWs,{balance:1,subscribe:1,req_id:rid()});send(tradeWs,{portfolio:1,req_id:rid()})}
  proposal=null;$("proposalBox").classList.add("hidden");$("buyBtn").disabled=true;
 }catch(e){toast("Purchase failed: "+e.message)}
}
async function sellContract(id){
 if(!selectedAccount||selectedAccount.mode==="real"&&!$("realConfirm").checked){toast("For a real account, confirm the real-account authorization first.");return}
 if(!confirm("Sell contract "+id+" now? Deriv may return a different sell price than the current displayed value."))return;
 try{const d=await rpc(tradeWs,{sell:Number(id),price:0,req_id:rid()});toast(d.sell?"Sell request sent.":"Sell request submitted.")}
 catch(e){toast("Sell failed: "+e.message)}
}
async function logout(){try{await fetch("/auth/logout",{method:"POST"})}catch{}if(tradeWs)tradeWs.close();location.href="/"}
$("loginBtn").onclick=connectDeriv;$("logoutBtn").onclick=logout;$("marketSelect").onchange=()=>{proposal=null;$("buyBtn").disabled=true;$("proposalBox").classList.add("hidden");connectMarket()};
$("accountSelect").onchange=e=>selectAccount(e.target.value);$("quoteBtn").onclick=getProposal;$("buyBtn").onclick=requestBuy;$("cancelBuy").onclick=()=>$("confirmModal").classList.add("hidden");$("confirmBuy").onclick=confirmBuy;
$("realConfirm").onchange=e=>{realAck=e.target.checked;if(realAck)toast("Real-account trade authorization enabled for this session.")};
$("refreshPortfolio").onclick=()=>send(tradeWs,{portfolio:1,req_id:rid()});
$("clock").textContent=new Date().toLocaleTimeString("en-GB",{timeZone:"Africa/Kampala"});
setInterval(()=>$("clock").textContent=new Date().toLocaleTimeString("en-GB",{timeZone:"Africa/Kampala"}),1000);
connectMarket();
(async()=>{const p=new URLSearchParams(location.search);if(p.get("auth_error"))toast("Authorization error: "+p.get("auth_error"));if(p.get("connected"))toast("Deriv connected. Choose an account.");try{const c=await api("/api/config");if(c.connected)await loadAccounts()}catch{}})();
