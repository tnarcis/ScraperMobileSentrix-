/**
 * Main Page JavaScript for MobileSentrix Extractor
 * Handles scraping, filtering, and UI interactions
 */

// Utility functions
const $ = id => document.getElementById(id);
const escapeHtml = (str = '') => String(str)
  .replace(/&/g,'&amp;')
  .replace(/</g,'&lt;')
  .replace(/>/g,'&gt;')
  .replace(/"/g,'&quot;')
  .replace(/'/g,'&#39;');

// DOM elements
const runBtn = $('runBtn'), csvBtn = $('csvBtn'), xlsxBtn = $('xlsxBtn'), copyBtn = $('copyBtn');
const clearResultsBtn = $('clearResultsBtn');
const urlsTA = $('urls'), percentInput = $('percent'), absOffInput = $('absOff');
const priceMin = $('priceMin'), priceMax = $('priceMax');
const kwInclude = $('kwInclude'), kwExclude = $('kwExclude');
const sortBy = $('sortBy'), hideDupes = $('hideDupes'), groupModel = $('groupModel');
const csvUpload = $('csvUpload'), dropPct = $('dropPct');
const alertBox = $('alert'), tbody = document.querySelector('#resultsTable tbody');
const searchInput = $('search'), darkMode = $('darkMode'), loadingOverlay = $('loading');
const countBadge = $('countBadge'), showWatchlistOnly = $('showWatchlistOnly');
const exportWatchlistBtn = $('exportWatchlistBtn');
const clearWatchlistBtn = $('clearWatchlistBtn');
const resultsEmpty = $('resultsEmpty');
const resultsFooter = $('resultsFooter');
const pageSizeSelect = $('pageSize');
const prevPageBtn = $('prevPage');
const nextPageBtn = $('nextPage');
const pageInfo = $('pageInfo');
const resultsTableContainer = document.querySelector('.results-table');
const modelChipWrap = $('modelChipWrap');
const heroWatchCount = $('heroWatchCount');
const currentDateValue = $('currentDateValue');
const currentTimeValue = $('currentTimeValue');
const dateChip = $('dateChip');
const timeChip = $('timeChip');
const exportActions = $('exportActions');

// State variables
let rawItems = [];            // server items
let rows = [];                // rendered/filtered rows
let compareMap = new Map();   // normalized title -> price
let lastExportRows = [];      // for CSV/XLSX export
let currentPage = 1;
let pageSize = 25;
let currentModels = [];
let loadingMessages = [];
let loadingInterval;

// Constants
const MODEL_STORAGE_KEY = 'msx_last_models_v1';
const PAGE_SIZE_KEY = 'msx_page_size_v1';
const RESULTS_STORAGE_KEY = 'msx_results_v1';
const MODEL_SKIP_SEGMENTS = new Set([
  'replacement-parts','parts','apple','samsung','huawei','xiaomi','oneplus','google','lg','sony','nokia','motorola','oppo','vivo','ipad','iphone','watch','macbook','mac','tablet',
  'iphone-parts','ipad-parts','watch-parts','tablet-parts','phone-parts','category','products','product','collections','accessories','accessory','shop','all','index'
]);

// Dynamic loading messages for scraping (default)
let scrapingMessages = [
  { text: "Connecting to Store...", sub: "Establishing secure connection" },
  { text: "Analyzing Page Structure...", sub: "Understanding the website layout" },
  { text: "Extracting Product Data...", sub: "Finding products and prices" },
  { text: "Processing Images...", sub: "Collecting product images" },
  { text: "Calculating Discounts...", sub: "Computing price differences" },
  { text: "Organizing Results...", sub: "Sorting and filtering data" },
  { text: "Almost Complete...", sub: "Finalizing extraction process" }
];

// Function to detect site type from URLs and update loading messages
function getScrapingMessages(urls) {
  const urlList = urls.split('\n').map(s => s.trim()).filter(Boolean);
  
  // Check which sites are being scraped
  const hasMobileSentrix = urlList.some(url => url.includes('mobilesentrix.'));
  const hasXCellParts = urlList.some(url => url.includes('xcellparts.com'));
  const hasTXParts = urlList.some(url => url.includes('txparts.com'));
  
  let siteName = "Store";
  let siteDesc = "products";
  
  if (hasXCellParts && !hasMobileSentrix && !hasTXParts) {
    siteName = "XCellParts";
    siteDesc = "WooCommerce products";
  } else if (hasTXParts && !hasMobileSentrix && !hasXCellParts) {
    siteName = "TXParts";
    siteDesc = "catalog items";
  } else if (hasMobileSentrix && !hasXCellParts && !hasTXParts) {
    siteName = "MobileSentrix";
    siteDesc = "products";
  } else if (hasXCellParts || hasTXParts || hasMobileSentrix) {
    siteName = "Multiple Sites";
    siteDesc = "products from all sites";
  }
  
  return [
    { text: `Connecting to ${siteName}...`, sub: "Establishing secure connection" },
    { text: "Analyzing Page Structure...", sub: "Understanding the website layout" },
    { text: `Extracting ${siteDesc}...`, sub: "Finding products and prices" },
    { text: "Processing Images...", sub: "Collecting product images" },
    { text: "Calculating Discounts...", sub: "Computing price differences" },
    { text: "Organizing Results...", sub: "Sorting and filtering data" },
    { text: "Almost Complete...", sub: "Finalizing extraction process" }
  ];
}

// Utility functions
const norm = s => (s||'')
  .toLowerCase()
  .replace(/[^a-z0-9\s]/g,' ')
  .replace(/\b(for|with|without|frame|lcd|assembly|screen|display|series|model|global|original|grade|version|and|the|a|an)\b/g,'')
  .replace(/\s+/g,' ')
  .trim();

function modelKey(title){
  let t = norm(title);
  const m = t.match(/\b([a-z]{1,3}-?\d{1,4}[a-z]?)\b|\b(galaxy|iphone|ipad|a\d{2}|a0?\d{1,2}|xs?max|pro|max|mini|se|plus)\b/g);
  if(m) return m.join(' ');
  return t;
}

function formatSource(site){
  if(!site) return { label: '', title: '' };
  const raw = String(site).trim();
  if(!raw) return { label: '', title: '' };

  let label = raw;
  if(/^https?:\/\//i.test(raw)){
    try {
      const u = new URL(raw);
      label = u.hostname;
    } catch (_) {
      // keep raw fallback
    }
  }

  if(label === raw){
    label = label.replace(/^https?:\/\//i,'');
  }

  label = label.replace(/^www\./i,'');
  if(label.includes('/')) label = label.split('/')[0];
  label = label.trim();

  return { label: label || raw, title: raw };
}

const STOCK_LABELS = {
  in_stock: { label: 'In Stock', tone: 'positive' },
  limited: { label: 'Limited', tone: 'warning' },
  back_order: { label: 'Back Order', tone: 'warning' },
  preorder: { label: 'Preorder', tone: 'info' },
  pre_order: { label: 'Preorder', tone: 'info' },
  out_of_stock: { label: 'Out of Stock', tone: 'danger' },
  sold_out: { label: 'Sold Out', tone: 'danger' },
  discontinued: { label: 'Discontinued', tone: 'muted' },
  unavailable: { label: 'Unavailable', tone: 'muted' },
  unknown: { label: 'Unknown', tone: 'muted' }
};

const STOCK_KEYWORDS = [
  { regex: /(not available|no stock|sold out|unavailable|out of stock|out-of-stock|outofstock)/i, value: 'out_of_stock' },
  { regex: /(back[-\s]?order|awaiting stock|ships in|special order|backordered|backorder)/i, value: 'back_order' },
  { regex: /(pre[-\s]?order|coming soon|preorder)/i, value: 'preorder' },
  { regex: /(limited|low stock|few left|almost gone|only \d+|limitedavailability)/i, value: 'limited' },
  { regex: /(discontinued|no longer available|retired)/i, value: 'discontinued' },
  { regex: /(in stock|available now|ready to ship|ships today|instock|yes$|available$)/i, value: 'in_stock' }
];

function toTitleCase(text = ''){
  return text
    .split(/[_\s-]+/)
    .filter(Boolean)
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function normaliseStockValue(raw){
  if(raw == null) return 'unknown';
  const str = String(raw).trim();
  if(!str) return 'unknown';
  const lowered = str.toLowerCase();
  const canonical = lowered.replace(/[^a-z]+/g,'_');
  if(STOCK_LABELS[canonical]) return canonical;
  for(const { regex, value } of STOCK_KEYWORDS){
    if(regex.test(lowered)){
      return value;
    }
  }
  return canonical || 'unknown';
}

function getStockMeta(raw){
  const value = normaliseStockValue(raw);
  const meta = STOCK_LABELS[value] || {};
  const label = meta.label || toTitleCase(value || 'Unknown');
  const tone = meta.tone || 'muted';
  return { value, label, tone };
}

const moneyFormatter = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD'
});

function formatMoney(val){
  if(val == null || Number.isNaN(val) || !Number.isFinite(val)) return '';
  return moneyFormatter.format(val);
}

function extractDomain(url){
  try {
    const parsed = new URL(url);
    return parsed.hostname.replace(/^www\./i, '');
  } catch {
    return '';
  }
}

function formatModelWord(word){
  const lower = word.toLowerCase();
  if(!lower) return '';
  if(/^[0-9]+$/.test(lower)) return word.toUpperCase();
  if(/^[a-z]?\d+[a-z]?$/i.test(word)) return word.toUpperCase();
  const upperTokens = ['se','tv','lte','5g','usb'];
  if(upperTokens.includes(lower)) return lower.toUpperCase();
  if(lower === 'iphone') return 'iPhone';
  if(lower === 'ipad') return 'iPad';
  if(lower === 'ipod') return 'iPod';
  return lower.charAt(0).toUpperCase() + lower.slice(1);
}

function normaliseModelString(candidate){
  if(!candidate) return '';
  let pretty = decodeURIComponent(candidate)
    .replace(/[-_]+/g, ' ')
    .replace(/\s+/g,' ')
    .replace(/\.(htm|html)$/i,'')
    .trim();
  if(!pretty) return '';
  const words = pretty.split(' ').filter(Boolean).map(formatModelWord);
  let result = words.join(' ');
  result = result
    .replace(/\b5g\b/gi,'5G')
    .replace(/\bUsb\b/g,'USB')
    .replace(/\bWi\s?fi\b/gi,'Wi‑Fi');
  return result.trim();
}

function hostToSource(url){
  try{
    const { hostname } = new URL(url);
    if(/\.ca$/i.test(hostname)) return 'MS-CA';
    return 'MS-US';
  }catch{
    return 'MS';
  }
}

function deriveModelName(url){
  try{
    const u = new URL(url);
    const decoded = u.pathname.split('/').filter(Boolean).map(seg => decodeURIComponent(seg));
    let candidate = '';
    for(let i = decoded.length - 1; i >= 0; i--){
      const seg = decoded[i].toLowerCase();
      if(!MODEL_SKIP_SEGMENTS.has(seg)){
        candidate = decoded[i];
        break;
      }
    }
    if(!candidate && decoded.length){
      candidate = decoded[decoded.length - 1];
    }
    const pretty = normaliseModelString(candidate || '');
    return pretty;
  }catch{
    return '';
  }
}

function deriveModelNames(list){
  const seen = new Set();
  const models = [];
  (list || []).forEach(url => {
    const trimmed = (url || '').trim();
    if(!trimmed) return;
    const name = deriveModelName(trimmed);
    if(!name) return;
    const source = hostToSource(trimmed);
    const key = `${name}__${source}`;
    if(seen.has(key)) return;
    seen.add(key);
    models.push({ name, source });
  });
  return models;
}

function renderModelChips(models){
  if(!modelChipWrap) return;
  modelChipWrap.innerHTML = '';
  if(!models || !models.length){
    modelChipWrap.hidden = true;
    localStorage.removeItem(MODEL_STORAGE_KEY);
    return;
  }
  const MAX_VISIBLE = 3;
  models.slice(0, MAX_VISIBLE).forEach(model => {
    const chip = document.createElement('button');
    chip.type = 'button';
    chip.className = 'context-chip';
    chip.setAttribute('aria-pressed','false');
    chip.innerHTML = `<span>${escapeHtml(model.name)}</span><span class="context-chip__badge">${escapeHtml(model.source)}</span>`;
    modelChipWrap.appendChild(chip);
  });
  if(models.length > MAX_VISIBLE){
    const more = document.createElement('span');
    more.className = 'context-chip context-chip--more';
    more.textContent = `+${models.length - MAX_VISIBLE} more`;
    more.title = models.slice(MAX_VISIBLE).map(m => `${m.name} (${m.source})`).join(', ');
    modelChipWrap.appendChild(more);
  }
  modelChipWrap.hidden = false;
  localStorage.setItem(MODEL_STORAGE_KEY, JSON.stringify(models.slice(0, 8)));
}

function parseMoney(maybe){
  if(!maybe) return null;
  const m = String(maybe).match(/([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})|[0-9]+(?:\.[0-9]{2})?)/);
  if(!m) return null;
  return parseFloat(m[1].replace(/,/g,''));
}

function lookupComparePrice(title, site, url){
  const safeTitle = (title || '').trim();
  const titleKey = safeTitle.toLowerCase();
  const normKey = norm(safeTitle);
  const modelKeyVal = modelKey(safeTitle);
  const siteKey = String(site || '').trim().toLowerCase();
  const urlKey = String(url || '').trim().toLowerCase();

  const grab = key => (key ? compareMap.get(key) : undefined);

  return grab(urlKey && `url:${urlKey}`)
    ?? grab(siteKey && titleKey && `site:${siteKey}:${titleKey}`)
    ?? grab(titleKey && `title:${titleKey}`)
    ?? grab(siteKey && modelKeyVal && `site-model:${siteKey}:${modelKeyVal}`)
    ?? grab(normKey && `norm:${normKey}`)
    ?? grab(modelKeyVal && `model:${modelKeyVal}`);
}

function startDateTimeTicker(){
  if(!currentDateValue && !currentTimeValue) return;
  const tz = 'Asia/Karachi';
  const longTzLabel = 'Pakistan Standard Time (UTC+05:00)';
  const dateFormatter = new Intl.DateTimeFormat('en-PK', {
    weekday: 'short',
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    timeZone: tz
  });
  const timeFormatter = new Intl.DateTimeFormat('en-PK', {
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
    timeZone: tz
  });

  const applyDateTime = () => {
    const now = new Date();
    if(currentDateValue){
      const formattedDate = dateFormatter.format(now);
      currentDateValue.textContent = formattedDate;
      if(dateChip) dateChip.title = `${formattedDate} • ${longTzLabel}`;
    }
    if(currentTimeValue){
      const formattedTime = timeFormatter.format(now);
      currentTimeValue.textContent = formattedTime;
      if(timeChip) timeChip.title = `${formattedTime} • ${longTzLabel}`;
    }
  };

  applyDateTime();
  const tick = () => {
    applyDateTime();
    const now = new Date();
    const msUntilNextMinute = 60000 - (now.getSeconds() * 1000 + now.getMilliseconds());
    setTimeout(tick, Math.max(1000, msUntilNextMinute));
  };
  const now = new Date();
  const msUntilNextMinute = 60000 - (now.getSeconds() * 1000 + now.getMilliseconds());
  setTimeout(tick, Math.max(1000, msUntilNextMinute));
}

function csvToRowsVisible(){
  const header = ['image_url','title','final_price','original_price','percent_off','absolute_off','compare_price','delta','delta_pct','stock_status','stock_value','url','source','model'];
  const body = rows.map(r => [
    r.image_url || '',
    r.title || '',
    r.final || '',
    r.original || '',
    r.percent_off ?? '',
    r.absolute_off ?? '',
    Number.isFinite(r.compare_price) ? r.compare_price : '',
    r.delta ?? '',
    r.delta_pct ?? '',
    r.stock_meta?.label || '',
    r.stock_meta?.value || '',
    r.url || '',
    r.site || '',
    r.model || ''
  ]);
  return [header, ...body];
}

function toCSV(table){
  return table.map(cols => cols.map(v => `"${String(v??'').replaceAll('"','""')}"`).join(',')).join('\n');
}

// Watchlist functions
function wlKey(){ return 'ms_watchlist_v8'; }
function loadWatch(){ try{ return new Set(JSON.parse(localStorage.getItem(wlKey())||'[]')); } catch{ return new Set(); } }
function saveWatch(set){ localStorage.setItem(wlKey(), JSON.stringify(Array.from(set))); }
let watch = loadWatch();

// Price memory functions
function priceMemKey(){ return 'ms_last_prices_v8'; }
function loadPriceMem(){ try{ return JSON.parse(localStorage.getItem(priceMemKey())||'{}'); } catch{ return {}; } }
function savePriceMem(mem){ localStorage.setItem(priceMemKey(), JSON.stringify(mem)); }
let priceMem = loadPriceMem();

// Results persistence functions
function saveResults(items, models = []) {
  try {
    const data = {
      items: items || [],
      models: models || [],
      timestamp: Date.now()
    };
    localStorage.setItem(RESULTS_STORAGE_KEY, JSON.stringify(data));
    console.log('✅ Saved', items.length, 'results to localStorage');
  } catch(e) {
    console.error('❌ Failed to save results:', e);
  }
}

function loadResults() {
  try {
    const data = JSON.parse(localStorage.getItem(RESULTS_STORAGE_KEY) || 'null');
    if (!data) {
      console.log('📭 No saved results found');
      return null;
    }
    // Check if results are less than 24 hours old
    const age = Date.now() - (data.timestamp || 0);
    const ageHours = (age / (1000 * 60 * 60)).toFixed(1);
    if (age > 24 * 60 * 60 * 1000) {
      console.log('🗑️ Saved results expired (', ageHours, 'hours old)');
      localStorage.removeItem(RESULTS_STORAGE_KEY);
      return null;
    }
    console.log('📦 Found saved results:', data.items.length, 'items,', ageHours, 'hours old');
    return data;
  } catch(e) {
    console.error('❌ Failed to load results:', e);
    return null;
  }
}

function clearResults() {
  try {
    localStorage.removeItem(RESULTS_STORAGE_KEY);
    console.log('🧹 Cleared saved results');
  } catch(e) {
    console.error('❌ Failed to clear results:', e);
  }
}

// Advanced Loading System with Progress Tracking
class LoadingProgressTracker {
  constructor() {
    this.startTime = null;
    this.currentStep = 0;
    this.totalSteps = 7;
    this.isActive = false;
    this.progressInterval = null;
    this.messageInterval = null;
    this.timerInterval = null;
  }

  start(urls = '') {
    this.isActive = true;
    this.startTime = Date.now();
    this.currentStep = 0;
    
    // Get dynamic scraping messages based on URLs
    const messages = urls ? getScrapingMessages(urls) : scrapingMessages;
    
    const progressBar = document.querySelector('.progress-bar');
    const progressPercentage = document.querySelector('.progress-percentage');
    const loadingText = document.querySelector('.loading-text');
    const loadingMessage = document.querySelector('.loading-message');
    const timerValue = document.querySelector('.timer-value');
    const steps = document.querySelectorAll('.loading-step');
    
    // Reset UI
    if (progressBar) progressBar.style.width = '0%';
    if (progressPercentage) progressPercentage.textContent = '0%';
    
    // Reset steps
    steps.forEach(step => {
      step.classList.remove('active', 'completed');
    });
    
    // Start timer
    this.timerInterval = setInterval(() => {
      if (this.startTime && timerValue) {
        const elapsed = ((Date.now() - this.startTime) / 1000).toFixed(1);
        timerValue.textContent = `${elapsed}s`;
      }
    }, 100);
    
    // Progressive percentage update
    this.progressInterval = setInterval(() => {
      if (!this.isActive) return;
      
      // Ensure currentStep is a valid number
      const step = isNaN(this.currentStep) ? 0 : this.currentStep;
      const baseProgress = step * 14;
      const randomIncrement = Math.random() * 8;
      const currentProgress = Math.min(baseProgress + randomIncrement, 98);
      const roundedProgress = Math.max(0, Math.floor(currentProgress)) || 0;
      
      if (progressBar) progressBar.style.width = `${roundedProgress}%`;
      if (progressPercentage) progressPercentage.textContent = `${roundedProgress}%`;
      
      // Update step indicators
      const activeStepIndex = Math.max(0, Math.floor(step));
      steps.forEach((step, index) => {
        step.classList.remove('active');
        if (index < activeStepIndex) {
          step.classList.add('completed');
        } else if (index === activeStepIndex) {
          step.classList.add('active');
        } else {
          step.classList.remove('completed');
        }
      });
      
    }, 300 + Math.random() * 200);
    
    // Message cycling
    let messageIndex = 0;
    if (messages[messageIndex]) {
      if (loadingText) loadingText.textContent = messages[messageIndex].text;
      if (loadingMessage) loadingMessage.textContent = messages[messageIndex].sub;
    }
    
    this.messageInterval = setInterval(() => {
      if (!this.isActive) return;
      
      messageIndex = (messageIndex + 1) % messages.length;
      this.currentStep = Math.max(0, Math.min(messageIndex, this.totalSteps - 1));
      
      // Ensure currentStep is always a valid number
      if (isNaN(this.currentStep)) {
        this.currentStep = 0;
      }
      
      if (messages[messageIndex]) {
        if (loadingText) loadingText.textContent = messages[messageIndex].text;
        if (loadingMessage) loadingMessage.textContent = messages[messageIndex].sub;
      }
    }, 1800 + Math.random() * 400);
    
    // Show overlay
    if (loadingOverlay) {
      loadingOverlay.classList.remove('d-none');
      loadingOverlay.style.display = 'flex';
    }
  }

  complete() {
    this.isActive = false;
    
    // Clear intervals
    clearInterval(this.progressInterval);
    clearInterval(this.messageInterval);
    clearInterval(this.timerInterval);
    
    const progressBar = document.querySelector('.progress-bar');
    const progressPercentage = document.querySelector('.progress-percentage');
    const loadingText = document.querySelector('.loading-text');
    const loadingMessage = document.querySelector('.loading-message');
    const steps = document.querySelectorAll('.loading-step');
    
    // Complete animation
    if (progressBar) progressBar.style.width = '100%';
    if (progressPercentage) progressPercentage.textContent = '100%';
    if (loadingText) loadingText.textContent = 'Complete!';
    if (loadingMessage) loadingMessage.textContent = 'Processing results...';
    
    // Mark all steps as completed
    steps.forEach(step => {
      step.classList.remove('active');
      step.classList.add('completed');
    });
    
    // Hide overlay after brief completion display
    setTimeout(() => {
      if (loadingOverlay) {
        loadingOverlay.classList.add('d-none');
        loadingOverlay.style.display = 'none';
      }
      
      // Final timer update
      const timerValue = document.querySelector('.timer-value');
      if (this.startTime && timerValue) {
        const totalTime = ((Date.now() - this.startTime) / 1000).toFixed(1);
        console.log(`Loading completed in ${totalTime}s`);
      }
    }, 800);
  }

  stop() {
    this.isActive = false;
    clearInterval(this.progressInterval);
    clearInterval(this.messageInterval);
    clearInterval(this.timerInterval);
    
    if (loadingOverlay) {
      loadingOverlay.classList.add('d-none');
      loadingOverlay.style.display = 'none';
    }
  }
}

const progressTracker = new LoadingProgressTracker();

// Loading and UI functions
function setLoading(on, urls = ''){
  if (on) {
    progressTracker.start(urls);
  } else {
    progressTracker.complete();
  }
  if (runBtn) runBtn.disabled = on;
}

function showAlert(type, msg){
  if (!alertBox) return;
  alertBox.className = 'alert cy-card p-3 alert-' + type;
  alertBox.textContent = msg;
  alertBox.classList.remove('d-none');
}

function clearAlert(){ 
  if (alertBox) alertBox.classList.add('d-none'); 
}

function updateStats(partial={}){
  const watchSize = watch.size;
  if(heroWatchCount){
    heroWatchCount.textContent = watchSize;
  }
  if(exportWatchlistBtn){
    exportWatchlistBtn.disabled = watchSize === 0;
  }
  if(clearWatchlistBtn){
    clearWatchlistBtn.disabled = watchSize === 0;
  }
}

// Main render function
function render(){
  if (!tbody) return;
  
  tbody.innerHTML = '';
  if(resultsTableContainer){
    resultsTableContainer.classList.remove('fade-in');
    void resultsTableContainer.offsetWidth;
    resultsTableContainer.classList.add('fade-in');
  }

  // Get input values
  // Keyword filters - Read from hidden inputs populated by chips
  const includeHidden = $('include');
  const excludeHidden = $('exclude');
  
  const inc = (includeHidden?.value || '')
    .split(',')
    .map(s => norm(s))
    .filter(Boolean);
  const exc = (excludeHidden?.value || '')
    .split(',')
    .map(s => norm(s))
    .filter(Boolean);

  const minP = Number.isFinite(parseFloat(priceMin?.value)) ? parseFloat(priceMin.value) : null;
  const maxP = Number.isFinite(parseFloat(priceMax?.value)) ? parseFloat(priceMax.value) : null;

  // Map raw items to rows
  rows = (rawItems || []).map(it => {
    const original = it.original_formatted || it.price_text || '';
    const finalRaw = it.discounted_formatted || '';
    const finalDisplay = finalRaw || original;
    const originalNum = parseMoney(original);
    const finalNum = parseMoney(finalRaw || original);
    const priceForDelta = Number.isFinite(originalNum) ? originalNum : finalNum;
    const percentOffActual = (Number.isFinite(originalNum) && Number.isFinite(finalNum) && originalNum > 0)
      ? +(((originalNum - finalNum) / originalNum) * 100).toFixed(2)
      : null;
    const absoluteOffActual = (Number.isFinite(originalNum) && Number.isFinite(finalNum))
      ? +(originalNum - finalNum).toFixed(2)
      : null;
  const cleanTitle = it.title || '';
  const model = modelKey(cleanTitle);
  const cmpPrice = lookupComparePrice(cleanTitle, it.site, it.url);
    let delta = null, deltaPct = null;
    if(priceForDelta != null && cmpPrice != null){
      delta = +(priceForDelta - cmpPrice).toFixed(2);
      deltaPct = cmpPrice ? +(((priceForDelta - cmpPrice)/cmpPrice)*100).toFixed(2) : null;
    }
    const stockMeta = getStockMeta(it.stock_status);
    const domain = extractDomain(it.url || it.source || '');
    const watchKey = it.url || '';
    const safeWatchKey = escapeHtml(watchKey);
    const isWatched = watch.has(watchKey) || watch.has(safeWatchKey);

    return {
      url: it.url,
      site: it.site || 'mobilesentrix',
      image_url: it.image_url || '',
      title: cleanTitle,
      original: original,
      percent_off: percentOffActual,
      absolute_off: absoluteOffActual,
      final: finalDisplay,
      final_num: finalNum,
      original_num: originalNum,
      clean_title: cleanTitle,
      model: model,
      compare_price: cmpPrice,
      delta: delta,
      delta_pct: deltaPct,
      watchlisted: isWatched,
      stock_meta: stockMeta,
      domain,
      model_label: model ? model.split(' ').map(formatModelWord).join(' ') : ''
    };
  });

  // Apply filters
  rows = rows.filter(r => {
    const t = norm((r.title || '') + ' ' + (r.url || ''));
    if (inc.length && !inc.every(k => t.includes(k))) return false;
    if (exc.length &&  exc.some(k => t.includes(k))) return false;
    return true;
  });

  // Price filters
  rows = rows.filter(r => {
    if(minP != null && (r.final_num == null || r.final_num < minP)) return false;
    if(maxP != null && (r.final_num != null && r.final_num > maxP)) return false;
    return true;
  });

  // Hide duplicates
  if(hideDupes?.checked){
    const seen = new Set();
    rows = rows.filter(r => {
      const key = r.model || r.title.toLowerCase();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  // Sort
  const sort = sortBy?.value;
  const coll = new Intl.Collator(undefined, {numeric:true, sensitivity:'base'});
  rows.sort((a,b)=>{
    switch(sort){
      case 'final_asc': return (a.final_num??Infinity) - (b.final_num??Infinity);
      case 'final_desc': return (b.final_num??-Infinity) - (a.final_num??-Infinity);
      case 'orig_asc': return (a.original_num??Infinity) - (b.original_num??Infinity);
      case 'orig_desc': return (b.original_num??-Infinity) - (a.original_num??-Infinity);
      case 'disc_asc': return (a.percent_off||0) - (b.percent_off||0);
      case 'disc_desc': return (b.percent_off||0) - (a.percent_off||0);
      case 'title_asc': return coll.compare(a.title, b.title);
      case 'title_desc': return coll.compare(b.title, a.title);
      case 'source_asc': return coll.compare(a.site, b.site);
      case 'source_desc': return coll.compare(b.site, a.site);
      default: return 0;
    }
  });

  // Group by model
  if(groupModel?.checked){
    rows.sort((a,b)=>{
      const x = coll.compare(a.model, b.model);
      if(x!==0) return x;
      return coll.compare(a.title, b.title);
    });
  }

  // Watchlist filter
  if(showWatchlistOnly?.checked){
    rows = rows.filter(r => r.watchlisted);
  }

  // Pagination
  const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
  if(pageSizeSelect && pageSizeSelect.value !== String(pageSize)){
    pageSizeSelect.value = String(pageSize);
  }
  if(rows.length === 0){
    currentPage = 1;
  } else if(currentPage > totalPages){
    currentPage = totalPages;
  }
  const start = (currentPage - 1) * pageSize;
  const pageRows = rows.slice(start, start + pageSize);

  // Render table rows
  pageRows.forEach((r, idx) => {
    const tr = document.createElement('tr');
    tr.className = 'row-cy';
    const star = r.watchlisted ? '★' : '☆';
    const rawUrl = r.url || '';
    const safeUrlAttr = escapeHtml(rawUrl);
    const safeTitle = escapeHtml(r.title || 'Untitled product');
    const finalDisplay = escapeHtml(r.final || '—');
    const originalDisplay = r.original ? escapeHtml(r.original) : '';
    const percentBadge = Number.isFinite(r.percent_off)
      ? `<span class="price-pill price-pill--discount">-${r.percent_off.toFixed(1)}%</span>`
      : '';
    const absoluteDisplay = Number.isFinite(r.absolute_off) ? formatMoney(r.absolute_off) : '';
    const absoluteBadge = absoluteDisplay
      ? `<span class="price-pill price-pill--savings">-${absoluteDisplay}</span>`
      : '';
    const compareDisplay = Number.isFinite(r.compare_price) ? formatMoney(r.compare_price) : '';
    const deltaMoneyDisplay = (r.delta == null || Number.isNaN(r.delta))
      ? ''
      : `${r.delta > 0 ? '+' : ''}${formatMoney(Math.abs(r.delta))}`;
    const deltaPctDisplay = (r.delta_pct == null || Number.isNaN(r.delta_pct))
      ? ''
      : `${r.delta_pct > 0 ? '+' : ''}${r.delta_pct.toFixed(2)}%`;
    const deltaToneClass = (r.delta == null)
      ? 'delta-badge--neutral'
      : (r.delta < 0 ? 'delta-badge--drop' : r.delta > 0 ? 'delta-badge--rise' : 'delta-badge--neutral');
    const deltaPctToneClass = (r.delta_pct == null)
      ? 'delta-badge--neutral'
      : (r.delta_pct < 0 ? 'delta-badge--drop' : r.delta_pct > 0 ? 'delta-badge--rise' : 'delta-badge--neutral');
    const stockLabel = escapeHtml(r.stock_meta?.label || 'Unknown');
    const stockTone = r.stock_meta?.tone || 'muted';
    const domainLabel = r.domain ? escapeHtml(r.domain) : '';
    const modelLabel = r.model_label ? escapeHtml(r.model_label) : '';
    const sourceInfo = formatSource(r.site || '');
    const sourceLabel = sourceInfo.label;
    const safeSourceLabel = escapeHtml(sourceLabel);
    const safeSourceTitle = escapeHtml(sourceInfo.title);
    const sourceShortAttr = sourceLabel && sourceLabel.length <= 4 ? " data-short='1'" : '';

    const productImage = r.image_url
      ? `<img src="${r.image_url}" class="product-thumb" alt="">`
      : '<div class="product-thumb product-thumb--placeholder" aria-hidden="true">📦</div>';

    const productLink = rawUrl
      ? `<a class="product-name" href="${safeUrlAttr}" target="_blank" rel="noopener">${safeTitle}</a>`
      : `<span class="product-name">${safeTitle}</span>`;

    const productTags = [];
    if(domainLabel) productTags.push(`<span class="product-tag">${domainLabel}</span>`);
    if(modelLabel) productTags.push(`<span class="product-tag product-tag--model">${modelLabel}</span>`);

    const changeBadges = [
      deltaMoneyDisplay ? `<span class="delta-badge ${deltaToneClass}">${deltaMoneyDisplay}</span>` : '',
      deltaPctDisplay ? `<span class="delta-badge ${deltaPctToneClass}">${deltaPctDisplay}</span>` : ''
    ].filter(Boolean).join('');

    tr.innerHTML = `
      <td class="col-index">${start + idx + 1}</td>
      <td class="star" data-url="${safeUrlAttr}" title="Toggle watchlist">${star}</td>
      <td class="product-cell">
        ${productImage}
        <div class="product-meta">
          ${productLink}
          ${productTags.length ? `<div class="product-meta__extras">${productTags.join('')}</div>` : ''}
        </div>
      </td>
      <td class="pricing-cell">
        <div class="price-primary">${finalDisplay}</div>
        <div class="price-secondary">
          ${originalDisplay ? `<span class="price-original">${originalDisplay}</span>` : ''}
          ${percentBadge}
          ${absoluteBadge}
        </div>
      </td>
      <td class="change-cell">
        ${compareDisplay ? `<div class="change-compare">Prev ${compareDisplay}</div>` : ''}
        ${changeBadges ? `<div class="change-diff">${changeBadges}</div>` : ''}
      </td>
      <td class="stock-cell">
        <span class="stock-chip stock-chip--${stockTone}">${stockLabel}</span>
      </td>
      <td class="source-cell">
        ${sourceLabel ? `<span class="source-chip" title="${safeSourceTitle}"${sourceShortAttr}>${safeSourceLabel}</span>` : ''}
      </td>
    `;
    const starCell = tr.querySelector('td.star');
    if(starCell) starCell.dataset.url = rawUrl;
    tbody.appendChild(tr);
  });

  // Update UI controls
  const has = rows.length > 0;
  if(resultsEmpty) resultsEmpty.classList.toggle('d-none', has);
  if(resultsFooter){
    resultsFooter.classList.toggle('d-none', !has);
    if(prevPageBtn) prevPageBtn.disabled = currentPage <= 1;
    if(nextPageBtn) nextPageBtn.disabled = currentPage >= totalPages;
    if(pageInfo) pageInfo.textContent = has ? `Page ${Math.min(currentPage, totalPages)} of ${totalPages}` : 'Page 0 of 0';
  }
  if(csvBtn) csvBtn.disabled = !has;
  if(xlsxBtn) xlsxBtn.disabled = !has;
  if(copyBtn) copyBtn.disabled = !has;
  if(countBadge) {
    countBadge.textContent = `${rows.length} item${rows.length === 1 ? '' : 's'}`;
    countBadge.classList.toggle('d-none', !has);
  }
  
  // Show/hide export actions based on whether we have results
  const exportActions = $('exportActions');
  if(exportActions) {
    exportActions.style.display = has ? 'block' : 'none';
  }

  // Export rows
  lastExportRows = rows.map(r => ({
    image_url: r.image_url || '',
    title: r.title || '',
    final_price: r.final || '',
    original_price: r.original || '',
    percent_off: r.percent_off ?? '',
    absolute_off: r.absolute_off ?? '',
    compare_price: Number.isFinite(r.compare_price) ? r.compare_price : '',
    delta: r.delta ?? '',
    delta_pct: r.delta_pct ?? '',
    stock_status: r.stock_meta?.label || '',
    stock_value: r.stock_meta?.value || '',
    url: r.url || '',
    source: r.site || '',
    model: r.model || ''
  }));

  // Add star click handlers
  for(const td of document.querySelectorAll('td.star')){
    td.addEventListener('click', ()=>{
      const u = td.dataset.url;
      const safeKey = escapeHtml(u || '');
      if(watch.has(u) || watch.has(safeKey)){
        watch.delete(u);
        watch.delete(safeKey);
      } else {
        watch.delete(safeKey);
        watch.add(u);
      }
      saveWatch(watch);
      render();
    });
  }
  updateStats();
}

function refilter(){ currentPage = 1; render(); }

// Initialize page size
(function restorePageSize(){
  const stored = parseInt(localStorage.getItem(PAGE_SIZE_KEY) || '25', 10);
  if([25,50,100].includes(stored)){
    pageSize = stored;
    if(pageSizeSelect) pageSizeSelect.value = String(pageSize);
  } else if(pageSizeSelect){
    pageSizeSelect.value = '25';
  }
})();

// Restore model bar
(function restoreModelBar(){
  try {
    const stored = JSON.parse(localStorage.getItem(MODEL_STORAGE_KEY) || '[]');
    if(Array.isArray(stored) && stored.length){
      currentModels = stored;
      renderModelChips(currentModels);
    }
  } catch {
    renderModelChips([]);
  }
})();
if(!currentModels.length) renderModelChips([]);

// Event listeners
document.addEventListener('DOMContentLoaded', function() {
  // Restore previous results on page load (must be after DOM is ready)
  (function restorePreviousResults(){
    try {
      const savedData = loadResults();
      if (savedData && savedData.items && savedData.items.length > 0) {
        rawItems = savedData.items;
        if (savedData.models && savedData.models.length > 0) {
          currentModels = savedData.models;
          renderModelChips(currentModels);
        }
        currentPage = 1;
        render();
        console.log('Restored', rawItems.length, 'items from previous session');
      } else {
        console.log('No saved results to restore');
      }
    } catch(e) {
      console.error('Failed to restore previous results:', e);
    }
  })();

  // Main fetch button
  if (runBtn) {
    runBtn.addEventListener('click', async ()=>{
      clearAlert();
      if (tbody) tbody.innerHTML = '';
      rows = [];
      rawItems = []; // Clear current results
      clearResults(); // Clear localStorage results
      if (csvBtn) csvBtn.disabled = true;
      if (xlsxBtn) xlsxBtn.disabled = true;
      if (copyBtn) copyBtn.disabled = true;
      if (countBadge) countBadge.classList.add('d-none');

      const urls = (urlsTA?.value || '').trim();
      if(!urls){ showAlert('warning','Please paste at least one URL.'); return; }
      const urlList = urls.split('\n').map(s => s.trim()).filter(Boolean);

      const payload = {
        urls,
        percent_off: parseFloat(percentInput?.value || '0') || 0,
        absolute_off: parseFloat(absOffInput?.value || '0') || 0,
        crawl_pagination: true,
        max_pages: 10  // Reduced from 20 to 10 for faster scraping
      };

      try{
        setLoading(true, urls); // Pass URLs for dynamic loading messages
        const res = await fetch('/api/scrape',{
          method:'POST',
          headers:{'Content-Type':'application/json'},
          body: JSON.stringify(payload)
        });

        let data = null;
        try {
          data = await res.json();
        } catch (parseError) {
          console.error('Failed to parse scrape response as JSON:', parseError);
        }

        if(!res.ok){
          const message = data?.error || `Request failed (${res.status})`;
          throw new Error(message);
        }

        if(!data){
          throw new Error('Empty response from scraper service');
        }
        rawItems = data.items || [];
        currentPage = 1;
        currentModels = deriveModelNames(urlList);
        renderModelChips(currentModels);

        // Price drop alerts
        const mem = loadPriceMem();
        const drop = parseFloat(dropPct?.value || '10') || 10;
        let alerts = [];
        rawItems.forEach(it => {
          const url = it.url;
          const nowFinal = parseMoney(it.discounted_formatted || it.original_formatted);
          if(nowFinal==null) return;
          const prev = mem[url];
          if(prev!=null){
            const downPct = ((prev - nowFinal) / prev) * 100;
            if(downPct >= drop) alerts.push({url, title: it.title, prev, now: nowFinal, downPct: +downPct.toFixed(2)});
          }
          mem[url] = nowFinal;
        });
        savePriceMem(mem);

        // Save results to localStorage for persistence
        saveResults(rawItems, currentModels);

        render();
        if(alerts.length){
          showAlert('success', `🔔 ${alerts.length} price drops detected (≥ ${drop}%). Check the newest results.`);
        } else {
          showAlert('success', `Fetched ${rawItems.length} item(s).`);
        }
      } catch(err){
        console.error('Extractor fetch failed:', err);
        const friendly = err?.message || 'Error fetching results. Check console for details.';
        showAlert('danger', friendly);
        if(!rawItems.length){
          renderModelChips(currentModels);
        }
      } finally {
        setLoading(false);
      }
    });
  }

  // Search and filter handlers
  if (searchInput) searchInput.addEventListener('input', refilter);
  // Note: kwInclude and kwExclude are handled by chip system, not direct input
  [priceMin, priceMax, sortBy, hideDupes, groupModel, showWatchlistOnly].forEach(el => {
    if (el) {
      el.addEventListener('input', refilter);
      el.addEventListener('change', refilter);
    }
  });

  // Pagination handlers
  if(prevPageBtn){
    prevPageBtn.addEventListener('click', ()=>{
      if(currentPage > 1){
        currentPage--;
        render();
      }
    });
  }
  if(nextPageBtn){
    nextPageBtn.addEventListener('click', ()=>{
      const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
      if(currentPage < totalPages){
        currentPage++;
        render();
      }
    });
  }

  if(pageSizeSelect){
    pageSizeSelect.addEventListener('change', ()=>{
      const value = parseInt(pageSizeSelect.value, 10);
      pageSize = Number.isFinite(value) ? value : 25;
      currentPage = 1;
      localStorage.setItem(PAGE_SIZE_KEY, String(pageSize));
      render();
    });
  }

  // Export handlers
  if (csvBtn) {
    csvBtn.addEventListener('click', ()=>{
      const table = csvToRowsVisible();
      const blob = new Blob([toCSV(table)], {type:'text/csv;charset=utf-8;'});
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a'); a.href = url; a.download = 'mobilesentrix_prices.csv'; a.click();
      URL.revokeObjectURL(url);
    });
  }

  if (xlsxBtn) {
    xlsxBtn.addEventListener('click', async ()=>{
      try{
        const res = await fetch('/api/export/xlsx',{
          method:'POST',
          headers:{'Content-Type':'application/json'},
          body: JSON.stringify({rows: lastExportRows})
        });
        const blob = await res.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a'); a.href = url; a.download = 'mobilesentrix_prices.xlsx'; a.click();
        URL.revokeObjectURL(url);
      } catch(err){
        console.error(err);
        showAlert('danger','XLSX export failed');
      }
    });
  }

  if (copyBtn) {
    copyBtn.addEventListener('click', async ()=>{
      try{
        const table = csvToRowsVisible();
        await navigator.clipboard.writeText(toCSV(table));
        showAlert('success','Copied table to clipboard (CSV format).');
      } catch(e){
        showAlert('warning','Copy failed. Use Download CSV instead.');
      }
    });
  }

  // Clear Results handler
  if (clearResultsBtn) {
    clearResultsBtn.addEventListener('click', ()=>{
      const confirmClear = window.confirm('⚠️ Clear all results and reset the extractor?\n\nThis will:\n• Clear the results table\n• Delete saved data from localStorage\n• Reset all filters and settings\n\nThis action cannot be undone.');
      if(!confirmClear) return;
      
      // Clear data
      rawItems = [];
      rows = [];
      currentModels = [];
      compareMap.clear();
      lastExportRows = [];
      
      // Clear localStorage
      clearResults();
      
      // Clear table
      if (tbody) tbody.innerHTML = '';
      
      // Hide export section
      if (exportActions) exportActions.style.display = 'none';
      
      // Disable export buttons
      if (csvBtn) csvBtn.disabled = true;
      if (xlsxBtn) xlsxBtn.disabled = true;
      if (copyBtn) copyBtn.disabled = true;
      
      // Clear model chips
      renderModelChips([]);
      
      // Reset page
      currentPage = 1;
      
      // Show empty state
      if (resultsEmpty) resultsEmpty.style.display = 'block';
      if (resultsFooter) resultsFooter.style.display = 'none';
      
      // Clear alert
      clearAlert();
      
      // Show success message
      showAlert('success', '🧹 Results cleared! Extractor has been reset.');
      
      console.log('🧹 Extractor reset - all data cleared');
    });
  }

  if (exportWatchlistBtn) {
    exportWatchlistBtn.addEventListener('click', ()=>{
      const wl = Array.from(watch);
      const csv = 'url\n' + wl.map(u => `"${u.replaceAll('"','""')}"`).join('\n');
      const blob = new Blob([csv], {type:'text/csv;charset=utf-8;'});
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a'); a.href = url; a.download = 'watchlist.csv'; a.click();
      URL.revokeObjectURL(url);
    });
  }

  if(clearWatchlistBtn){
    clearWatchlistBtn.addEventListener('click', ()=>{
      if(!watch.size) return;
      const confirmClear = window.confirm('Clear all items from your watchlist?');
      if(!confirmClear) return;
      watch.clear();
      saveWatch(watch);
      render();
      showAlert('info','Watchlist cleared.');
    });
  }

  // Theme persistence
  if (window.ThemeManager) {
    ThemeManager.initToggle(darkMode);
  } else if (darkMode) {
    darkMode.addEventListener('change', (e)=>{
      document.documentElement.setAttribute('data-bs-theme', e.target.checked ? 'dark' : 'light');
    });
  }

  // Initial render
  updateStats();
  render();
  
  // Enhanced UI Functionality for Redesigned Controls
  initializeEnhancedUI();
});

/**
 * Initialize enhanced UI functionality for the redesigned control section
 */
function initializeEnhancedUI() {
  const urlsTextarea = $('urls');
  const advancedToggle = $('advancedToggle');
  const advancedControls = $('advancedControls');
  const exportActions = $('exportActions');
  const urlStatus = $('urlStatus');
  const urlCount = $('urlCount');

  startDateTimeTicker();
  
  // Initialize Neo-Minimal UI features
  initializeKeywordChips();
  initializeFileUpload();
  initializePresets();
  initializeFilterCounter();
  initializeFloatingLabels();
  
  // URL monitoring and status updates
  function updateUrlStatus() {
    if (!urlsTextarea) return;
    
    const urls = urlsTextarea.value.trim().split('\n').filter(line => {
      const trimmed = line.trim();
      return trimmed && trimmed.startsWith('http');
    });
    
    const count = urls.length;
    urlCount.textContent = `${count} URL${count !== 1 ? 's' : ''}`;
    
    if (count === 0) {
      urlStatus.textContent = 'Ready';
      urlStatus.style.color = 'var(--muted)';
      advancedToggle.disabled = true;
      advancedToggle.classList.remove('active');
      advancedControls.style.display = 'none';
    } else {
      urlStatus.textContent = 'URLs Detected';
      urlStatus.style.color = 'var(--accent)';
      advancedToggle.disabled = false;
    }
    
    updateFilterCounter();
  }
  
  // Advanced controls toggle
  function toggleAdvancedControls() {
    if (!advancedToggle || !advancedControls) return;
    
    const isVisible = advancedControls.style.display !== 'none';
    
    if (isVisible) {
      advancedControls.style.display = 'none';
      advancedToggle.classList.remove('active');
    } else {
      advancedControls.style.display = 'block';
      advancedToggle.classList.add('active');
    }
  }
  
  // Show export actions after successful fetch
  function showExportActions() {
    if (exportActions) {
      exportActions.style.display = 'block';
    }
  }
  
  // Hide export actions when starting new fetch
  function hideExportActions() {
    if (exportActions) {
      exportActions.style.display = 'none';
    }
  }
  
  // Event listeners
  if (urlsTextarea) {
    urlsTextarea.addEventListener('input', updateUrlStatus);
    urlsTextarea.addEventListener('paste', () => {
      setTimeout(updateUrlStatus, 100); // Allow paste to complete
    });
  }
  
  if (advancedToggle) {
    advancedToggle.addEventListener('click', toggleAdvancedControls);
  }
  
  // Override existing runBtn functionality to integrate with new UI
  if (runBtn) {
    const originalRunHandler = runBtn.onclick;
    runBtn.onclick = async function(e) {
      hideExportActions();
      
      // Call original functionality
      if (originalRunHandler) {
        await originalRunHandler.call(this, e);
      }
      
      // Show export actions after successful fetch
      setTimeout(() => {
        if (rows.length > 0) {
          showExportActions();
        }
      }, 1000);
    };
  }
  
  // Initial status update
  updateUrlStatus();
  
  // Enhanced placeholder rotation for URL textarea
  if (urlsTextarea) {
    const placeholders = [
      "https://www.mobilesentrix.ca/category/...\nhttps://www.mobilesentrix.com/products/...\nhttps://example.com/phones/...",
      "https://store.example.com/iphone-parts\nhttps://parts.example.com/samsung-galaxy\nhttps://wholesale.example.com/screens",
      "https://retailer.com/mobile-accessories\nhttps://supplier.com/phone-batteries\nhttps://distributor.com/phone-cases"
    ];
    
    let placeholderIndex = 0;
    setInterval(() => {
      if (urlsTextarea.value.trim() === '') {
        placeholderIndex = (placeholderIndex + 1) % placeholders.length;
        urlsTextarea.placeholder = placeholders[placeholderIndex];
      }
    }, 8000);
  }
}

/**
 * Initialize keyword chip functionality
 */
function initializeKeywordChips() {
  const includeInput = $('kwInclude');
  const excludeInput = $('kwExclude');
  const includeChips = $('includeChips');
  const excludeChips = $('excludeChips');
  const includeHidden = $('include');
  const excludeHidden = $('exclude');
  
  let includeKeywords = [];
  let excludeKeywords = [];
  
  function createChip(text, onRemove) {
    const chip = document.createElement('span');
    chip.className = 'keyword-chip';
    chip.innerHTML = `
      ${text}
      <span class="remove-chip" title="Remove">×</span>
    `;
    
    chip.querySelector('.remove-chip').addEventListener('click', () => {
      chip.remove();
      onRemove(text);
      updateFilterCounter();
      refilter(); // Trigger refilter when chip is removed
    });
    
    return chip;
  }
  
  function addKeyword(input, keywords, container, hiddenInput) {
    const value = input.value.trim();
    if (value && !keywords.includes(value)) {
      keywords.push(value);
      const chip = createChip(value, (text) => {
        const index = keywords.indexOf(text);
        if (index > -1) keywords.splice(index, 1);
        if (hiddenInput) hiddenInput.value = keywords.join(', ');
      });
      container.appendChild(chip);
      input.value = '';
      if (hiddenInput) hiddenInput.value = keywords.join(', ');
      updateFilterCounter();
      refilter(); // Trigger refilter when chip is added
    }
  }
  
  if (includeInput && includeChips) {
    includeInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        addKeyword(includeInput, includeKeywords, includeChips, includeHidden);
      }
    });
  }
  
  if (excludeInput && excludeChips) {
    excludeInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        addKeyword(excludeInput, excludeKeywords, excludeChips, excludeHidden);
      }
    });
  }
}

/**
 * Initialize drag-and-drop file upload
 */
function initializeFileUpload() {
  const uploadZone = $('uploadZone');
  const fileInput = $('csvUpload');
  const clearBtn = $('clearFileBtn');
  const uploadText = uploadZone?.querySelector('.upload-text');
  const dataIndicator = $('dataIndicator');
  
  if (!uploadZone || !fileInput || !uploadText) return;

  const defaultText = 'Drop CSV/XLSX or click to upload';

  function setFileDisplay(filename, loaded = false) {
    if (!filename) {
      uploadText.textContent = defaultText;
      if (clearBtn) clearBtn.style.display = 'none';
      dataIndicator?.classList.remove('active');
    } else if (loaded) {
      uploadText.textContent = `Loaded: ${filename}`;
      if (clearBtn) clearBtn.style.display = 'block';
      dataIndicator?.classList.add('active');
    } else {
      uploadText.textContent = `Selected: ${filename}`;
      if (clearBtn) clearBtn.style.display = 'block';
    }
    updateFilterCounter();
  }

  async function processComparisonFile(file) {
    if (!file) return;
    const allowedExt = ['csv','txt','xlsx','xlsm','xltx','xltm'];
    const ext = (file.name.split('.').pop() || '').toLowerCase();
    if (!allowedExt.includes(ext)) {
      showAlert('warning', 'Unsupported file type. Please upload a CSV or XLSX file.');
      setFileDisplay('', false);
      fileInput.value = '';
      compareMap = new Map();
      render();
      return;
    }

    compareMap = new Map();
    render();

    // Optimistic UI update
    uploadText.textContent = `Uploading: ${file.name}...`;

    const formData = new FormData();
    formData.append('file', file);

    try {
      const res = await fetch('/api/comparison/upload', {
        method: 'POST',
        body: formData
      });

      const data = await res.json().catch(() => ({}));
      if (!res.ok || data.status !== 'success') {
        throw new Error(data.error || 'Failed to process comparison file.');
      }

      const rows = Array.isArray(data.rows) ? data.rows : [];
      compareMap = new Map();

      rows.forEach(row => {
        const title = (row.title || '').trim();
        if (!title) return;
        let priceValue = row.price;
        if (typeof priceValue === 'string') {
          priceValue = parseMoney(priceValue);
        }
        if (!Number.isFinite(priceValue)) {
          priceValue = parseMoney(String(priceValue ?? ''));
        }
        if (!Number.isFinite(priceValue)) return;

        const titleKey = title.toLowerCase();
        const normKey = norm(title);
        const modelKeyVal = modelKey(title);
        const siteValue = String(row.site || row.source || '').trim().toLowerCase();
        const urlValue = String(row.url || '').trim().toLowerCase();

        const addKey = key => {
          if (!key) return;
          compareMap.set(key, priceValue);
        };

        if (urlValue) addKey(`url:${urlValue}`);
        if (siteValue && titleKey) addKey(`site:${siteValue}:${titleKey}`);
        if (titleKey) addKey(`title:${titleKey}`);
        if (siteValue && modelKeyVal) addKey(`site-model:${siteValue}:${modelKeyVal}`);
        if (normKey) addKey(`norm:${normKey}`);
        if (modelKeyVal) addKey(`model:${modelKeyVal}`);
      });

      currentPage = 1;
      render();
      setFileDisplay(file.name, true);
      showAlert('success', data.message || `Loaded comparison data (${rows.length} rows).`);
    } catch (err) {
      console.error(err);
      compareMap = new Map();
      setFileDisplay('', false);
      fileInput.value = '';
      showAlert('danger', err.message || 'Failed to process comparison file.');
      render();
    }
  }

  // Click to upload
  uploadZone.addEventListener('click', () => fileInput.click());

  // Drag and drop
  uploadZone.addEventListener('dragover', (e) => {
    e.preventDefault();
    uploadZone.classList.add('dragover');
  });

  uploadZone.addEventListener('dragleave', () => {
    uploadZone.classList.remove('dragover');
  });

  uploadZone.addEventListener('drop', (e) => {
    e.preventDefault();
    uploadZone.classList.remove('dragover');

    const files = e.dataTransfer.files;
    if (files.length > 0) {
      fileInput.files = files;
      processComparisonFile(files[0]);
    }
  });

  // File input change
  fileInput.addEventListener('change', (e) => {
    if (e.target.files.length > 0) {
      processComparisonFile(e.target.files[0]);
    }
  });

  // Clear file
  if (clearBtn) {
    clearBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      fileInput.value = '';
      compareMap = new Map();
      setFileDisplay('', false);
      render();
      showAlert('info', 'Comparison data cleared.');
    });
  }

  // Initialize default state
  setFileDisplay('', false);
}

/**
 * Initialize preset functionality
 */
function initializePresets() {
  const presetSelect = $('presetSelect');
  const saveBtn = $('savePresetBtn');
  const loadBtn = $('loadPresetBtn');
  
  const presets = {
    'lcd-imports': {
      percent: 15,
      priceMin: 50,
      priceMax: 500,
      kwInclude: 'LCD, Incell, OEM',
      kwExclude: 'With Frame, Grade B'
    },
    'oem-only': {
      percent: 10,
      kwInclude: 'OEM, Original',
      kwExclude: 'Aftermarket, Copy'
    },
    'refurbished': {
      percent: 20,
      kwInclude: 'Refurbished, Renewed',
      priceMax: 200
    }
  };
  
  if (loadBtn) {
    loadBtn.addEventListener('click', () => {
      const selectedPreset = presetSelect?.value;
      if (selectedPreset && presets[selectedPreset]) {
        const preset = presets[selectedPreset];
        Object.keys(preset).forEach(key => {
          const element = $(key);
          if (element) {
            element.value = preset[key];
            // Trigger change event for reactive updates
            element.dispatchEvent(new Event('input', { bubbles: true }));
          }
        });
        updateFilterCounter();
      }
    });
  }
  
  if (saveBtn) {
    saveBtn.addEventListener('click', () => {
      const name = prompt('Enter preset name:');
      if (name) {
        // Save current filter state
        console.log('Saving preset:', name);
        // Implementation would save to localStorage or server
      }
    });
  }
}

/**
 * Initialize filter counter
 */
function initializeFilterCounter() {
  // Add change listeners to all filter inputs
  const filterInputs = [
    'percent', 'absOff', 'priceMin', 'priceMax', 'dropPct',
    'kwInclude', 'kwExclude', 'sortBy', 'hideDupes', 'groupModel'
  ];
  
  filterInputs.forEach(id => {
    const element = $(id);
    if (element) {
      element.addEventListener('input', updateFilterCounter);
      element.addEventListener('change', updateFilterCounter);
    }
  });
}

/**
 * Update filter counter and indicators
 */
function updateFilterCounter() {
  const filterCount = $('filterCount');
  const indicators = {
    pricing: $('pricingIndicator'),
    keywords: $('keywordsIndicator'),
    display: $('displayIndicator'),
    data: $('dataIndicator')
  };
  
  let activeCount = 0;
  
  // Check pricing filters
  const percent = parseFloat($('percent')?.value || 0);
  const absOff = parseFloat($('absOff')?.value || 0);
  const priceMin = parseFloat($('priceMin')?.value || 0);
  const priceMax = parseFloat($('priceMax')?.value || 0);
  const dropPct = parseFloat($('dropPct')?.value || 10);
  
  if (percent > 0 || absOff > 0 || priceMin > 0 || priceMax > 0 || dropPct !== 10) {
    indicators.pricing?.classList.add('active');
    activeCount++;
  } else {
    indicators.pricing?.classList.remove('active');
  }
  
  // Check keyword filters
  const kwInclude = $('kwInclude')?.value?.trim() || '';
  const kwExclude = $('kwExclude')?.value?.trim() || '';
  
  if (kwInclude || kwExclude) {
    indicators.keywords?.classList.add('active');
    activeCount++;
  } else {
    indicators.keywords?.classList.remove('active');
  }
  
  // Check display options
  const sortBy = $('sortBy')?.value || 'none';
  const hideDupes = $('hideDupes')?.checked || false;
  const groupModel = $('groupModel')?.checked || false;
  
  if (sortBy !== 'none' || hideDupes || groupModel) {
    indicators.display?.classList.add('active');
    activeCount++;
  } else {
    indicators.display?.classList.remove('active');
  }
  
  // Update filter count
  if (filterCount) {
    filterCount.textContent = `${activeCount} filter${activeCount !== 1 ? 's' : ''} active`;
    filterCount.style.display = activeCount > 0 ? 'block' : 'none';
  }
}

/**
 * Initialize floating labels
 */
function initializeFloatingLabels() {
  const compactInputs = document.querySelectorAll('.compact-input');
  
  compactInputs.forEach(input => {
    // Set initial state
    updateFloatingLabel(input);
    
    // Update on focus/blur/input
    input.addEventListener('focus', () => updateFloatingLabel(input));
    input.addEventListener('blur', () => updateFloatingLabel(input));
    input.addEventListener('input', () => updateFloatingLabel(input));
  });
}

function updateFloatingLabel(input) {
  const label = input.nextElementSibling;
  if (!label || !label.classList.contains('floating-label')) return;
  
  if (input.value || input === document.activeElement) {
    label.style.top = '-8px';
    label.style.fontSize = '0.7rem';
    label.style.color = '#00E5FF';
  } else {
    label.style.top = '50%';
    label.style.fontSize = '0.8rem';
    label.style.color = 'rgba(255, 255, 255, 0.6)';
  }
}