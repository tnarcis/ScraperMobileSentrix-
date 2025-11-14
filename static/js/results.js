/**
 * Results Dashboard JavaScript
 * Handles all interactions for the Results dashboard
 */

class ResultsDashboard {
    constructor() {
        this.currentClient = 'mobilesentrix';
        this.currentJobId = null;
        this.currentPage = 1;
        this.itemsPerPage = 50;
        this.storageKeys = {
            pageSize: 'results_page_size'
        };
        this.viewModeStorageKey = 'results_view_mode';
        this.ACTIVE_JOB_STORAGE_KEY = 'results_active_job_id';
        this.filters = {
            changeTypes: ['new', 'price', 'stock', 'description'],
            fromDate: null,
            toDate: null,
            searchQuery: '',
            sortMode: 'recent'
        };
        this.viewMode = this.getStoredViewMode();
        this.latestChanges = [];
        this.lastJobUpdateAt = null;
        this.pendingJobRestoreId = this.getStoredActiveJobId();
        this.categoryCache = new Map();
        this.categorySelections = new Map();
        this.categoryDrawerElements = {};
        this.categoryDrawerFilters = { search: '' };
        this.categoryDrawerOpen = false;
        this.pendingCategorySelection = null;
        this.visibleCategoryList = [];
        this.availableClients = [];
        this.totalRecords = 0;
        this.currentJobSnapshot = null;
    this.autoLimitSummary = null;
        this.firstRunBanner = document.getElementById('firstRunBanner');
        this.firstRunCountEl = document.getElementById('firstRunCount');
    }

    buildProductCell(change) {
        const cell = document.createElement('td');
        cell.className = 'change-table__product title-cell';

        const productMain = this.buildProductMain(change);
        cell.appendChild(productMain);

        const viewUrl = change.product_url || change.url;
        if (viewUrl) {
            const viewLink = document.createElement('a');
            viewLink.className = 'btn btn-sm btn-outline btn-cy change-table__view';
            viewLink.href = viewUrl;
            viewLink.target = '_blank';
            viewLink.rel = 'noopener';
            viewLink.textContent = 'View';
            cell.appendChild(viewLink);
        }

        return cell;
    }

    buildChangeSummaryCell(change) {
        const cell = document.createElement('td');
        cell.className = 'change-table__summary change-type-cell';

        const label = document.createElement('div');
        label.className = 'change-summary__label change-table__summary';
        label.textContent = this.getChangeLabel(change);
        cell.appendChild(label);

        if (change && change.change_type) {
            const typeClass = this.getChangeTypeClass(change.change_type);
            const badge = document.createElement('span');
            badge.className = `change-summary__type change-table__type change-table__type--${typeClass}`;
            badge.textContent = this.formatChangeType(change.change_type);
            cell.appendChild(badge);
        }

        return cell;
    }

    buildValueCell(change, side) {
        const cell = document.createElement('td');
        cell.className = `change-table__value change-table__value--${side} change-values`;

        const resolved = this.resolveChangeValue(change, side);
        const displayText = this.formatResolvedValue(resolved);
        cell.textContent = displayText;
        if (!resolved.isMissing && resolved.text) {
            cell.title = resolved.text;
        }

        if (resolved.isMissing) {
            cell.classList.add('is-missing');
        }

        if (side === 'old') {
            cell.classList.add('old-value');
        } else if (side === 'new') {
            cell.classList.add('new-value');
        }

        return cell;
    }

    buildDeltaCell(change) {
        const cell = document.createElement('td');
        cell.className = 'change-table__delta change-table__difference change-values delta-cell';

        const deltaInfo = this.formatDelta(change);
        if (deltaInfo) {
            cell.textContent = deltaInfo.text;
            if (deltaInfo.direction) {
                cell.classList.add(`change-table__difference--${deltaInfo.direction}`);
            }
            if (deltaInfo.title) {
                cell.title = deltaInfo.title;
            }
        } else {
            cell.textContent = '—';
        }

        return cell;
    }

    buildStockCell(change) {
        const cell = document.createElement('td');
        cell.className = 'change-table__stock stock-cell';
        const isStockChange = (change.change_type || '').toString().toLowerCase() === 'stock';
        const fallbackStock = change.stock_status
            || (change.metadata && (change.metadata.stock_status || change.metadata.stock));

        if (isStockChange) {
            const resolved = this.resolveChangeValue(change, 'new');
            const textValue = this.formatResolvedValue(resolved);
            cell.textContent = textValue;
            if (resolved.text) {
                cell.title = resolved.text;
            }
            if (resolved.isMissing) {
                cell.classList.add('is-missing');
            }
        } else if (fallbackStock) {
            const display = fallbackStock.toString().trim();
            cell.textContent = display || '—';
            if (!display) {
                cell.classList.add('is-missing');
            }
        } else {
            cell.textContent = '—';
            cell.classList.add('is-missing');
        }

        return cell;
    }

    buildTimestampCell(change) {
        const cell = document.createElement('td');
        cell.className = 'change-table__time timestamp-cell';
        const stamp = change.changed_at || change.timestamp;
        cell.textContent = this.formatDateTime(stamp);
        return cell;
    }

    buildProductMain(change) {
        const container = document.createElement('div');
        container.className = 'product-main';

        const viewUrl = change.product_url || change.url;
        const titleEl = document.createElement(viewUrl ? 'a' : 'div');
        titleEl.className = 'product-title change-table__title';
        const productTitle = change.product_title || change.title || 'Untitled product';
        titleEl.textContent = productTitle;
        titleEl.title = productTitle;
        if (viewUrl) {
            titleEl.classList.add('title-link');
            titleEl.href = viewUrl;
            titleEl.target = '_blank';
            titleEl.rel = 'noopener';
        }
        container.appendChild(titleEl);

        const metaPills = this.buildProductMetaPills(change);
        if (metaPills) {
            container.appendChild(metaPills);
        }

        return container;
    }

    buildProductMetaPills(change) {
        const entries = [];
        const cleanedSku = this.formatSku(change.sku);
        if (cleanedSku && cleanedSku !== 'NO SKU') {
            entries.push({ label: 'SKU', value: cleanedSku });
        }

        const category = (change.category || '').toString().trim();
        if (category) {
            entries.push({ label: 'Category', value: category });
        }

        const brand = (change.brand || change.vendor || change.brand_name || '').toString().trim();
        if (brand) {
            entries.push({ label: 'Brand', value: brand });
        }

        const unique = [];
        const seen = new Set();
        entries.forEach((entry) => {
            if (!entry.value) {
                return;
            }
            const key = `${entry.label}|${entry.value}`.toLowerCase();
            if (seen.has(key)) {
                return;
            }
            seen.add(key);
            unique.push(entry);
        });

        if (!unique.length) {
            return null;
        }

        const wrapper = document.createElement('div');
        wrapper.className = 'change-card__meta';
        unique.forEach(({ label, value }) => {
            const pill = document.createElement('div');
            pill.className = 'change-card__meta-pill';
            const labelSpan = document.createElement('span');
            labelSpan.className = 'change-card__meta-label';
            labelSpan.textContent = label;
            const valueSpan = document.createElement('span');
            valueSpan.className = 'change-card__meta-value';
            valueSpan.textContent = value;
            pill.title = value;
            pill.append(labelSpan, valueSpan);
            wrapper.appendChild(pill);
        });

        return wrapper;
    }

    getChangeLabel(change) {
        if (!change) {
            return 'Update';
        }

        if (change.change_label) {
            return change.change_label;
        }

        const type = (change.change_type || '').toString().toLowerCase();

        if (['new', 'product_new', 'new_product'].includes(type)) {
            return 'New product added';
        }

        if (type === 'price') {
            const deltaInfo = this.formatDelta(change);
            if (deltaInfo && deltaInfo.direction === 'up') {
                return 'Price increased';
            }
            if (deltaInfo && deltaInfo.direction === 'down') {
                return 'Price decreased';
            }
            return 'Price changed';
        }

        if (type === 'stock') {
            const oldSource = change.old_value_raw ?? change.old_value ?? '';
            const newSource = change.new_value_raw ?? change.new_value ?? '';
            const oldState = this.normalizeStockState(oldSource);
            const newState = this.normalizeStockState(newSource);

            if (newState === 'in_stock' && oldState !== 'in_stock') {
                return 'Back in stock';
            }
            if (newState === 'out_of_stock' && oldState !== 'out_of_stock') {
                return 'Out of stock';
            }
            if (newState === 'backorder' && oldState !== 'backorder') {
                return 'Backorder status updated';
            }
            return 'Stock status changed';
        }

        if (type === 'description') {
            return 'Description updated';
        }

        return 'Value updated';
    }

    normalizeStockState(value) {
        if (value == null) {
            return '';
        }

        const cleaned = value.toString().trim().toLowerCase();
        if (!cleaned) {
            return '';
        }

        const compact = cleaned.replace(/[^a-z0-9]+/g, ' ');
        const inStockMarkers = ['in stock', 'instock', 'available', 'ready', 'available now'];
        const outStockMarkers = ['out of stock', 'sold out', 'oos', 'unavailable', 'no stock'];
        const backorderMarkers = ['backorder', 'back order', 'preorder', 'pre order', 'back-ordered'];

        if (inStockMarkers.some((marker) => compact.includes(marker))) {
            return 'in_stock';
        }
        if (outStockMarkers.some((marker) => compact.includes(marker))) {
            return 'out_of_stock';
        }
        if (backorderMarkers.some((marker) => compact.includes(marker))) {
            return 'backorder';
        }

        return compact;
    }

    formatDelta(change) {
        if (!change) {
            return null;
        }

        const type = (change.change_type || '').toString().toLowerCase();
        if (type !== 'price') {
            return null;
        }

        let numeric = Number(change.difference);
        if (!Number.isFinite(numeric)) {
            const newResolved = this.resolveChangeValue(change, 'new');
            const oldResolved = this.resolveChangeValue(change, 'old');
            if (Number.isFinite(newResolved.numeric) && Number.isFinite(oldResolved.numeric)) {
                numeric = newResolved.numeric - oldResolved.numeric;
            } else {
                numeric = NaN;
            }
        }

        if (!Number.isFinite(numeric) || Math.abs(numeric) < 1e-9) {
            return null;
        }

        const direction = numeric > 0 ? 'up' : 'down';
        const amount = this.formatCurrency(Math.abs(numeric));
        const text = `${numeric > 0 ? '+' : '-'}${amount}`;
        const title = `Price ${numeric > 0 ? 'increased' : 'decreased'} by ${amount}`;

        return { text, direction, title };
    }

    buildChangesTable(changes) {
        const wrapper = document.createElement('div');
        wrapper.className = 'table-wrapper change-table-wrapper';

        const table = document.createElement('table');
        table.className = 'changes-table change-table';
        table.innerHTML = `
            <thead>
                <tr>
                    <th class="title-cell">Product</th>
                    <th class="change-type-cell">Change</th>
                    <th class="value-cell value-cell--old">Old</th>
                    <th class="value-cell value-cell--new">New</th>
                    <th class="delta-cell">Δ</th>
                    <th class="stock-cell">Stock</th>
                    <th class="timestamp-cell">Timestamp</th>
                </tr>
            </thead>
            <tbody></tbody>
        `;

        const tbody = table.querySelector('tbody');

        changes.forEach((change) => {
            const row = document.createElement('tr');

            row.appendChild(this.buildProductCell(change));
            row.appendChild(this.buildChangeSummaryCell(change));
            row.appendChild(this.buildValueCell(change, 'old'));
            row.appendChild(this.buildValueCell(change, 'new'));
            row.appendChild(this.buildDeltaCell(change));
            row.appendChild(this.buildStockCell(change));
            row.appendChild(this.buildTimestampCell(change));

            tbody.appendChild(row);
        });

        wrapper.appendChild(table);
        return wrapper;
    }

    init() {
        this.bindEvents();
        this.initCategoryControls();
        this.syncViewToggleState();
        this.setupHelpPopovers();
        this.loadDashboard();
        this.restoreActiveJob();
        this.startPolling();
    }

    restorePreferences() {
        const pageSizeSelect = document.getElementById('pageSizeSelect');
        const storedPageSize = this.getStoredPageSize();

        if (storedPageSize) {
            this.itemsPerPage = storedPageSize;
            if (pageSizeSelect) {
                pageSizeSelect.value = String(storedPageSize);
            }
            return;
        }

        if (pageSizeSelect) {
            const parsed = parseInt(pageSizeSelect.value, 10);
            if (Number.isFinite(parsed) && parsed > 0) {
                this.itemsPerPage = parsed;
            }
        }
    }
    
    bindEvents() {
        const clientSelector = document.getElementById('clientSelector');
        if (clientSelector) {
            clientSelector.addEventListener('change', (e) => {
                this.currentClient = e.target.value;
                this.handleClientChange();
            });
        }

        const clientToggle = document.getElementById('clientToggle');
        if (clientToggle) {
            clientToggle.addEventListener('click', (event) => {
                const target = event.target.closest('[data-client-toggle]');
                if (!target) {
                    return;
                }

                const nextClient = target.getAttribute('data-client');
                if (!nextClient || nextClient === this.currentClient) {
                    return;
                }

                this.switchClient(nextClient);
            });
        }

        const runScrapeBtn = document.getElementById('runScrapeBtn');
        if (runScrapeBtn) {
            runScrapeBtn.addEventListener('click', () => {
                this.startScrape();
            });
        }

        const stopScrapeBtn = document.getElementById('stopScrapeBtn');
        if (stopScrapeBtn) {
            stopScrapeBtn.addEventListener('click', () => {
                this.requestStopScrape();
            });
        }

        const exportBtn = document.getElementById('exportBtn');
        if (exportBtn) {
            exportBtn.addEventListener('click', () => {
                this.exportChanges();
            });
        }

        const applyFiltersBtn = document.getElementById('applyFiltersBtn');
        if (applyFiltersBtn) {
            applyFiltersBtn.addEventListener('click', () => {
                this.applyFilters();
            });
        }

        const searchInput = document.getElementById('searchInput');
        if (searchInput) {
            searchInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    this.applyFilters();
                }
            });
        }

        const pageSizeSelect = document.getElementById('pageSizeSelect');
        if (pageSizeSelect) {
            pageSizeSelect.addEventListener('change', (e) => {
                const nextValue = parseInt(e.target.value, 10);
                if (!Number.isFinite(nextValue) || nextValue <= 0) {
                    return;
                }
                this.itemsPerPage = nextValue;
                this.persistPageSize(nextValue);
                this.currentPage = 1;
                this.loadRecentChanges();
            });
        }

        const sortSelect = document.getElementById('sortSelect');
        if (sortSelect) {
            sortSelect.addEventListener('change', (e) => {
                this.filters.sortMode = e.target.value || 'recent';
                this.currentPage = 1;
                this.loadRecentChanges();
            });
        }

        const viewCardsBtn = document.getElementById('viewCardsBtn');
        const viewListBtn = document.getElementById('viewListBtn');
        if (viewCardsBtn && viewListBtn) {
            viewCardsBtn.addEventListener('click', () => this.setViewMode('cards'));
            viewListBtn.addEventListener('click', () => this.setViewMode('list'));
        }

        const prevPageBtn = document.getElementById('prevPageBtn');
        if (prevPageBtn) {
            prevPageBtn.addEventListener('click', () => {
                if (this.currentPage > 1) {
                    this.currentPage--;
                    this.loadRecentChanges();
                }
            });
        }

        const nextPageBtn = document.getElementById('nextPageBtn');
        if (nextPageBtn) {
            nextPageBtn.addEventListener('click', () => {
                this.currentPage++;
                this.loadRecentChanges();
            });
        }

        const toggleJobModalBtn = document.getElementById('toggleJobModalBtn');
        if (toggleJobModalBtn) {
            toggleJobModalBtn.addEventListener('click', () => {
                this.toggleJobModal();
            });
        }

        const modalCloseBtn = document.getElementById('modalCloseBtn');
        if (modalCloseBtn) {
            modalCloseBtn.addEventListener('click', () => {
                this.closeModal();
            });
        }

        const darkModeToggle = document.getElementById('darkMode');
        if (window.ThemeManager) {
            ThemeManager.initToggle(darkModeToggle);
        } else if (darkModeToggle) {
            darkModeToggle.addEventListener('change', (event) => {
                document.documentElement.setAttribute('data-bs-theme', event.target.checked ? 'dark' : 'light');
            });
        }
    }

    initCategoryControls() {
        this.ensureCategorySelectionStore(this.currentClient);

        const elements = {
            drawer: document.getElementById('categoryDrawer'),
            backdrop: document.getElementById('categoryDrawerBackdrop'),
            closeBtn: document.getElementById('categoryDrawerCloseBtn'),
            pickerBtn: document.getElementById('categoryPickerBtn'),
            list: document.getElementById('categoryList'),
            search: document.getElementById('categorySearchInput'),
            loading: document.getElementById('categoryLoading'),
            error: document.getElementById('categoryError'),
            selectAll: document.getElementById('categorySelectAllBtn'),
            clear: document.getElementById('categoryClearBtn'),
            apply: document.getElementById('categoryApplyBtn'),
            summary: document.getElementById('categorySummary'),
            selectionCount: document.getElementById('categorySelectionCount'),
            availableCount: document.getElementById('categoryAvailableCount')
        };

        this.categoryDrawerElements = elements;

        if (!elements.drawer || !elements.pickerBtn) {
            this.updateCategorySummary();
            return;
        }

        elements.pickerBtn.addEventListener('click', () => this.openCategoryDrawer());
        if (elements.closeBtn) {
            elements.closeBtn.addEventListener('click', () => this.closeCategoryDrawer());
        }
        if (elements.backdrop) {
            elements.backdrop.addEventListener('click', () => this.closeCategoryDrawer());
        }
        if (elements.apply) {
            elements.apply.addEventListener('click', () => this.applyCategorySelection());
        }
        if (elements.clear) {
            elements.clear.addEventListener('click', () => this.clearPendingCategorySelection());
        }
        if (elements.selectAll) {
            elements.selectAll.addEventListener('click', () => this.selectAllVisibleCategories());
        }
        if (elements.search) {
            elements.search.addEventListener('input', (event) => {
                this.categoryDrawerFilters.search = event.target.value || '';
                this.renderCategoryList();
            });
        }

        document.addEventListener('keydown', (event) => {
            if (event.key === 'Escape' && this.categoryDrawerOpen) {
                this.closeCategoryDrawer();
            }
        });

        this.updateCategorySummary();
    }

    switchClient(nextClient) {
        if (!nextClient || nextClient === this.currentClient) {
            return;
        }

        this.currentClient = nextClient;

        const clientSelector = document.getElementById('clientSelector');
        if (clientSelector) {
            clientSelector.value = nextClient;
        }

        this.syncClientToggle(nextClient);
        this.handleClientChange();
    }

    handleClientChange() {
        if (this.categoryDrawerOpen) {
            this.closeCategoryDrawer();
        }
        this.ensureCategorySelectionStore(this.currentClient);
        this.syncClientToggle(this.currentClient);
        this.updateCategorySummary();
        this.loadDashboard();
    }

    ensureCategorySelectionStore(client) {
        const targetClient = client || this.currentClient;
        if (!this.categorySelections.has(targetClient)) {
            this.categorySelections.set(targetClient, new Map());
        }
        return this.categorySelections.get(targetClient);
    }

    getSelectedCategories(client = this.currentClient) {
        const store = this.ensureCategorySelectionStore(client);
        return Array.from(store.values());
    }

    setSelectedCategories(client, urls) {
        const targetClient = client || this.currentClient;
        const nextStore = new Map();

        (urls || []).forEach((value) => {
            if (typeof value !== 'string') {
                return;
            }
            const trimmed = value.trim();
            if (!trimmed) {
                return;
            }
            nextStore.set(this.normalizeCategoryUrl(trimmed), trimmed);
        });

        this.categorySelections.set(targetClient, nextStore);
    }

    async openCategoryDrawer() {
        const { drawer, search } = this.categoryDrawerElements;
        if (!drawer) {
            return;
        }

        if (this.categoryDrawerOpen) {
            return;
        }

        this.categoryDrawerOpen = true;
        drawer.classList.add('is-open');
        drawer.removeAttribute('hidden');
        document.body.classList.add('category-drawer-open');

        this.pendingCategorySelection = new Map(this.ensureCategorySelectionStore(this.currentClient));
        this.categoryDrawerFilters.search = '';
        if (search) {
            search.value = '';
        }

        await this.prepareCategoryDrawer();
        if (search) {
            search.focus();
        }
    }

    closeCategoryDrawer() {
        const { drawer, search } = this.categoryDrawerElements;
        if (!drawer) {
            return;
        }

        if (!this.categoryDrawerOpen && drawer.hasAttribute('hidden')) {
            return;
        }

        drawer.classList.remove('is-open');
        drawer.setAttribute('hidden', '');
        document.body.classList.remove('category-drawer-open');
        this.categoryDrawerOpen = false;
        this.pendingCategorySelection = null;
        this.visibleCategoryList = [];
        this.categoryDrawerFilters.search = '';
        if (search) {
            search.value = '';
        }
        this.toggleCategoryLoading(false);
        this.setCategoryError('');

        if (this.categoryDrawerElements.pickerBtn) {
            this.categoryDrawerElements.pickerBtn.focus();
        }
    }

    async prepareCategoryDrawer() {
        await this.ensureCategoriesLoaded(this.currentClient);
        this.renderCategoryList();
        this.updateCategorySelectionMeta();
    }

    async ensureCategoriesLoaded(client) {
        const cached = this.categoryCache.get(client);
        if (Array.isArray(cached)) {
            this.updateCategoryCounts(cached.length);
            return cached;
        }

        this.toggleCategoryLoading(true);
        this.setCategoryError('');

        try {
            const params = new URLSearchParams({ client });
            const response = await fetch(`/api/scrape/categories?${params.toString()}`);
            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || 'Failed to load categories');
            }

            const categories = Array.isArray(data.categories) ? data.categories : [];
            this.categoryCache.set(client, categories);
            this.updateCategoryCounts(categories.length);
            if (data.warning) {
                this.setCategoryError(data.warning);
            }
            return categories;
        } catch (error) {
            console.error('Error loading categories:', error);
            this.setCategoryError(error.message || 'Unable to load categories.');
            return [];
        } finally {
            this.toggleCategoryLoading(false);
        }
    }

    getCachedCategories(client = this.currentClient) {
        return this.categoryCache.get(client) || [];
    }

    renderCategoryList() {
        const { list } = this.categoryDrawerElements;
        if (!list) {
            return;
        }

        const categories = this.getCachedCategories(this.currentClient);
        const searchTerm = (this.categoryDrawerFilters.search || '').trim().toLowerCase();

        const filtered = categories.filter((category) => {
            const tokens = [
                category.label_text,
                category.brand,
                category.url
            ].map((value) => (value || '').toString().toLowerCase());
            if (!searchTerm) {
                return true;
            }
            return tokens.some((token) => token.includes(searchTerm));
        });

        this.visibleCategoryList = filtered;
        list.innerHTML = '';

        if (!filtered.length) {
            const emptyState = document.createElement('div');
            emptyState.className = 'category-empty';
            emptyState.textContent = categories.length
                ? 'No categories match your search.'
                : 'No categories discovered yet.';
            list.appendChild(emptyState);
            this.updateCategorySelectionMeta();
            return;
        }

        const groups = new Map();
        filtered.forEach((category) => {
            const rawBrand = category.brand ? category.brand.toString().trim() : '';
            const brand = rawBrand ? this.capitalizeWords(rawBrand) : 'General';
            if (!groups.has(brand)) {
                groups.set(brand, []);
            }
            groups.get(brand).push(category);
        });

        const brandNames = Array.from(groups.keys()).sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
        const activeSelection = this.pendingCategorySelection || this.ensureCategorySelectionStore(this.currentClient);

        brandNames.forEach((brand) => {
            const section = document.createElement('div');
            section.className = 'category-group';

            const title = document.createElement('div');
            title.className = 'category-group__title';
            title.textContent = brand;
            section.appendChild(title);

            groups.get(brand).forEach((category) => {
                const option = document.createElement('label');
                option.className = 'category-option';
                option.setAttribute('role', 'option');
                option.dataset.categoryUrl = category.url;

                const checkbox = document.createElement('input');
                checkbox.type = 'checkbox';
                checkbox.className = 'form-check-input';
                checkbox.dataset.categoryUrl = category.url;
                const isSelected = activeSelection.has(this.normalizeCategoryUrl(category.url));
                checkbox.checked = isSelected;
                option.setAttribute('aria-selected', String(isSelected));
                checkbox.addEventListener('change', (event) => {
                    this.togglePendingCategory(category.url, event.target.checked);
                    option.setAttribute('aria-selected', String(event.target.checked));
                });

                const details = document.createElement('div');
                details.className = 'category-option__details';

                const labelText = document.createElement('div');
                labelText.className = 'category-option__label';
                const friendly = category.label_text || this.prettyCategoryLabel(category.url);
                labelText.textContent = friendly;
                details.appendChild(labelText);

                const meta = document.createElement('div');
                meta.className = 'category-option__meta';
                const hostname = (() => {
                    try {
                        return new URL(category.url).hostname.replace(/^www\./, '');
                    } catch (error) {
                        return category.url;
                    }
                })();
                const discovered = category.discovered_at ? ` • Added ${category.discovered_at}` : '';
                meta.textContent = `${hostname}${discovered}`;
                details.appendChild(meta);

                option.appendChild(checkbox);
                option.appendChild(details);
                section.appendChild(option);
            });

            list.appendChild(section);
        });

        this.updateCategorySelectionMeta();
    }

    togglePendingCategory(url, shouldSelect) {
        if (!url) {
            return;
        }

        const normalized = this.normalizeCategoryUrl(url);
        if (!normalized) {
            return;
        }

        const working = this.pendingCategorySelection
            ? this.pendingCategorySelection
            : new Map(this.ensureCategorySelectionStore(this.currentClient));

        if (shouldSelect) {
            working.set(normalized, url);
        } else {
            working.delete(normalized);
        }

        this.pendingCategorySelection = working;
        this.updateCategorySelectionMeta();
    }

    selectAllVisibleCategories() {
        const source = this.visibleCategoryList && this.visibleCategoryList.length
            ? this.visibleCategoryList
            : this.getCachedCategories(this.currentClient);

        if (!source.length) {
            return;
        }

        const working = this.pendingCategorySelection
            ? this.pendingCategorySelection
            : new Map(this.ensureCategorySelectionStore(this.currentClient));

        source.forEach((category) => {
            if (!category || !category.url) {
                return;
            }
            working.set(this.normalizeCategoryUrl(category.url), category.url);
        });

        this.pendingCategorySelection = working;
        this.renderCategoryList();
    }

    clearPendingCategorySelection() {
        this.pendingCategorySelection = new Map();
        this.renderCategoryList();
    }

    updateCategorySelectionMeta() {
        const { selectionCount, availableCount } = this.categoryDrawerElements;
        const sourceMap = this.pendingCategorySelection || this.ensureCategorySelectionStore(this.currentClient);
        const selectedCount = sourceMap.size;

        if (selectionCount) {
            selectionCount.textContent = `${selectedCount} selected`;
        }

        if (availableCount) {
            const totalAvailable = this.getCachedCategories(this.currentClient).length;
            availableCount.textContent = `${totalAvailable} available`;
        }
    }

    updateCategoryCounts(total) {
        const { availableCount } = this.categoryDrawerElements;
        if (availableCount) {
            availableCount.textContent = `${total} available`;
        }
    }

    toggleCategoryLoading(isLoading) {
        const { loading } = this.categoryDrawerElements;
        if (!loading) {
            return;
        }
        if (isLoading) {
            loading.removeAttribute('hidden');
        } else {
            loading.setAttribute('hidden', '');
        }
    }

    setCategoryError(message) {
        const { error } = this.categoryDrawerElements;
        if (!error) {
            return;
        }
        if (message) {
            error.textContent = message;
            error.removeAttribute('hidden');
        } else {
            error.textContent = '';
            error.setAttribute('hidden', '');
        }
    }

    applyCategorySelection() {
        const activeMap = this.pendingCategorySelection
            ? new Map(this.pendingCategorySelection)
            : new Map(this.ensureCategorySelectionStore(this.currentClient));

        this.categorySelections.set(this.currentClient, activeMap);
        this.pendingCategorySelection = null;
        this.updateCategorySummary();
        this.closeCategoryDrawer();
    }

    updateCategorySummary() {
        const { summary } = this.categoryDrawerElements;
        if (!summary) {
            return;
        }

        const selected = this.getSelectedCategories(this.currentClient);
        summary.title = selected.length ? '' : 'Scraping all categories';

        if (!selected.length) {
            if (this.autoLimitSummary && this.autoLimitSummary.total > this.autoLimitSummary.limit) {
                const { limit, total } = this.autoLimitSummary;
                summary.textContent = `Auto cap ${limit}/${total}`;
                summary.title = `Automatically limited to the first ${limit} of ${total} discovered categories. Use the picker to override.`;
                summary.classList.add('has-selection');
            } else {
                summary.textContent = 'Scraping all categories';
                summary.classList.remove('has-selection');
                summary.title = 'Scraping all categories';
            }
            return;
        }

        const preview = selected.slice(0, 2).map((url) => this.prettyCategoryLabel(url)).join(', ');
        const remainder = selected.length > 2 ? ` +${selected.length - 2} more` : '';
        summary.textContent = `${preview}${remainder}`;
        summary.title = selected.join('\n');
        summary.classList.add('has-selection');
    }

    normalizeCategoryUrl(url) {
        if (!url || typeof url !== 'string') {
            return '';
        }
        return url.trim().replace(/\/+$/, '').toLowerCase();
    }

    prettyCategoryLabel(url) {
        if (!url) {
            return '';
        }

        try {
            const parsed = new URL(url, window.location.origin);
            const parts = parsed.pathname.split('/').filter(Boolean);
            if (!parts.length) {
                return parsed.hostname.replace(/^www\./, '');
            }
            const last = parts[parts.length - 1];
            return this.capitalizeWords(last.replace(/[-_]+/g, ' '));
        } catch (error) {
            const segments = url.split('/').filter(Boolean);
            const lastSegment = segments.length ? segments[segments.length - 1] : url;
            return this.capitalizeWords(lastSegment.replace(/[-_]+/g, ' '));
        }
    }

    syncJobCategorySelection(jobData) {
        if (!jobData || !jobData.job_id) {
            return;
        }
        const config = jobData.config || {};
        const selected = Array.isArray(config.selected_categories) ? config.selected_categories : [];
        const targetClient = jobData.client || this.currentClient;
        this.setSelectedCategories(targetClient, selected);
        if (targetClient === this.currentClient) {
            this.updateCategorySummary();
        }
    }

    updateRunMeta(job) {
        const metaEl = document.getElementById('runMetaInfo');
        if (!metaEl) {
            return;
        }

        if (!job || !job.job_id) {
            this.autoLimitSummary = null;
            this.updateCategorySummary();
            metaEl.hidden = false;
            metaEl.textContent = 'No active scrape. Start a new run to see live progress.';
            this.updateProgressToggleState(null);
            return;
        }

        const config = job.config || {};
        const selectedCount = Array.isArray(config.selected_categories) ? config.selected_categories.length : 0;
        const autoLimit = Number(config.category_auto_limit);
        const autoTotal = Number(config.category_auto_total);
        if (
            Number.isFinite(autoLimit)
            && autoLimit > 0
            && Number.isFinite(autoTotal)
            && autoTotal > autoLimit
        ) {
            this.autoLimitSummary = { limit: autoLimit, total: autoTotal };
        } else {
            this.autoLimitSummary = null;
        }
        const scopeText = selectedCount > 0
            ? `Selected ${selectedCount} categor${selectedCount === 1 ? 'y' : 'ies'}`
            : 'All categories';

        const maxPages = config.max_pages;
        let depthText = 'full depth';
        if (typeof maxPages === 'number' && Number.isFinite(maxPages) && maxPages > 0) {
            depthText = `${maxPages} pages`; 
        } else if (typeof maxPages === 'string' && maxPages !== 'unlimited') {
            depthText = `${maxPages} pages`;
        }

        const timing = this.computeJobTiming(job);
        const statusLabel = this.formatJobStatus(job);
        const clientLabel = this.formatClientLabel(job.client || this.currentClient);
        const progressParts = [];

        if (
            Number.isFinite(job.categories_done)
            && Number.isFinite(job.total_categories)
            && job.total_categories > 0
        ) {
            progressParts.push(`${job.categories_done}/${job.total_categories} categories`);
        } else if (Number.isFinite(job.total_categories) && job.total_categories > 0) {
            progressParts.push(`${job.total_categories} categories queued`);
        } else {
            progressParts.push(scopeText);
        }

        if (depthText === 'full depth') {
            progressParts.push('Full depth');
        } else if (depthText) {
            progressParts.push(`Max ${depthText}`);
        }

        if (Number.isFinite(job.pages_done) && job.pages_done > 0) {
            progressParts.push(`${job.pages_done} pages processed`);
        }

        if (Number.isFinite(job.items_found) && job.items_found > 0) {
            progressParts.push(`${this.formatNumber(job.items_found)} items captured`);
        }

        if (this.autoLimitSummary) {
            progressParts.push(`Auto cap ${this.autoLimitSummary.limit}/${this.autoLimitSummary.total}`);
        }

        if (timing.remainingMs != null && timing.remainingMs > 0) {
            progressParts.push(`ETA ${timing.remainingText}`);
        } else if (timing.remainingMs === 0 && job.completed_at) {
            progressParts.push(`Finished ${this.formatRelativeTime(job.completed_at)}`);
        } else if (job.started_at) {
            progressParts.push(`Started ${this.formatRelativeTime(job.started_at)}`);
        }

        const jobLabel = job.job_id.toString().slice(0, 8);
        const prefix = `${statusLabel} ${clientLabel} scrape`;
        metaEl.textContent = `${prefix} · Job ${jobLabel} · ${progressParts.join(' · ')}`;
        metaEl.hidden = false;

        this.updateProgressToggleState(job);
        this.updateCategorySummary();
    }

    formatJobStatus(job) {
        if (!job) {
            return 'Unknown';
        }

        if (job.cancel_requested && job.status === 'running') {
            return 'Cancelling';
        }

        const status = (job.status || '').toString().toLowerCase();
        switch (status) {
            case 'queued':
                return 'Queued';
            case 'running':
                return 'Running';
            case 'done':
                return 'Completed';
            case 'cancelled':
                return 'Cancelled';
            case 'error':
            case 'failed':
                return 'Failed';
            default:
                return this.capitalizeWords(status || 'Unknown');
        }
    }

    updateProgressToggleState(job) {
        const toggleBtn = document.getElementById('toggleJobModalBtn');
        if (!toggleBtn) {
            return;
        }
        const hasJob = Boolean(job && job.job_id);
        toggleBtn.disabled = !hasJob;
        toggleBtn.setAttribute('aria-disabled', String(!hasJob));
    }

    async loadDashboard() {
        try {
            await this.loadSummary();
        } catch (error) {
            console.error('Error loading dashboard summary:', error);
        }

        try {
            await this.loadRecentChanges();
        } catch (error) {
            console.error('Error loading recent changes:', error);
        }
    }
    
    async loadSummary() {
        try {
            const params = new URLSearchParams({ client: this.currentClient });
            const response = await fetch(`/api/results/summary?${params.toString()}`);
            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || 'Failed to load summary');
            }

            const activeClient = data.active_client || this.currentClient;
            this.currentClient = activeClient;

            if (Array.isArray(data.clients)) {
                this.populateClientSelector(data.clients, activeClient);
            }

            this.ensureCategorySelectionStore(this.currentClient);
            this.updateTotals(data.totals || {});
            this.updateCategoryProgress(data.categories || {});
            this.updateRunInfo(data.runs || {});
            this.syncJobCategorySelection(data.job || null);
            this.updateRunMeta(data.job || null);
            this.updateCategorySummary();

            if (data.job) {
                this.updateJobStatus(data.job);
                this.updateLiveIndicators(data.job);

                const status = (data.job.status || '').toLowerCase();
                const isRunning = ['running', 'queued', 'cancelling'].includes(status);
                this.currentJobId = isRunning ? data.job.job_id : null;

                if (isRunning && data.job.job_id) {
                    this.persistActiveJobId(data.job.job_id);
                    this.pollJobStatusSilent();
                } else {
                    this.clearStoredActiveJobId();
                }
            }
        } catch (error) {
            console.error('Error loading summary:', error);
        }
    }

    async loadRecentChanges() {
        try {
            const params = new URLSearchParams({
                client: this.currentClient,
                limit: this.itemsPerPage,
                offset: (this.currentPage - 1) * this.itemsPerPage
            });

            this.filters.changeTypes.forEach((type) => {
                params.append('change_types', type);
            });

            if (this.filters.fromDate) {
                params.set('from', this.filters.fromDate);
            }

            if (this.filters.toDate) {
                params.set('to', this.filters.toDate);
            }

            if (this.filters.searchQuery) {
                params.set('q', this.filters.searchQuery);
            }

            const response = await fetch(`/api/results/recent?${params.toString()}`);
            const payload = await response.json();

            if (!response.ok) {
                throw new Error(payload.error || 'Failed to load recent changes');
            }

            const items = Array.isArray(payload.items)
                ? payload.items
                : Array.isArray(payload.changes)
                    ? payload.changes
                    : [];
            const total = typeof payload.total === 'number' ? payload.total : items.length;

            const sortedItems = this.sortChanges(items.slice());
            this.latestChanges = sortedItems;

            this.totalRecords = total;
            this.updateChangesGrid(sortedItems);
            this.updatePagination({ total, changes: sortedItems });
        } catch (error) {
            console.error('Error loading recent changes:', error);
            this.showError('Failed to load recent changes: ' + error.message);
        }
    }

    toggleFirstRunBanner(isVisible, count = 0) {
        if (!this.firstRunBanner) {
            return;
        }

        if (isVisible) {
            if (this.firstRunCountEl) {
                this.firstRunCountEl.textContent = Number(count).toLocaleString();
            }
            this.firstRunBanner.classList.remove('d-none');
        } else {
            this.firstRunBanner.classList.add('d-none');
        }
    }

    updateChangesGrid(changes) {
        const grid = document.getElementById('changesGrid');
        if (!grid) {
            return;
        }

        grid.classList.toggle('is-list', this.viewMode === 'list');
        grid.innerHTML = '';

        this.updateChangeSummary(changes);

        const isBaselineOnly = Array.isArray(changes)
            && changes.length > 0
            && changes.every((change) => change.is_baseline);
        this.toggleFirstRunBanner(isBaselineOnly, Array.isArray(changes) ? changes.length : 0);

        if (!Array.isArray(changes) || changes.length === 0) {
            const emptyState = document.createElement('div');
            emptyState.className = 'results-empty';
            emptyState.innerHTML = `
                <span class="emoji">📊</span>
                <p>No recent changes found. Start a scraping job to see data here.</p>
            `;
            grid.appendChild(emptyState);
            return;
        }

        if (this.viewMode === 'list') {
            grid.appendChild(this.buildChangesTable(changes));
            return;
        }

        const fragment = document.createDocumentFragment();

        changes.forEach((change) => {
            fragment.appendChild(this.buildChangeCard(change));
        });

        grid.appendChild(fragment);
    }

    updateChangeSummary(changes) {
        const container = document.getElementById('changesSummary');
        if (!container) {
            return;
        }

        if (!Array.isArray(changes) || changes.length === 0) {
            container.hidden = true;
            container.innerHTML = '';
            return;
        }

        const totalChanges = changes.length;
        const filterSummary = this.describeActiveFilters();

        container.hidden = false;
        container.innerHTML = `
            <div class="changes-summary__main">
                <div class="changes-summary__total">${totalChanges.toLocaleString()} change${totalChanges === 1 ? '' : 's'} shown</div>
                <div class="changes-summary__filters">${filterSummary}</div>
            </div>
        `;
    }

    describeActiveFilters() {
        const parts = [];

        if (Array.isArray(this.filters.changeTypes) && this.filters.changeTypes.length) {
            const defaultTypes = ['new', 'price', 'stock', 'description'];
            const normalizedSelected = [...this.filters.changeTypes]
                .map((type) => type.toString().toLowerCase())
                .sort();
            const normalizedDefault = [...defaultTypes].sort();
            const allTypesSelected = normalizedSelected.length === normalizedDefault.length
                && normalizedSelected.every((type, idx) => type === normalizedDefault[idx]);

            if (allTypesSelected) {
                parts.push('All change types');
            } else {
                const formatted = this.filters.changeTypes
                    .map((type) => this.formatChangeType(type))
                    .join(', ');
                parts.push(formatted);
            }
        }

        if (this.filters.fromDate || this.filters.toDate) {
            const from = this.filters.fromDate ? this.filters.fromDate : 'start';
            const to = this.filters.toDate ? this.filters.toDate : 'now';
            parts.push(`Date window ${from} → ${to}`);
        }

        if (this.filters.searchQuery) {
            parts.push(`Matches “${this.truncateText(this.filters.searchQuery, 40)}”`);
        }

        if (!parts.length) {
            return 'No filters applied';
        }

        return parts.join(' • ');
    }

    setViewMode(mode) {
        const normalized = mode === 'list' ? 'list' : 'cards';
        if (this.viewMode === normalized) {
            this.syncViewToggleState();
            return;
        }
        this.viewMode = normalized;
        this.persistViewMode(normalized);
        this.syncViewToggleState();
        this.updateChangesGrid(this.latestChanges);
    }

    syncViewToggleState() {
        const cardsBtn = document.getElementById('viewCardsBtn');
        const listBtn = document.getElementById('viewListBtn');
        const isList = this.viewMode === 'list';

        if (cardsBtn) {
            cardsBtn.classList.toggle('active', !isList);
            cardsBtn.setAttribute('aria-pressed', String(!isList));
        }

        if (listBtn) {
            listBtn.classList.toggle('active', isList);
            listBtn.setAttribute('aria-pressed', String(isList));
        }

        const toggle = document.getElementById('viewToggle');
        if (toggle) {
            toggle.setAttribute('data-view-mode', this.viewMode);
        }
    }

    syncClientToggle(client) {
        const toggle = document.getElementById('clientToggle');
        if (!toggle) {
            return;
        }

        const targetClient = client || this.currentClient;
        const buttons = toggle.querySelectorAll('[data-client-toggle]');
        buttons.forEach((button) => {
            const isActive = button.getAttribute('data-client') === targetClient;
            button.classList.toggle('is-active', isActive);
            button.setAttribute('aria-pressed', String(isActive));
        });
    }

    buildChangeCard(change) {
        const typeClass = this.getChangeTypeClass(change.change_type);
        const card = document.createElement('article');
        card.className = `change-card change-card--${typeClass}`;
        const header = document.createElement('div');
        header.className = 'change-card__header';

        const badge = document.createElement('span');
        badge.className = `change-card__badge change-card__badge--${typeClass}`;
        badge.textContent = this.formatChangeType(change.change_type);
        header.appendChild(badge);

        const headerMeta = document.createElement('div');
        headerMeta.className = 'change-card__header-meta';

        const timestamp = document.createElement('time');
        timestamp.className = 'change-card__timestamp';
        const stamp = change.changed_at || change.timestamp;
        if (stamp) {
            timestamp.dateTime = stamp;
        }
        timestamp.textContent = this.formatDateTime(stamp);
        headerMeta.appendChild(timestamp);

        const deltaInfo = this.formatDelta(change);
        if (deltaInfo) {
            const deltaBadge = document.createElement('span');
            deltaBadge.className = 'change-card__delta';
            if (deltaInfo.direction) {
                deltaBadge.classList.add(`change-card__delta--${deltaInfo.direction}`);
            }
            deltaBadge.textContent = deltaInfo.text;
            if (deltaInfo.title) {
                deltaBadge.title = deltaInfo.title;
            }
            headerMeta.appendChild(deltaBadge);
        }

        header.appendChild(headerMeta);
        card.appendChild(header);

        const body = document.createElement('div');
        body.className = 'change-card__body';

        const productBlock = this.buildProductMain(change);
        productBlock.classList.add('change-card__product');
        body.appendChild(productBlock);

        const summary = document.createElement('div');
        summary.className = 'change-summary';

        const changeLabel = document.createElement('div');
        changeLabel.className = 'change-label';
        changeLabel.textContent = this.getChangeLabel(change);
        summary.appendChild(changeLabel);

        const valuesGroup = document.createElement('div');
        valuesGroup.className = 'change-values';

        const oldResolved = this.resolveChangeValue(change, 'old');
        const newResolved = this.resolveChangeValue(change, 'new');

        const oldSpan = document.createElement('span');
        oldSpan.className = 'change-values__item change-values__item--old';
        oldSpan.textContent = `Old: ${this.formatResolvedValue(oldResolved)}`;
        valuesGroup.appendChild(oldSpan);

        const newSpan = document.createElement('span');
        newSpan.className = 'change-values__item change-values__item--new';
        newSpan.textContent = `New: ${this.formatResolvedValue(newResolved)}`;
        valuesGroup.appendChild(newSpan);

        if (deltaInfo) {
            const deltaSpan = document.createElement('span');
            deltaSpan.className = 'change-values__item change-values__item--delta';
            deltaSpan.textContent = deltaInfo.text;
            valuesGroup.appendChild(deltaSpan);
        }

        summary.appendChild(valuesGroup);
        body.appendChild(summary);

        card.appendChild(body);

        const footer = document.createElement('div');
        footer.className = 'change-card__footer';
        let footerHasContent = false;

        if (change.product_url || change.url) {
            const viewBtn = document.createElement('a');
            viewBtn.className = 'btn btn-outline btn-cy btn-sm';
            viewBtn.textContent = 'View';
            viewBtn.href = change.product_url || change.url;
            viewBtn.target = '_blank';
            viewBtn.rel = 'noopener';
            footer.appendChild(viewBtn);
            footerHasContent = true;
        }

        if (footerHasContent) {
            card.appendChild(footer);
        }

        return card;
    }
    
    updatePagination(data) {
        const totalRecords = typeof data.total === 'number' && data.total >= 0 ? data.total : null;
        const totalPages = totalRecords !== null ? Math.max(1, Math.ceil(totalRecords / this.itemsPerPage)) : this.currentPage;

        const pageInfo = document.getElementById('pageInfo');
        if (pageInfo) {
            pageInfo.textContent = totalRecords !== null
                ? `Page ${this.currentPage} of ${totalPages}`
                : `Page ${this.currentPage}`;
        }

        const prevPageBtn = document.getElementById('prevPageBtn');
        if (prevPageBtn) {
            prevPageBtn.disabled = this.currentPage <= 1;
        }

        const nextPageBtn = document.getElementById('nextPageBtn');
        if (nextPageBtn) {
            const reachedLastPage = totalRecords !== null
                ? this.currentPage >= totalPages
                : !(data.changes && data.changes.length === this.itemsPerPage);
            nextPageBtn.disabled = reachedLastPage;
        }
    }
    
    applyFilters() {
        // Get change types
    const changeTypeCheckboxes = document.querySelectorAll('input[name="change_types"]:checked');
        this.filters.changeTypes = Array.from(changeTypeCheckboxes).map(cb => cb.value);
        
        // Get date range
        this.filters.fromDate = document.getElementById('fromDate').value || null;
        this.filters.toDate = document.getElementById('toDate').value || null;
        
        // Get search query
        this.filters.searchQuery = document.getElementById('searchInput').value.trim();

        const sortSelect = document.getElementById('sortSelect');
        if (sortSelect) {
            this.filters.sortMode = sortSelect.value || 'recent';
        }
        
        // Reset to first page
        this.currentPage = 1;
        
        // Reload changes
        this.loadRecentChanges();
    }

    sortChanges(items) {
        if (!Array.isArray(items) || items.length === 0) {
            return [];
        }

        if (this.filters.sortMode === 'title') {
            return items.sort((a, b) => {
                const titleA = (a.title || '').toString().toLowerCase();
                const titleB = (b.title || '').toString().toLowerCase();
                if (titleA && titleB) {
                    return titleA.localeCompare(titleB, undefined, { sensitivity: 'base' });
                }
                if (titleA) return -1;
                if (titleB) return 1;
                return 0;
            });
        }

        // Default: newest first (already ordered by API)
        return items;
    }
    
    async startScrape() {
        try {
            this.showLoading('Starting scrape...');
            const maxPagesInput = document.getElementById('maxPagesInput');
            const maxPagesValue = maxPagesInput ? parseInt(maxPagesInput.value || '0', 10) : 0;
            const maxPages = Number.isFinite(maxPagesValue) && maxPagesValue > 0 ? maxPagesValue : 0;
            const selectedCategoryUrls = this.getSelectedCategories(this.currentClient);

            if (this.currentClient === 'txparts' && !selectedCategoryUrls.length) {
                throw new Error('TXParts scraping requires selecting category URLs first.');
            }
            
            const seedUrlMap = {
                mobilesentrix: 'https://www.mobilesentrix.com/',
                xcellparts: 'https://xcellparts.com/',
                txparts: 'https://txparts.com/'
            };

            const payload = {
                client: this.currentClient,
                seed_url: seedUrlMap[this.currentClient] || seedUrlMap.mobilesentrix,
                max_pages: maxPages
            };

            if (selectedCategoryUrls.length) {
                payload.categories = selectedCategoryUrls;
            }

            const response = await fetch('/api/scrape/start', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            });
            
            const data = await response.json();
            
            if (!response.ok) {
                throw new Error(data.error || 'Failed to start scrape');
            }
            
            this.currentJobId = data.job_id;
            this.persistActiveJobId(this.currentJobId);
            const maxPagesForMeta = maxPages > 0 ? maxPages : 'unlimited';
            this.updateRunMeta({
                job_id: this.currentJobId,
                client: this.currentClient,
                status: 'queued',
                config: {
                    selected_categories: selectedCategoryUrls,
                    max_pages: maxPagesForMeta
                }
            });
            this.openJobModal();
            this.pollJobStatus();
            
        } catch (error) {
            console.error('Error starting scrape:', error);
            this.showError('Failed to start scrape: ' + error.message);
        } finally {
            this.hideLoading();
        }
    }

    async requestStopScrape() {
        if (!this.currentJobId) {
            this.showError('No active scraping job to stop.');
            return;
        }

        try {
            this.showLoading('Requesting stop...');
            const response = await fetch('/api/scrape/stop', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    job_id: this.currentJobId,
                    reason: 'User cancelled from dashboard'
                })
            });

            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || 'Failed to stop scrape');
            }

            console.info('Stop request acknowledged:', data.message || data.status);
            this.pollJobStatus();
        } catch (error) {
            console.error('Error stopping scrape:', error);
            this.showError('Failed to stop scrape: ' + error.message);
        } finally {
            this.hideLoading();
        }
    }
    
    async exportChanges() {
        try {
            this.showLoading('Preparing export...');
            
            const response = await fetch('/api/results/export/xlsx', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    client: this.currentClient,
                    filters: this.filters
                })
            });
            
            if (!response.ok) {
                const data = await response.json();
                throw new Error(data.error || 'Export failed');
            }
            
            // Download file
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `${this.currentClient}_changes_${new Date().toISOString().slice(0, 10)}.xlsx`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            window.URL.revokeObjectURL(url);
            
        } catch (error) {
            console.error('Error exporting changes:', error);
            this.showError('Failed to export changes: ' + error.message);
        } finally {
            this.hideLoading();
        }
    }
    
    showJobModal() {
        const modal = this.getJobModalInstance();
        if (modal) {
            modal.show();
        }
    }
    
    closeModal() {
        const modal = this.getJobModalInstance();
        if (modal) {
            modal.hide();
        }
    }

    getJobModalInstance() {
        const jobModal = document.getElementById('jobModal');
        if (!jobModal || !window.bootstrap) {
            return null;
        }
        let modal = bootstrap.Modal.getInstance(jobModal);
        if (!modal) {
            modal = new bootstrap.Modal(jobModal, {
                backdrop: 'static',
                keyboard: false
            });
        }
        return modal;
    }

    openJobModal() {
        const modal = this.getJobModalInstance();
        if (!modal) {
            return;
        }
        modal.show();
    }

    toggleJobModal() {
        const modal = this.getJobModalInstance();
        if (!modal) {
            return;
        }
        const modalElement = document.getElementById('jobModal');
        const hasJobData = Boolean(this.currentJobId || (this.currentJobSnapshot && this.currentJobSnapshot.job_id));
        if (!hasJobData) {
            this.showError('No scraping job in progress. Start a scrape first.');
            return;
        }

        const isVisible = modalElement.classList.contains('show');
        if (isVisible) {
            modal.hide();
        } else {
            modal.show();
        }
    }
    
    async pollJobStatus() {
        if (!this.currentJobId) return;
        
        try {
            const response = await fetch(`/api/scrape/status?job_id=${this.currentJobId}`);
            const data = await response.json();
            
            if (response.ok) {
                this.updateJobStatus(data);
                this.updateLiveIndicators(data);

                const status = (data.status || '').toLowerCase();
                const isActive = ['running', 'queued', 'cancelling'].includes(status);

                if (isActive) {
                    setTimeout(() => this.pollJobStatus(), 2000); // Poll every 2 seconds
                } else {
                    this.currentJobId = null;
                    this.clearStoredActiveJobId();
                    // Job completed, refresh dashboard after a delay
                    setTimeout(() => {
                        this.loadDashboard();
                        this.closeModal();
                    }, 1000);
                }
            }
        } catch (error) {
            console.error('Error polling job status:', error);
        }
    }

    async pollJobStatusSilent() {
        if (!this.currentJobId) return;
        
        try {
            const response = await fetch(`/api/scrape/status?job_id=${this.currentJobId}`);
            const data = await response.json();
            
            if (response.ok) {
                // Update live indicators without modal
                this.updateLiveIndicators(data);

                const status = (data.status || '').toLowerCase();
                const isActive = ['running', 'queued', 'cancelling'].includes(status);

                if (!isActive) {
                    // Job completed, clear current job and refresh
                    this.currentJobId = null;
                    this.clearStoredActiveJobId();
                    this.loadDashboard();
                }
            }
        } catch (error) {
            console.error('Error polling job status silently:', error);
        }
    }
    
    updateJobStatus(data) {
        const statusLabel = (data.status || 'idle').toUpperCase();
        const pagesDone = data.pages_done || 0;
        const itemsFound = data.items_found || 0;
        const categoriesDone = data.categories_done || 0;
        const totalCategories = data.total_categories || 0;
        const newProducts = data.new_products || 0;
        const updatedProducts = data.updated_products || 0;
        const currentCategory = data.current_category || '—';
        const config = data.config || {};
        const selectedCategories = Array.isArray(config.selected_categories)
            ? config.selected_categories
            : [];
        const timing = this.computeJobTiming(data);

        const statusEl = document.getElementById('jobStatus');
        if (statusEl) statusEl.textContent = statusLabel;

        const pagesEl = document.getElementById('jobPages');
        if (pagesEl) pagesEl.textContent = pagesDone;

        const itemsEl = document.getElementById('jobItems');
        if (itemsEl) itemsEl.textContent = itemsFound;

        const categoriesEl = document.getElementById('jobCategories');
        if (categoriesEl) {
            categoriesEl.textContent = totalCategories > 0
                ? `${categoriesDone}/${totalCategories}`
                : categoriesDone;
        }

        const newProductsEl = document.getElementById('jobNewProducts');
        if (newProductsEl) newProductsEl.textContent = newProducts;

        const updatedProductsEl = document.getElementById('jobUpdatedProducts');
        if (updatedProductsEl) updatedProductsEl.textContent = updatedProducts;

        const currentCategoryEl = document.getElementById('jobCurrentCategory');
        if (currentCategoryEl) currentCategoryEl.textContent = currentCategory;

        const selectedContainer = document.getElementById('jobSelectedCategories');
        if (selectedContainer) {
            selectedContainer.innerHTML = '';
            if (!selectedCategories.length) {
                if (this.autoLimitSummary && this.autoLimitSummary.total > this.autoLimitSummary.limit) {
                    selectedContainer.textContent = `Auto cap ${this.autoLimitSummary.limit}/${this.autoLimitSummary.total}`;
                    selectedContainer.title = 'Automatically limited for faster runs. Use the category picker to target a smaller set or set 0 to crawl everything.';
                } else {
                    selectedContainer.textContent = 'All categories';
                    selectedContainer.removeAttribute('title');
                }
            } else {
                selectedCategories.slice(0, 6).forEach((url) => {
                    const pill = document.createElement('span');
                    pill.className = 'job-category-pill';
                    pill.textContent = this.prettyCategoryLabel(url);
                    pill.title = url;
                    selectedContainer.appendChild(pill);
                });
                if (selectedCategories.length > 6) {
                    const more = document.createElement('span');
                    more.className = 'job-category-pill job-category-pill--more';
                    more.textContent = `+${selectedCategories.length - 6} more`;
                    selectedContainer.appendChild(more);
                }
            }
        }

        const elapsedEl = document.getElementById('jobElapsed');
        if (elapsedEl) {
            elapsedEl.textContent = timing.elapsedText;
        }

        const etaEl = document.getElementById('jobEta');
        if (etaEl) {
            etaEl.textContent = timing.remainingText;
        }

        const finishEl = document.getElementById('jobEtaTime');
        if (finishEl) {
            finishEl.textContent = timing.finishText;
        }
        
        const errorDiv = document.getElementById('jobError');
        if (data.last_error) {
            errorDiv.style.display = 'flex';
            document.getElementById('jobErrorText').textContent = data.last_error;
        } else {
            errorDiv.style.display = 'none';
        }
        
        let progress = 0;
        if (statusLabel === 'DONE' || statusLabel === 'COMPLETED') {
            progress = 100;
        } else if (totalCategories > 0) {
            progress = Math.round((categoriesDone / totalCategories) * 100);
        } else if (pagesDone > 0) {
            progress = Math.min(95, pagesDone * 5);
        } else if (statusLabel === 'RUNNING') {
            progress = 15;
        }

        const jobProgress = document.getElementById('jobProgress');
        if (jobProgress) {
            jobProgress.style.width = `${Math.min(100, progress)}%`;
        }

        const jobContext = {
            ...data,
            job_id: data.job_id || this.currentJobId,
            client: data.client || this.currentClient,
            config
        };
        this.updateRunMeta(jobContext);
        this.currentJobSnapshot = jobContext;
    }
    
    updateLiveIndicators(data = {}) {
    const status = (data.status || 'idle').toLowerCase();
        const categoriesDone = data.categories_done || 0;
        const totalCategories = data.total_categories || 0;
        const pagesDone = data.pages_done || 0;
        const itemsFound = data.items_found || 0;
        const currentCategory = data.current_category || '';
    const isRunning = ['running', 'queued', 'cancelling'].includes(status);
        const timing = this.computeJobTiming(data);

        const liveIndicator = document.getElementById('liveIndicator');
        if (liveIndicator) {
            liveIndicator.classList.toggle('active', isRunning);
            liveIndicator.classList.toggle('d-none', !isRunning);
        }

        const liveStatusDisplay = document.getElementById('liveStatusDisplay');
        if (liveStatusDisplay) {
            const statusTextElement = liveStatusDisplay.querySelector('.job-status-text');
            if (isRunning) {
                liveStatusDisplay.classList.remove('d-none');
                liveStatusDisplay.classList.add('show');
                if (statusTextElement) {
                    const categoryProgress = totalCategories > 0
                        ? `${categoriesDone}/${totalCategories} categories`
                        : `${categoriesDone} categories`;
                    const details = [`${categoryProgress}`, `${pagesDone} pages`, `${itemsFound} items`];
                    if (timing.remainingMs != null && timing.remainingMs > 0) {
                        details.push(`ETA ${timing.remainingText}`);
                    }
                    if (currentCategory) {
                        details.push(`Now: ${currentCategory}`);
                    }
                    statusTextElement.textContent = `${status.toUpperCase()} · ${details.join(' · ')}`;
                }
            } else {
                liveStatusDisplay.classList.add('d-none');
                liveStatusDisplay.classList.remove('show');
                if (statusTextElement) {
                    statusTextElement.textContent = 'Status updates will appear here...';
                }
            }
        }

        const runBtn = document.getElementById('runScrapeBtn');
        if (runBtn) {
            const btnText = runBtn.querySelector('.btn-text');
            if (isRunning) {
                const categoryProgress = totalCategories > 0
                    ? `${categoriesDone}/${totalCategories} categories`
                    : `${categoriesDone} categories`;
                if (btnText) {
                    btnText.textContent = `Running… (${categoryProgress})`;
                }
                runBtn.disabled = true;
                runBtn.classList.add('btn-warning');
                runBtn.classList.remove('btn-primary');
            } else {
                if (btnText) {
                    btnText.textContent = 'Run Scrape';
                }
                runBtn.disabled = false;
                runBtn.classList.add('btn-primary');
                runBtn.classList.remove('btn-warning');
            }
        }

        const stopBtn = document.getElementById('stopScrapeBtn');
        if (stopBtn) {
            stopBtn.disabled = !isRunning;
            stopBtn.classList.toggle('btn-danger', isRunning);
            stopBtn.classList.toggle('btn-outline', !isRunning);
            stopBtn.setAttribute('aria-disabled', String(!isRunning));
        }

        const jobContext = {
            ...data,
            job_id: data.job_id || this.currentJobId,
            client: data.client || this.currentClient,
            config: data.config || {}
        };
        this.updateRunMeta(jobContext);
    }
    
    restoreActiveJob() {
        const storedJobId = this.pendingJobRestoreId;
        if (!storedJobId) {
            return;
        }

        this.pendingJobRestoreId = null;
        this.currentJobId = storedJobId;
        this.persistActiveJobId(storedJobId);
        this.pollJobStatusSilent();
    }

    startPolling() {
        // Poll for dashboard updates every 5 seconds when active
        setInterval(() => {
            if (!document.hidden) {
                this.loadSummary();
                
                // If we have an active job, poll more frequently
                if (this.currentJobId) {
                    this.pollJobStatusSilent();
                }
            }
        }, 5000);
        
        // Refresh recent changes less frequently 
        setInterval(() => {
            if (!document.hidden && !this.currentJobId) {
                this.loadRecentChanges();
            }
        }, 30000);
    }

    setupHelpPopovers() {
        const helpButtons = document.querySelectorAll('.section-help[data-help-content]');
        helpButtons.forEach((button) => {
            if (button.dataset.popoverInitialized === 'true') {
                return;
            }

            const content = button.getAttribute('data-help-content');
            if (!content) {
                return;
            }

            if (window.bootstrap && bootstrap.Popover) {
                new bootstrap.Popover(button, {
                    trigger: 'focus hover',
                    placement: 'auto',
                    html: false,
                    content
                });
            } else {
                button.addEventListener('click', () => {
                    alert(content);
                });
            }

            button.dataset.popoverInitialized = 'true';
        });
    }
    
    showLoading(message) {
        const loadingText = document.getElementById('loadingText');
        const loadingOverlay = document.getElementById('loadingOverlay');
        if (loadingText) loadingText.textContent = message;
        if (loadingOverlay) {
            loadingOverlay.classList.remove('d-none');
        }
    }
    
    hideLoading() {
        const loadingOverlay = document.getElementById('loadingOverlay');
        if (loadingOverlay) {
            loadingOverlay.classList.add('d-none');
        }
    }
    
    showError(message) {
        // Simple error display - could be enhanced with a proper toast/notification system
        alert('Error: ' + message);
    }
    
    // Utility functions
    getStoredViewMode() {
        try {
            const stored = localStorage.getItem(this.viewModeStorageKey);
            if (stored === 'cards' || stored === 'list') {
                return stored;
            }
        } catch (error) {
            console.warn('Unable to read stored view mode preference:', error);
        }
        return 'cards';
    }

    persistViewMode(mode) {
        try {
            localStorage.setItem(this.viewModeStorageKey, mode);
        } catch (error) {
            console.warn('Unable to persist view mode preference:', error);
        }
    }

    getStoredActiveJobId() {
        try {
            const stored = localStorage.getItem(this.ACTIVE_JOB_STORAGE_KEY);
            return stored || null;
        } catch (error) {
            console.warn('Unable to read stored active job id:', error);
            return null;
        }
    }

    persistActiveJobId(jobId) {
        try {
            if (jobId) {
                localStorage.setItem(this.ACTIVE_JOB_STORAGE_KEY, String(jobId));
            } else {
                localStorage.removeItem(this.ACTIVE_JOB_STORAGE_KEY);
            }
        } catch (error) {
            console.warn('Unable to persist active job id:', error);
        }
    }

    clearStoredActiveJobId() {
        this.persistActiveJobId(null);
    }

    getStoredPageSize() {
        try {
            const stored = localStorage.getItem(this.storageKeys.pageSize);
            const parsed = parseInt(stored, 10);
            return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
        } catch (error) {
            console.warn('Unable to read stored page size preference:', error);
            return null;
        }
    }

    persistPageSize(value) {
        try {
            localStorage.setItem(this.storageKeys.pageSize, String(value));
        } catch (error) {
            console.warn('Unable to persist page size preference:', error);
        }
    }

    populateClientSelector(clients, activeClient) {
        const normalized = Array.isArray(clients)
            ? Array.from(new Set(
                clients
                    .filter((value) => typeof value === 'string')
                    .map((value) => value.trim())
                    .filter(Boolean)
            ))
            : [];

        if (!normalized.length) {
            normalized.push('mobilesentrix');
        }

        const resolvedActive = (activeClient && normalized.includes(activeClient))
            ? activeClient
            : null;
        const effectiveActive = resolvedActive
            || (normalized.includes(this.currentClient) ? this.currentClient : normalized[0]);

        if (effectiveActive) {
            this.currentClient = effectiveActive;
        }

        this.availableClients = normalized.slice();

        const selector = document.getElementById('clientSelector');
        if (selector) {
            const currentOptions = Array.from(selector.options).map((opt) => opt.value);
            const needsUpdate = currentOptions.length !== normalized.length
                || currentOptions.some((value, index) => normalized[index] !== value);

            if (needsUpdate) {
                selector.innerHTML = '';
                normalized.forEach((client) => {
                    const option = document.createElement('option');
                    option.value = client;
                    option.textContent = this.formatClientLabel(client);
                    selector.appendChild(option);
                });
            }

            if (effectiveActive) {
                selector.value = effectiveActive;
            }
        }

        this.renderClientToggle(normalized, effectiveActive);
        this.syncClientToggle(effectiveActive);
    }

    renderClientToggle(clients, activeClient) {
        const toggle = document.getElementById('clientToggle');
        if (!toggle) {
            return;
        }

        const safeClients = Array.isArray(clients)
            ? Array.from(new Set(
                clients
                    .filter((value) => typeof value === 'string')
                    .map((value) => value.trim())
                    .filter(Boolean)
            ))
            : [];

        if (!safeClients.length) {
            safeClients.push('mobilesentrix');
        }

        const effectiveActive = (activeClient && safeClients.includes(activeClient))
            ? activeClient
            : (safeClients.includes(this.currentClient) ? this.currentClient : safeClients[0]);

        const fragment = document.createDocumentFragment();

        safeClients.forEach((client) => {
            const button = document.createElement('button');
            button.type = 'button';
            button.className = 'client-toggle__btn';
            button.dataset.client = client;
            button.dataset.clientToggle = 'true';
            button.textContent = this.formatClientLabel(client);

            const isActive = client === effectiveActive;
            button.classList.toggle('is-active', isActive);
            button.setAttribute('aria-pressed', String(isActive));

            fragment.appendChild(button);
        });

        toggle.replaceChildren(fragment);
    }

    updateTotals(totals) {
        const safeTotals = totals || {};
        const totalProductsEl = document.getElementById('totalProductsValue');
        if (totalProductsEl) {
            totalProductsEl.textContent = this.formatNumber(safeTotals.total_products || 0);
        }

        const priceChangesEl = document.getElementById('priceChangesValue');
        if (priceChangesEl) {
            priceChangesEl.textContent = this.formatNumber(safeTotals.price_changes_24h || 0);
        }

        const stockChangesEl = document.getElementById('stockChangesValue');
        if (stockChangesEl) {
            stockChangesEl.textContent = this.formatNumber(safeTotals.stock_changes_24h || 0);
        }

        const descriptionUpdatesEl = document.getElementById('descriptionUpdatesValue');
        if (descriptionUpdatesEl) {
            descriptionUpdatesEl.textContent = this.formatNumber(safeTotals.description_updates_24h || 0);
        }
    }

    updateCategoryProgress(stats) {
        const safeStats = stats || {};
        const completed = Number(safeStats.completed || 0);
        const total = Number(safeStats.total || 0);
        const percentage = Number.isFinite(safeStats.completion_pct)
            ? Number(safeStats.completion_pct)
            : total > 0
                ? (completed / total) * 100
                : 0;

        const clamped = Math.max(0, Math.min(100, percentage));

        const batteryFill = document.getElementById('batteryFill');
        if (batteryFill) {
            batteryFill.style.width = `${clamped}%`;

            let color = '#ef4444';
            if (clamped >= 75) {
                color = '#10b981';
            } else if (clamped >= 30) {
                color = '#f59e0b';
            }

            batteryFill.style.background = `linear-gradient(90deg, #ef4444 0%, #f59e0b 50%, ${color} 100%)`;
        }

        const batteryPercentage = document.getElementById('batteryPercentage');
        if (batteryPercentage) {
            batteryPercentage.textContent = `${clamped.toFixed(1)}%`;
        }

        const batteryStats = document.getElementById('batteryStats');
        if (batteryStats) {
            batteryStats.textContent = `${completed} of ${total} categories completed`;
        }
    }

    updateRunInfo(runs) {
        const safeRuns = runs || {};

        const lastRunText = document.getElementById('lastRunText');
        if (lastRunText) {
            const lastRun = safeRuns.last_run_at;
            lastRunText.textContent = `Last run: ${lastRun ? this.formatRelativeTime(lastRun) : 'Never'}`;
        }

        const nextRunText = document.getElementById('nextRunText');
        if (nextRunText) {
            const eta = Number(safeRuns.next_run_eta_minutes || 0);
            nextRunText.textContent = `Next run in: ${eta > 0 ? this.formatDuration(eta) : 'Now'}`;
        }
    }

    computeJobTiming(job = {}) {
        const startedAt = job.started_at ? new Date(job.started_at) : null;
        const completedAt = job.completed_at ? new Date(job.completed_at) : null;
        const now = new Date();

        const effectiveEnd = completedAt || now;
        const elapsedMs = startedAt ? Math.max(effectiveEnd - startedAt, 0) : 0;

        const completedCount = Number(job.categories_done) || 0;
        const totalCount = Number(job.total_categories) || 0;

        let remainingMs = null;
        let finishAt = null;

        if (completedAt) {
            remainingMs = 0;
            finishAt = completedAt;
        } else if (startedAt && completedCount > 0 && totalCount >= completedCount) {
            const perUnit = elapsedMs / completedCount;
            const remainingUnits = Math.max(totalCount - completedCount, 0);
            remainingMs = perUnit * remainingUnits;
            finishAt = new Date(now.getTime() + remainingMs);
        } else if (startedAt && totalCount === 0 && job.status === 'done') {
            remainingMs = 0;
            finishAt = now;
        }

        return {
            elapsedMs,
            remainingMs,
            finishAt,
            elapsedText: startedAt ? this.formatDurationMs(elapsedMs) : '—',
            remainingText: remainingMs == null
                ? 'Calculating…'
                : remainingMs <= 0
                    ? 'Finishing…'
                    : this.formatDurationMs(remainingMs),
            finishText: finishAt ? this.formatTimeOfDay(finishAt) : '—'
        };
    }

    formatDurationMs(ms) {
        if (!Number.isFinite(ms) || ms < 0) {
            return '—';
        }

        const totalSeconds = Math.floor(ms / 1000);
        const days = Math.floor(totalSeconds / 86400);
        const hours = Math.floor((totalSeconds % 86400) / 3600);
        const minutes = Math.floor((totalSeconds % 3600) / 60);
        const seconds = totalSeconds % 60;

        const parts = [];
        if (days > 0) {
            parts.push(`${days}d`);
        }
        if (hours > 0) {
            parts.push(`${hours}h`);
        }
        if (minutes > 0) {
            parts.push(`${minutes}m`);
        }
        if (!parts.length || (days === 0 && hours === 0 && minutes === 0)) {
            parts.push(`${seconds}s`);
        }

        return parts.join(' ');
    }

    formatTimeOfDay(date) {
        if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
            return '—';
        }

        return date.toLocaleString(undefined, {
            hour: 'numeric',
            minute: '2-digit',
            hour12: true
        });
    }

    buildDifferenceBadge(change, options = {}) {
        if (!change || !change.change_type) {
            return null;
        }

        if (change.is_baseline) {
            const badge = document.createElement('span');
            if (!options.asText) {
                badge.className = 'change-card__delta change-card__delta--new';
            }
            badge.textContent = this.getBaselineLabel(change);
            badge.title = this.getBaselineDescription(change);
            badge.dataset.baseline = 'true';
            return badge;
        }

        const type = change.change_type.toString().toLowerCase();

        if (type === 'price') {
            const oldResolved = this.resolveChangeValue(change, 'old');
            const newResolved = this.resolveChangeValue(change, 'new');

            let difference = Number(change.difference);
            if (!Number.isFinite(difference)) {
                if (newResolved.numeric != null && oldResolved.numeric != null) {
                    difference = newResolved.numeric - oldResolved.numeric;
                } else {
                    difference = null;
                }
            }

            if (!Number.isFinite(difference) || difference === 0) {
                return null;
            }

            const direction = difference > 0 ? 'up' : 'down';
            const badge = document.createElement('span');
            if (!options.asText) {
                badge.className = `change-card__delta change-card__delta--${direction}`;
            }
            badge.dataset.direction = direction;

            const absoluteDiff = this.formatCurrency(Math.abs(difference));
            const prefix = difference > 0 ? '+' : '-';
            badge.textContent = `${prefix}${absoluteDiff}`;
            badge.title = `Price ${difference > 0 ? 'increased' : 'decreased'} by ${absoluteDiff}`;

            return badge;
        }

        if (type === 'stock') {
            const rawOld = (change.old_value_raw ?? change.old_value ?? '').toString();
            const rawNew = (change.new_value_raw ?? change.new_value ?? '').toString();
            if (!rawOld && !rawNew) {
                return null;
            }

            const oldLower = rawOld.toLowerCase();
            const newLower = rawNew.toLowerCase();
            const containsAny = (value, tokens) => tokens.some((token) => value.includes(token));

            let label = 'Stock Updated';
            let direction = 'flat';

            if (containsAny(newLower, ['in stock', 'instock', 'available', 'ready'])) {
                label = 'Restocked';
                direction = 'up';
            } else if (containsAny(newLower, ['out of stock', 'sold out', 'unavailable', 'oos'])) {
                label = 'Out of Stock';
                direction = 'down';
            } else if (containsAny(newLower, ['backorder', 'pre-order', 'preorder'])) {
                label = 'Backorder';
                direction = 'flat';
            } else if (!newLower && oldLower) {
                label = 'Stock Removed';
                direction = 'down';
            } else if (newLower && !oldLower) {
                label = 'Stock Added';
                direction = 'up';
            }

            const badge = document.createElement('span');
            if (!options.asText) {
                badge.className = `change-card__delta change-card__delta--${direction}`;
            }
            badge.dataset.direction = direction;
            badge.textContent = label;
            badge.title = `Stock changed from "${rawOld || '—'}" to "${rawNew || '—'}"`;
            return badge;
        }

        if (type === 'description') {
            const badge = document.createElement('span');
            if (!options.asText) {
                badge.className = 'change-card__delta change-card__delta--flat';
            }
            badge.textContent = 'Description Updated';
            badge.title = 'Product description content changed';
            return badge;
        }

        return null;
    }

    buildMetaPills(change) {
        if (!change) {
            return null;
        }

        const metaItems = [
            { label: 'Model', value: change.model_identifier || change.model },
            { label: 'Chipset', value: change.chipset }
        ].filter((item) => Boolean(item.value));

        if (!metaItems.length) {
            return null;
        }

        const wrapper = document.createElement('div');
        wrapper.className = 'change-card__meta';

        metaItems.forEach((item) => {
            const pill = document.createElement('span');
            pill.className = 'change-card__meta-pill';

            const labelEl = document.createElement('span');
            labelEl.className = 'change-card__meta-label';
            labelEl.textContent = `${item.label}:`;

            const valueEl = document.createElement('span');
            valueEl.className = 'change-card__meta-value';
            const displayValue = this.truncateText(item.value.toString(), 60);
            valueEl.textContent = displayValue;
            valueEl.title = item.value;

            pill.append(labelEl, valueEl);
            wrapper.appendChild(pill);
        });

        return wrapper;
    }

    buildCompatibilityList(change) {
        if (!change || !Array.isArray(change.compatibility) || change.compatibility.length === 0) {
            return null;
        }

        const normalizedCompatibility = change.compatibility
            .map((item) => (item == null ? '' : item.toString().trim()))
            .filter((item) => item.length > 0);

        if (!normalizedCompatibility.length) {
            return null;
        }

        const skipValues = new Set();
        if (change.model_identifier) {
            skipValues.add(change.model_identifier.toString().trim().toLowerCase());
        }
        if (change.model) {
            skipValues.add(change.model.toString().trim().toLowerCase());
        }

        const uniqueCompatibility = [];
        const seen = new Set();

        normalizedCompatibility.forEach((item) => {
            const lower = item.toLowerCase();
            if (skipValues.has(lower) || seen.has(lower)) {
                return;
            }
            seen.add(lower);
            uniqueCompatibility.push(item);
        });

        if (!uniqueCompatibility.length) {
            return null;
        }

        const container = document.createElement('div');
        container.className = 'change-card__compatibility';

        const label = document.createElement('div');
        label.className = 'change-card__compatibility-label';
        label.textContent = 'Compatible With';
        container.appendChild(label);

        const list = document.createElement('div');
        list.className = 'change-card__compatibility-list';

        uniqueCompatibility.slice(0, 4).forEach((item) => {
            const tag = document.createElement('span');
            tag.className = 'change-card__compatibility-pill';
            const displayValue = this.truncateText(item, 36);
            tag.textContent = displayValue;
            tag.title = item;
            list.appendChild(tag);
        });

        container.appendChild(list);
        return container;
    }

    describeChange(change) {
        if (!change) {
            return 'Product updated';
        }

        const type = (change.change_type || '').toString().toLowerCase();

        if (change.is_baseline) {
            if (type === 'price') {
                return 'First price captured';
            }
            if (type === 'stock') {
                return 'Initial stock snapshot recorded';
            }
            if (type === 'description') {
                return 'Initial description captured';
            }
            return 'Initial data captured';
        }

        const oldResolved = this.resolveChangeValue(change, 'old');
        const newResolved = this.resolveChangeValue(change, 'new');
        const oldText = oldResolved.isMissing ? '' : oldResolved.text;
        const newText = newResolved.isMissing ? '' : newResolved.text;
        const oldNumeric = oldResolved.numeric;
        const newNumeric = newResolved.numeric;

        const oldRaw = change.old_value_raw ?? change.old_value ?? '';
        const newRaw = change.new_value_raw ?? change.new_value ?? '';
        const oldRawLength = oldRaw ? oldRaw.toString().trim().length : 0;
        const newRawLength = newRaw ? newRaw.toString().trim().length : 0;

        if (type === 'price') {
            let difference = Number(change.difference);
            if (!Number.isFinite(difference) && newNumeric != null && oldNumeric != null) {
                difference = newNumeric - oldNumeric;
            }

            if (Number.isFinite(difference) && difference !== 0) {
                const direction = difference > 0 ? 'increased' : 'decreased';
                return `Price ${direction}`;
            }

            if (!oldText && newText) {
                return 'Price added';
            }

            if (oldText && !newText) {
                return 'Price removed';
            }

            if (oldText && newText) {
                return 'Price updated';
            }

            return 'Price change recorded';
        }

        if (type === 'stock') {
            if (oldText && newText) {
                return 'Stock status changed';
            }
            if (!oldText && newText) {
                return 'Stock status captured';
            }
            if (oldText && !newText) {
                return 'Stock status cleared';
            }
            return 'Stock update logged';
        }

        if (type === 'description') {
            if (oldRawLength && newRawLength) {
                return 'Description edited';
            }
            if (!oldRawLength && newRawLength) {
                return 'Description added';
            }
            if (oldRawLength && !newRawLength) {
                return 'Description removed';
            }
            return 'Description updated';
        }

        if (!oldText && newText) {
            return 'Value added';
        }
        if (oldText && !newText) {
            return 'Value cleared';
        }
        if (oldText && newText) {
            return 'Value changed';
        }
        return 'Record updated';
    }

    describeDifference(change) {
        if (!change) {
            return null;
        }
        if (change.is_baseline) {
            return 'First snapshot';
        }
        const badge = this.buildDifferenceBadge(change, { asText: true });
        if (!badge) {
            return null;
        }
        return badge.textContent || badge.innerText || null;
    }

    resolveChangeValue(change, side) {
        if (!change) {
            return {
                text: '',
                isMissing: true,
                numeric: null
            };
        }

        const type = (change.change_type || '').toString().toLowerCase();
        const rawField = change[`${side}_value_raw`];
        const structuredField = change[`${side}_value`];

        const stringCandidates = [];
        if (rawField !== null && rawField !== undefined) {
            stringCandidates.push(rawField);
        }
        if (structuredField !== null && structuredField !== undefined && structuredField !== rawField) {
            stringCandidates.push(structuredField);
        }

        const normalizedStrings = stringCandidates
            .map((value) => (value == null ? '' : value.toString().trim()))
            .filter((value) => this.hasMeaningfulValueText(value));

        let numeric = null;
        if (structuredField !== null && structuredField !== undefined) {
            const trimmedStructured = structuredField.toString().trim();
            if (trimmedStructured.length > 0) {
                const parsed = Number(structuredField);
                if (Number.isFinite(parsed)) {
                    numeric = parsed;
                }
            }
        }

        if (numeric === null && type === 'price') {
            for (const candidate of normalizedStrings) {
                const sanitized = candidate.replace(/[^0-9.-]/g, '');
                if (!sanitized) {
                    continue;
                }
                const parsed = Number(sanitized);
                if (Number.isFinite(parsed)) {
                    numeric = parsed;
                    break;
                }
            }
        }

        let text = '';
        if (type === 'price') {
            if (numeric !== null) {
                text = this.formatCurrency(numeric);
            } else if (normalizedStrings.length) {
                text = normalizedStrings[0];
            }
        } else if (normalizedStrings.length) {
            text = normalizedStrings[0];
        }

        return {
            text,
            isMissing: !text,
            numeric
        };
    }

    formatResolvedValue(resolved) {
        if (!resolved || resolved.isMissing) {
            return '—';
        }
        return resolved.text;
    }

    hasMeaningfulValueText(value) {
        if (value == null) {
            return false;
        }
        const trimmed = value.toString().trim();
        if (!trimmed) {
            return false;
        }
        const lowered = trimmed.toLowerCase();
        const placeholders = ['-', '—', 'n/a', 'na', 'none', 'null', 'tbd', 'pending'];
        if (placeholders.includes(lowered)) {
            return false;
        }
        return true;
    }

    formatSku(sku) {
        const cleaned = (sku || '').toString().trim();
        if (!cleaned) {
            return 'NO SKU';
        }
        const normalized = cleaned
            .replace(/\s+/g, ' ')
            .replace(/[_]+/g, '-')
            .toUpperCase();

        return normalized;
    }

    buildValueRow(label, value, modifier, resolved = null) {
        const wrapper = document.createElement('div');
        wrapper.className = `change-card__value change-card__value--${modifier}`;

        const labelEl = document.createElement('dt');
        labelEl.textContent = label;

        const valueEl = document.createElement('dd');
        const displayValue = value === null || value === undefined || value === ''
            ? '—'
            : value.toString();
        const truncated = this.truncateText(displayValue, 160);
        valueEl.textContent = truncated;

        const resolvedText = resolved && !resolved.isMissing ? resolved.text : null;
        if (resolvedText) {
            valueEl.title = resolvedText;
        } else if (displayValue !== '—') {
            valueEl.title = displayValue;
        } else {
            valueEl.title = 'Not recorded yet';
        }

        if ((resolved && resolved.isMissing) || displayValue === '—') {
            valueEl.classList.add('text-muted', 'is-missing');
        }

        wrapper.append(labelEl, valueEl);
        return wrapper;
    }

    formatClientLabel(client) {
        const safe = (client || '').toString().trim();
        if (!safe) {
            return 'Client';
        }

        const overrides = {
            mobilesentrix: 'MobileSentrix',
            xcellparts: 'XCell Parts',
            txparts: 'TXParts'
        };

        const normalized = safe.toLowerCase();
        if (overrides[normalized]) {
            return overrides[normalized];
        }

        return this.capitalizeWords(safe.replace(/[-_]+/g, ' '));
    }

    formatChangeType(type) {
        if (!type) {
            return 'Update';
        }

        const normalized = type.toString().trim().toLowerCase();
        const lookup = {
            price: 'Price',
            stock: 'Stock',
            description: 'Description',
            new: 'New',
            new_product: 'New',
            product_new: 'New'
        };

        if (lookup[normalized]) {
            return lookup[normalized];
        }

        const friendly = normalized.replace(/[_-]+/g, ' ');
        return this.capitalizeWords(friendly);
    }

    getBaselineLabel(change) {
        const type = change && change.change_type
            ? change.change_type.toString().toLowerCase()
            : '';

        switch (type) {
            case 'price':
                return 'New Price';
            case 'stock':
                return 'New Stock';
            case 'description':
                return 'New Description';
            case 'new':
                return 'New Product';
            default:
                return 'New Data';
        }
    }

    getBaselineDescription(change) {
        const type = change && change.change_type
            ? change.change_type.toString().toLowerCase()
            : '';

        if (type === 'price') {
            return 'First price snapshot captured for this product.';
        }
        if (type === 'stock') {
            return 'Initial stock status captured for this product.';
        }
        if (type === 'description') {
            return 'Initial description captured for this product.';
        }
        if (type === 'new') {
            return 'Product discovered during the most recent run.';
        }
        return 'Initial data captured for this product.';
    }

    getChangeTypeClass(type) {
        const safeValue = (type || 'update').toString().toLowerCase();
        const sanitized = safeValue.replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
        return sanitized || 'update';
    }

    capitalizeWords(text) {
        return text.replace(/\b\w/g, (char) => char.toUpperCase());
    }

    formatNumber(num) {
        if (num == null) return '-';
        return new Intl.NumberFormat().format(num);
    }

    formatCurrency(value) {
        const numeric = Number(value);
        if (!Number.isFinite(numeric)) {
            return '—';
        }

        try {
            return new Intl.NumberFormat('en-US', {
                style: 'currency',
                currency: 'USD',
                minimumFractionDigits: 2,
                maximumFractionDigits: 2
            }).format(numeric);
        } catch (error) {
            return numeric.toFixed(2);
        }
    }
    
    formatRelativeTime(dateString) {
        if (!dateString) return 'Never';
        
        const date = new Date(dateString);
        const now = new Date();
        const diff = now - date;
        
        const seconds = Math.floor(diff / 1000);
        const minutes = Math.floor(seconds / 60);
        const hours = Math.floor(minutes / 60);
        const days = Math.floor(hours / 24);
        
        if (days > 0) return `${days} day${days === 1 ? '' : 's'} ago`;
        if (hours > 0) return `${hours} hour${hours === 1 ? '' : 's'} ago`;
        if (minutes > 0) return `${minutes} minute${minutes === 1 ? '' : 's'} ago`;
        return 'Just now';
    }
    
    formatDuration(minutes) {
        if (minutes <= 0) return 'Now';
        
        const hours = Math.floor(minutes / 60);
        const mins = minutes % 60;
        
        if (hours > 0) {
            return `${hours}h ${mins}m`;
        }
        return `${mins}m`;
    }
    
    formatDateTime(dateString) {
        if (!dateString) return 'N/A';
        
        const date = new Date(dateString);
        return date.toLocaleString();
    }
    
    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
    
    truncateText(text, maxLength) {
        if (text === null || text === undefined) {
            return '';
        }

        const str = text.toString();
        if (str.length <= maxLength) {
            return str;
        }

        return str.substring(0, Math.max(0, maxLength - 3)) + '...';
    }
}

// Initialize dashboard when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    const dashboard = new ResultsDashboard();
    dashboard.init();
});