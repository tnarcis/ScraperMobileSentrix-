# Project TODO

## UI & Theming
- [ ] Audit existing UI issues on all pages and document required fixes (spacing, responsiveness, accessibility).
- [x] Implement global light/dark theme system with persistent toggle (localStorage) applied across every page/template.
- [ ] Refactor shared components/styles to remove inline styles and ensure consistent typography and spacing.

## Real-Time Results Page
- [ ] Design and build run monitor panel showing status, progress, current URL, queue depth.
- [ ] Implement SSE/WebSocket client to stream `run`, `progress`, `scraping`, and `item` events into the UI.
- [ ] Update results table to support streaming rows, virtualization, and loading/error states.

## Backend Real-Time Infrastructure
- [ ] Implement SSE/WebSocket endpoint in backend emitting run lifecycle events.
- [ ] Add run management endpoints (`POST /api/run`, `GET /api/run/:id`, `GET /api/items`) and integrate with existing app.
- [ ] Ensure backend updates database counters and broadcasts events in real time.

## Scraper Enhancements
- [ ] Implement discovery of all MobileSentrix "2400 Plus" model pages (pagination, dedupe).
- [ ] Scrape and extract item fields including SKU, price, stock, title, URL, checksum.
- [ ] Integrate scraper with worker queue emitting real-time events and writing to database.

## Persistence & Diffing
- [ ] Create Postgres migrations for `runs`, `items`, `item_snapshots`, and `run_diffs` tables.
- [ ] Implement per-run snapshots and canonical item upsert with checksum hashing.
- [ ] Build diff computation comparing latest run to previous run and expose via API.

## Frontend Diff Views
- [ ] Add diff tabs (Added, Removed, Changed) with inline field change display.
- [ ] Implement filters (stock status, price range, text search, new since previous run).
- [ ] Ensure diff data loads on page refresh and matches backend results.

## Testing & Documentation
- [ ] Add automated tests for discovery, scraping, diffing, and SSE streaming.
- [ ] Document environment variables, migration steps, and run instructions in README.
- [ ] Perform manual QA checklist covering theme toggle, real-time updates, diff accuracy, and persistence.
