(function (window, document) {
  'use strict';

  const STORAGE_KEY = 'msx_theme';
  const DEFAULT_THEME = 'dark';
  const THEME_EVENT = 'themechange';

  function safeLocalStorage() {
    try {
      return window.localStorage;
    } catch (error) {
      console.warn('ThemeManager: localStorage unavailable', error);
      return null;
    }
  }

  function readStoredTheme() {
    const storage = safeLocalStorage();
    return storage ? storage.getItem(STORAGE_KEY) : null;
  }

  function writeStoredTheme(theme) {
    const storage = safeLocalStorage();
    if (!storage) return;
    try {
      storage.setItem(STORAGE_KEY, theme);
    } catch (error) {
      console.warn('ThemeManager: failed to persist theme', error);
    }
  }

  function systemPreference() {
    if (!window.matchMedia) return null;
    try {
      return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    } catch (error) {
      console.warn('ThemeManager: prefers-color-scheme unsupported', error);
      return null;
    }
  }

  function normalizeTheme(candidate) {
    return candidate === 'light' ? 'light' : 'dark';
  }

  function dispatchTheme(theme) {
    try {
      const event = new CustomEvent(THEME_EVENT, { detail: { theme } });
      window.dispatchEvent(event);
    } catch (error) {
      console.warn('ThemeManager: failed to dispatch theme event', error);
    }
  }

  function applyTheme(theme, shouldPersist = true) {
    const finalTheme = normalizeTheme(theme || currentTheme());
    document.documentElement.setAttribute('data-bs-theme', finalTheme);
    document.documentElement.dataset.theme = finalTheme;
    if (shouldPersist) {
      writeStoredTheme(finalTheme);
    }
    dispatchTheme(finalTheme);
    return finalTheme;
  }

  function currentTheme() {
    const attrTheme = document.documentElement.getAttribute('data-bs-theme');
    if (attrTheme) return normalizeTheme(attrTheme);
    const stored = readStoredTheme();
    if (stored) return normalizeTheme(stored);
    const system = systemPreference();
    if (system) return normalizeTheme(system);
    return DEFAULT_THEME;
  }

  function syncToggleState(toggle) {
    if (!toggle) return;
    toggle.checked = currentTheme() === 'dark';
  }

  function initToggle(toggle) {
    if (!toggle) return;
    syncToggleState(toggle);
    const onThemeChange = () => syncToggleState(toggle);
    toggle.addEventListener('change', function onToggleChange(event) {
      const nextTheme = event.target.checked ? 'dark' : 'light';
      applyTheme(nextTheme);
    });
    window.addEventListener(THEME_EVENT, onThemeChange);
  }

  function bootstrap() {
    applyTheme(currentTheme(), false);
  }

  bootstrap();

  window.ThemeManager = {
    STORAGE_KEY,
    applyTheme,
    currentTheme,
    initToggle,
    bootstrap
  };
})(window, document);
