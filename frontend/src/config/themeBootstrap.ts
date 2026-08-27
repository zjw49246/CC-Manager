import { applyCustomTheme, clearCustomTheme } from './customTheme';
import { applyBootstrapBgImage } from './customBg';
import { DEFAULT_THEME, THEME_OPTIONS, type Theme } from './themeRegistry';

const THEME_MAP = new Map(THEME_OPTIONS.map((theme) => [theme.value, theme]));

export function applyPrepaintTheme(): Theme {
  let stored: string | null = null;
  try {
    stored = localStorage.getItem('cc_theme');
  } catch {
    // Storage can be unavailable in locked-down browser contexts.
  }

  let theme = stored && THEME_MAP.has(stored as Theme) ? stored as Theme : DEFAULT_THEME;
  let option = THEME_MAP.get(theme) ?? THEME_MAP.get(DEFAULT_THEME)!;
  let themeColor: string = option.themeColor;
  let scheme: 'dark' | 'light' = option.scheme;

  if (theme === 'custom') {
    try {
      themeColor = applyCustomTheme();
      applyBootstrapBgImage();
      scheme = document.documentElement.dataset.scheme === 'light' ? 'light' : 'dark';
    } catch {
      clearCustomTheme();
      theme = DEFAULT_THEME;
      option = THEME_MAP.get(DEFAULT_THEME)!;
      themeColor = option.themeColor;
      scheme = option.scheme;
    }
  } else {
    clearCustomTheme();
  }

  document.documentElement.dataset.theme = theme;
  document.querySelector('meta[name="theme-color"]')?.setAttribute('content', themeColor);
  document.querySelector('meta[name="apple-mobile-web-app-status-bar-style"]')
    ?.setAttribute('content', scheme === 'light' ? 'default' : 'black-translucent');
  return theme;
}
