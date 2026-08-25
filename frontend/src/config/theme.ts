import { applyCustomTheme, clearCustomTheme } from './customTheme';
import { applyBgImage } from './customBg';
import {
  DEFAULT_THEME,
  THEME_OPTIONS,
  type Theme,
  type ThemeOption,
} from './themeRegistry';

export { DEFAULT_THEME, THEME_OPTIONS, type Theme, type ThemeOption } from './themeRegistry';

const STORAGE_KEY = 'cc_theme';

const THEME_MAP = new Map(THEME_OPTIONS.map((t) => [t.value, t]));

export function getThemeOption(theme: Theme): ThemeOption {
  return THEME_MAP.get(theme) ?? THEME_MAP.get(DEFAULT_THEME)!;
}

/** 主题变更订阅（useTheme hook 用 useSyncExternalStore 接入，
 *  解决换主题后 React 侧图标集不重渲染的问题） */
const themeListeners = new Set<() => void>();
export function subscribeTheme(fn: () => void): () => void {
  themeListeners.add(fn);
  return () => {
    themeListeners.delete(fn);
  };
}

export function getTheme(): Theme {
  const stored = localStorage.getItem(STORAGE_KEY);
  return stored && THEME_MAP.has(stored as Theme) ? (stored as Theme) : DEFAULT_THEME;
}

export function setTheme(theme: Theme) {
  localStorage.setItem(STORAGE_KEY, theme);
  applyTheme(theme);
  themeListeners.forEach((fn) => fn());
}

export function applyTheme(theme?: Theme) {
  const t = theme || getTheme();
  const opt = THEME_MAP.get(t) ?? THEME_MAP.get(DEFAULT_THEME)!;
  document.documentElement.classList.remove('light');
  document.documentElement.dataset.theme = t;
  // custom 的色阶是运行时算出来的内联变量；切走时必须清场，否则会盖住新主题
  // 注意类型：THEME_OPTIONS 是 as const，opt.themeColor 是字面量联合；
  // custom 的取色是运行时算的普通 string，故这里必须显式放宽
  let themeColor: string = opt.themeColor;
  if (t === 'custom') {
    themeColor = applyCustomTheme();
    void applyBgImage();  // 图片字节要读 IDB，异步铺；色阶已同步就位
  } else {
    clearCustomTheme();
  }
  // 同步移动端状态栏 / PWA 顶栏颜色
  const meta = document.querySelector('meta[name="theme-color"]');
  if (meta) meta.setAttribute('content', themeColor);
  const statusBar = document.querySelector('meta[name="apple-mobile-web-app-status-bar-style"]');
  if (statusBar) {
    const scheme = t === 'custom' ? document.documentElement.dataset.scheme : opt.scheme;
    statusBar.setAttribute('content', scheme === 'light' ? 'default' : 'black-translucent');
  }
}
