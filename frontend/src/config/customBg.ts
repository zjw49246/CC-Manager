/*
 * 自定义主题的背景图：存取（IndexedDB）+ 取色。
 * ----------------------------------------------------------------------
 * 图片本身只做两件事：① 作为 html 背景铺在最底层（表面档位带 alpha 透出，
 * 见 customTheme.SURFACE_ALPHA）；② 「种」出背景色与品牌色两个 hex，喂给
 * 既有的色阶推导管线（buildCustomTheme）。取色之后图片就不参与配色计算了，
 * 用户仍可手动改那两个颜色。
 *
 * 存 IndexedDB 而非 localStorage：后者约 5MB 且只收字符串，壁纸很容易撑爆。
 */

import {
  hexToOklch, oklchToHex, hasBgImage, setHasBgImage,
  getCustomColors, getBgVisible, type Oklch,
} from './customTheme';

const DB_NAME = 'ccm-theme';
const DB_VERSION = 1;
const STORE = 'bg';
const IMAGE_KEY = 'image';
const BOOTSTRAP_KEY = 'ccm-theme-bootstrap-image';
/** Keep the parser-blocking snapshot small; IDB remains authoritative for the full image. */
const MAX_BOOTSTRAP_CHARS = 350_000;

/** 壳色 scrim 的浓度：可见度=100 时最淡（图片最明显），越低越浓（图片渐隐）。 */
const SCRIM_MIN = 0;
const SCRIM_MAX = 0.40;

/** 落盘前缩到最长边不超过此值，控制体积（壁纸多为 4K，直接存太浪费）。 */
const MAX_EDGE = 1920;
/** 取色采样边长：32×32 足够代表整图色调，且逐像素转 OKLCh 开销可忽略。 */
const SAMPLE_EDGE = 32;

function openDb(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      if (!req.result.objectStoreNames.contains(STORE)) req.result.createObjectStore(STORE);
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

function tx<T>(mode: IDBTransactionMode, fn: (s: IDBObjectStore) => IDBRequest<T>): Promise<T> {
  return openDb().then(
    (db) =>
      new Promise<T>((resolve, reject) => {
        const req = fn(db.transaction(STORE, mode).objectStore(STORE));
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
      }),
  );
}

export async function loadBgImage(): Promise<string | null> {
  if (!hasBgImage()) return null;
  try {
    return (await tx<string | undefined>('readonly', (s) => s.get(IMAGE_KEY))) ?? null;
  } catch {
    return null;  // IDB 不可用（隐私模式等）时静默降级为无图
  }
}

export async function saveBgImage(dataUrl: string): Promise<void> {
  await tx('readwrite', (s) => s.put(dataUrl, IMAGE_KEY));
  persistBootstrapSnapshot(dataUrl);
  setHasBgImage(true);
}

export async function clearBgImage(): Promise<void> {
  setHasBgImage(false);
  try { localStorage.removeItem(BOOTSTRAP_KEY); } catch { /* storage may be unavailable */ }
  try {
    await tx('readwrite', (s) => s.delete(IMAGE_KEY));
  } catch { /* 标记已清，残留数据无害 */ }
}

function loadImage(src: string): Promise<HTMLImageElement> {
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.onload = () => resolve(img);
    img.onerror = () => reject(new Error('图片解码失败'));
    img.src = src;
  });
}

function draw(img: HTMLImageElement, w: number, h: number): CanvasRenderingContext2D {
  const canvas = document.createElement('canvas');
  canvas.width = w;
  canvas.height = h;
  const ctx = canvas.getContext('2d', { willReadFrequently: true });
  if (!ctx) throw new Error('canvas 不可用');
  ctx.drawImage(img, 0, 0, w, h);
  return ctx;
}

/** 读文件 → 缩放 → 重编码 jpeg。返回可直接进 CSS 的 data URL。 */
export async function fileToDataUrl(file: File): Promise<string> {
  const raw = await new Promise<string>((resolve, reject) => {
    const r = new FileReader();
    r.onload = () => resolve(String(r.result));
    r.onerror = () => reject(r.error);
    r.readAsDataURL(file);
  });
  const img = await loadImage(raw);
  const scale = Math.min(1, MAX_EDGE / Math.max(img.width, img.height));
  if (scale === 1 && raw.length < 1_500_000) return raw;
  const w = Math.round(img.width * scale);
  const h = Math.round(img.height * scale);
  return draw(img, w, h).canvas.toDataURL('image/jpeg', 0.85);
}

/**
 * 从图片取「整体色」与「点缀色」。
 *
 * 整体色用于背景/中性色阶：亮度取均值（决定明暗与壳色），色相取**以色度
 * 加权的圆周均值**——直接平均色相会被灰像素带偏，且 0°/360° 环绕处会算出
 * 完全错误的中间值；加权圆周均值让"有颜色的像素说话"。
 *
 * 点缀色用于品牌色：取色度最高的那一档（图里最跳的颜色），亮度收进可用区
 * 间。若整图接近灰度（无明显点缀），退回整体色相的一个中亮度版本。
 */
export function extractColors(img: HTMLImageElement): { bg: string; brand: string } {
  const ctx = draw(img, SAMPLE_EDGE, SAMPLE_EDGE);
  const { data } = ctx.getImageData(0, 0, SAMPLE_EDGE, SAMPLE_EDGE);

  const px: Oklch[] = [];
  for (let i = 0; i < data.length; i += 4) {
    if (data[i + 3] < 128) continue;  // 跳过透明像素
    const hex = `#${[data[i], data[i + 1], data[i + 2]]
      .map((v) => v.toString(16).padStart(2, '0'))
      .join('')}`;
    px.push(hexToOklch(hex));
  }
  if (!px.length) return { bg: '#131316', brand: '#4f7cf7' };

  const avgL = px.reduce((s, p) => s + p.l, 0) / px.length;
  const avgC = px.reduce((s, p) => s + p.c, 0) / px.length;
  // 色度加权圆周均值：把每个色相当作单位向量按色度加权求和再取辐角
  let x = 0, y = 0;
  for (const p of px) {
    const rad = (p.h * Math.PI) / 180;
    x += Math.cos(rad) * p.c;
    y += Math.sin(rad) * p.c;
  }
  let avgH = (Math.atan2(y, x) * 180) / Math.PI;
  if (avgH < 0) avgH += 360;

  const bg = oklchToHex({ l: avgL, c: Math.min(avgC, 0.08), h: avgH });

  const vivid = px.reduce((best, p) => (p.c > best.c ? p : best), px[0]);
  // 点缀色太灰（整图近灰度）时不足以做品牌色，退回整体色相
  const brandSrc: Oklch = vivid.c < 0.04
    ? { l: 62, c: 0.15, h: avgH }
    : { l: Math.max(45, Math.min(70, vivid.l)), c: Math.min(vivid.c, 0.2), h: vivid.h };

  return { bg, brand: oklchToHex(brandSrc) };
}

/** 上传入口：缩放 → 存 IDB → 返回取好的两个颜色。 */
export async function importBgImage(file: File): Promise<{ bg: string; brand: string; dataUrl: string }> {
  const dataUrl = await fileToDataUrl(file);
  const img = await loadImage(dataUrl);
  const colors = extractColors(img);
  await saveBgImage(dataUrl);
  return { ...colors, dataUrl };
}

/** 壳色 scrim 的 CSS 色（半透明壳色，浓度随可见度）。作为伪元素上叠的一层，
 * 轻压对比；深色主题压暗、浅色主题提亮。 */
function scrimColor(): string {
  const shell = hexToOklch(getCustomColors().bg);
  const a = SCRIM_MIN + (1 - getBgVisible() / 100) * (SCRIM_MAX - SCRIM_MIN);
  return `oklch(${shell.l.toFixed(2)}% ${shell.c.toFixed(4)} ${shell.h.toFixed(2)} / ${a.toFixed(3)})`;
}

/** Store only a bounded first-paint representation. IDB is always the source of truth. */
function persistBootstrapSnapshot(dataUrl: string): void {
  try {
    if (dataUrl.length > MAX_BOOTSTRAP_CHARS || !dataUrl.startsWith('data:image/')) {
      localStorage.removeItem(BOOTSTRAP_KEY);
      return;
    }
    localStorage.setItem(BOOTSTRAP_KEY, dataUrl);
  } catch {
    // A failed replacement must not leave an older image available to bootstrap.
    try { localStorage.removeItem(BOOTSTRAP_KEY); } catch { /* storage may be unavailable */ }
  }
}

/** Apply the last saved image synchronously during the pre-paint bootstrap.
 * IndexedDB remains authoritative; this bounded localStorage snapshot only
 * bridges the first paint, after which applyBgImage() refreshes from IDB. */
export function applyBootstrapBgImage(): boolean {
  if (!hasBgImage()) return false;
  try {
    const dataUrl = localStorage.getItem(BOOTSTRAP_KEY);
    if (!dataUrl || dataUrl.length > MAX_BOOTSTRAP_CHARS || !dataUrl.startsWith('data:image/')) {
      localStorage.removeItem(BOOTSTRAP_KEY);
      document.documentElement.style.backgroundColor = 'var(--color-gray-950)';
      return false;
    }
    const el = document.documentElement;
    el.style.setProperty('--ccm-bg-url', `url("${dataUrl}")`);
    el.style.setProperty('--ccm-bg-scrim', scrimColor());
    el.dataset.hasBg = '1';
    return true;
  } catch {
    document.documentElement.style.backgroundColor = 'var(--color-gray-950)';
    return false;
  }
}

/** 把背景图交给 documentElement（异步读 IDB，故与色阶应用分开）。
 * 图片经由 CSS 变量喂给伪元素 [data-has-bg]::before，叠 scrim 后透出背景。 */
export async function applyBgImage(): Promise<void> {
  const el = document.documentElement;
  const dataUrl = await loadBgImage();
  if (!dataUrl) {
    el.style.removeProperty('--ccm-bg-url');
    el.style.removeProperty('--ccm-bg-scrim');
    delete el.dataset.hasBg;
    el.style.backgroundColor = 'var(--color-gray-950)';
    return;
  }
  persistBootstrapSnapshot(dataUrl);
  el.style.setProperty('--ccm-bg-url', `url("${dataUrl}")`);
  el.style.setProperty('--ccm-bg-scrim', scrimColor());
  el.dataset.hasBg = '1';
}
