import { describe, it, expect, beforeEach } from 'vitest';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { DEFAULT_THEME, THEME_OPTIONS, getTheme, setTheme, applyTheme } from './theme';
import { applyPrepaintTheme } from './themeBootstrap';
import { buildCustomTheme, clearCustomTheme, setCustomColors } from './customTheme';
import { applyBootstrapBgImage } from './customBg';

// vitest root = frontend/（jsdom 下 import.meta.url 非 file 协议，用 cwd 定位）
const indexCss = readFileSync(join(process.cwd(), 'src/index.css'), 'utf-8');
const indexHtml = readFileSync(join(process.cwd(), 'index.html'), 'utf-8');
const packageJson = JSON.parse(readFileSync(join(process.cwd(), 'package.json'), 'utf-8')) as {
  scripts?: Record<string, string>;
};

/**
 * 提取 index.css 中所有作用于该主题的变量块（块内无嵌套大括号）。
 * 含逗号选择器列表里的共享规则——浅色 accent 反转由 light 与 custom
 * 共用一条规则，故不能只认 `html[data-theme='x'] {` 这一种写法。
 */
function themeBlock(theme: string): string {
  const sel = `html[data-theme='${theme}']`;
  const blocks: string[] = [];
  // ^ 锚定行首：文件头注释里也会出现 `html[data-theme='x']：...` 字样，
  // 不锚定会把注释误当规则起点、一路吞到 @theme 块（2026-07-17 发现）
  const ruleStart = /^html\[data-theme=[^{]*\{/gm;
  let m: RegExpExecArray | null;
  while ((m = ruleStart.exec(indexCss)) !== null) {
    const selectors = m[0].slice(0, -1).split(',').map((s) => s.trim());
    if (!selectors.some((s) => s.startsWith(sel))) continue;
    blocks.push(indexCss.slice(m.index, indexCss.indexOf('\n}', m.index)));
  }
  expect(blocks.length, `index.css 应包含作用于 ${sel} 的规则`).toBeGreaterThan(0);
  return blocks.join('\n');
}

/** CLAUDE.md 约定：新增主题必须同时覆盖 gray 全档 + indigo 全档 */
const REQUIRED_GRAY = [950, 900, 800, 750, 700, 600, 500, 400, 300, 200, 100, 50];
const REQUIRED_INDIGO = [950, 900, 800, 700, 600, 500, 400, 300];

describe('theme config', () => {
  beforeEach(() => {
    localStorage.clear();
    document.querySelector('meta[name="theme-color"]')?.remove();
    const meta = document.createElement('meta');
    meta.setAttribute('name', 'theme-color');
    meta.setAttribute('content', '#131316');
    document.head.appendChild(meta);
    document.querySelector('meta[name="apple-mobile-web-app-status-bar-style"]')?.remove();
    const statusBar = document.createElement('meta');
    statusBar.setAttribute('name', 'apple-mobile-web-app-status-bar-style');
    statusBar.setAttribute('content', 'default');
    document.head.appendChild(statusBar);
    clearCustomTheme();
    document.documentElement.dataset.theme = 'light';
  });

  it('注册了飞书主题（modern 组浅色）', () => {
    const feishu = THEME_OPTIONS.find((o) => o.value === 'feishu');
    expect(feishu).toBeDefined();
    expect(feishu!.group).toBe('modern');
    expect(feishu!.scheme).toBe('light');
    expect(feishu!.themeColor).toBe('#ecedef');
  });

  it('注册了苹果主题（modern 组浅色）', () => {
    const apple = THEME_OPTIONS.find((o) => o.value === 'apple');
    expect(apple).toBeDefined();
    expect(apple!.group).toBe('modern');
    expect(apple!.scheme).toBe('light');
    expect(apple!.themeColor).toBe('#f9f9f9');
  });

  it('主题 value 无重复', () => {
    const values = THEME_OPTIONS.map((o) => o.value);
    expect(new Set(values).size).toBe(values.length);
  });

  it('首屏主题脚本阻塞加载且开发/构建前强制从权威源码生成', () => {
    const bootstrap = '<script src="/theme-bootstrap.js"></script>';
    expect(indexHtml).toContain(bootstrap);
    expect(indexHtml.indexOf(bootstrap)).toBeLessThan(indexHtml.indexOf('src="/src/main.tsx"'));
    expect(packageJson.scripts?.predev).toContain('generate:theme-bootstrap');
    expect(packageJson.scripts?.prebuild).toContain('generate:theme-bootstrap');
    expect(packageJson.scripts?.['generate:theme-bootstrap']).toContain('themeBootstrapEntry.ts');
  });

  it('setTheme 持久化并应用 data-theme 与 theme-color', () => {
    setTheme('feishu');
    expect(getTheme()).toBe('feishu');
    expect(document.documentElement.dataset.theme).toBe('feishu');
    expect(
      document.querySelector('meta[name="theme-color"]')!.getAttribute('content'),
    ).toBe('#ecedef');
  });

  it('首次访问和无效存储值都回退到浅色主题', () => {
    expect(DEFAULT_THEME).toBe('light');
    expect(getTheme()).toBe('light');

    localStorage.setItem('cc_theme', 'no-such-theme');
    expect(getTheme()).toBe('light');
    applyTheme();
    expect(document.documentElement.dataset.theme).toBe('light');
    expect(
      document.querySelector('meta[name="theme-color"]')!.getAttribute('content'),
    ).toBe('#e9e9ec');
  });

  it('首屏 bootstrap 在应用启动前恢复已保存的深色主题与浏览器 chrome', () => {
    localStorage.setItem('cc_theme', 'dark');
    applyPrepaintTheme();

    expect(document.documentElement.dataset.theme).toBe('dark');
    expect(document.querySelector('meta[name="theme-color"]')!.getAttribute('content')).toBe('#131316');
    expect(
      document.querySelector('meta[name="apple-mobile-web-app-status-bar-style"]')!.getAttribute('content'),
    ).toBe('black-translucent');
  });

  it('首屏 bootstrap 对缺失或无效偏好使用浅色默认值', () => {
    applyPrepaintTheme();
    expect(document.documentElement.dataset.theme).toBe('light');
    expect(document.querySelector('meta[name="theme-color"]')!.getAttribute('content')).toBe('#e9e9ec');

    localStorage.setItem('cc_theme', 'invalid');
    document.documentElement.dataset.theme = 'dark';
    applyPrepaintTheme();
    expect(document.documentElement.dataset.theme).toBe('light');
  });

  it.each(THEME_OPTIONS.filter((option) => option.value !== 'custom'))(
    '首屏 bootstrap 与权威注册表同步恢复 $value',
    (option) => {
      localStorage.setItem('cc_theme', option.value);
      expect(applyPrepaintTheme()).toBe(option.value);
      expect(document.documentElement.dataset.theme).toBe(option.value);
      expect(document.querySelector('meta[name="theme-color"]')!.getAttribute('content'))
        .toBe(option.themeColor);
      expect(
        document.querySelector('meta[name="apple-mobile-web-app-status-bar-style"]')!.getAttribute('content'),
      ).toBe(option.scheme === 'light' ? 'default' : 'black-translucent');
    },
  );

  it.each([
    ['light', '#e9e9ec', '#4f7cf7'],
    ['dark', '#131316', '#16a34a'],
  ] as const)('首屏完整恢复 %s 自定义主题', (expectedScheme, bg, brand) => {
    setCustomColors(bg, brand);
    localStorage.setItem('cc_theme', 'custom');
    const expected = buildCustomTheme(bg, brand);

    expect(applyPrepaintTheme()).toBe('custom');
    expect(document.documentElement.dataset.theme).toBe('custom');
    expect(document.documentElement.dataset.scheme).toBe(expectedScheme);
    expect(document.querySelector('meta[name="theme-color"]')!.getAttribute('content')).toBe(bg);
    expect(
      document.querySelector('meta[name="apple-mobile-web-app-status-bar-style"]')!.getAttribute('content'),
    ).toBe(expectedScheme === 'light' ? 'default' : 'black-translucent');
    for (const [name, value] of Object.entries(expected.vars)) {
      expect(document.documentElement.style.getPropertyValue(name), name).toBe(value);
    }
  });

  it('首屏恢复已保存的自定义背景图快照', () => {
    setCustomColors('#131316', '#4f7cf7');
    localStorage.setItem('cc_theme', 'custom');
    localStorage.setItem('cc_theme_custom_has_bg', '1');
    localStorage.setItem('ccm-theme-bootstrap-image', 'data:image/jpeg;base64,ZmFrZQ==');

    expect(applyPrepaintTheme()).toBe('custom');
    expect(applyBootstrapBgImage()).toBe(true);
    expect(document.documentElement.dataset.hasBg).toBe('1');
    expect(document.documentElement.style.getPropertyValue('--ccm-bg-url')).toContain('data:image/jpeg');
    expect(document.documentElement.style.getPropertyValue('--ccm-bg-scrim')).toContain('oklch(');
  });

  it('首屏没有有效背景快照时回退到壳色并清除坏快照', () => {
    setCustomColors('#131316', '#4f7cf7');
    localStorage.setItem('cc_theme', 'custom');
    localStorage.setItem('cc_theme_custom_has_bg', '1');
    localStorage.setItem('ccm-theme-bootstrap-image', `data:image/jpeg;base64,${'x'.repeat(350_001)}`);

    expect(applyPrepaintTheme()).toBe('custom');
    expect(localStorage.getItem('ccm-theme-bootstrap-image')).toBeNull();
    expect(document.documentElement.dataset.hasBg).toBeUndefined();
    expect(document.documentElement.style.backgroundColor).toBe('var(--color-gray-950)');
  });

  it('运行时切换主题同步 Apple 状态栏对比度', () => {
    setTheme('dark');
    expect(
      document.querySelector('meta[name="apple-mobile-web-app-status-bar-style"]')!.getAttribute('content'),
    ).toBe('black-translucent');

    setTheme('light');
    expect(
      document.querySelector('meta[name="apple-mobile-web-app-status-bar-style"]')!.getAttribute('content'),
    ).toBe('default');
  });
});

describe('index.css 主题变量覆盖完整性', () => {
  // 现代组非 CSS 基础主题（dark 定义在 @theme 里）都必须覆盖全档
  const modernOverrideThemes = THEME_OPTIONS.filter(
    (o) => o.group === 'modern' && o.value !== 'dark',
  ).map((o) => o.value);

  it.each(modernOverrideThemes)('%s 覆盖 gray + indigo 全档', (theme) => {
    const block = themeBlock(theme);
    for (const shade of REQUIRED_GRAY) {
      expect(block, `${theme} 缺 --color-gray-${shade}`).toContain(`--color-gray-${shade}:`);
    }
    for (const shade of REQUIRED_INDIGO) {
      expect(block, `${theme} 缺 --color-indigo-${shade}`).toContain(`--color-indigo-${shade}:`);
    }
    expect(block).toContain('--color-foreground:');
    expect(block).toContain('--ring:');
  });

  it('浅色主题声明 color-scheme: light 并深色化 accent 300/400 档', () => {
    for (const opt of THEME_OPTIONS.filter((o) => o.group === 'modern' && o.scheme === 'light')) {
      const block = themeBlock(opt.value);
      expect(block, `${opt.value} 缺 color-scheme`).toContain('color-scheme: light');
      // 浅色兼容规则：chip 文字用的 accent 300/400 必须反转为深色调
      for (const accent of ['red', 'orange', 'green', 'yellow', 'blue', 'purple']) {
        expect(block, `${opt.value} 缺 --color-${accent}-400`).toContain(`--color-${accent}-400:`);
        expect(block, `${opt.value} 缺 --color-${accent}-300`).toContain(`--color-${accent}-300:`);
      }
    }
  });

  it('飞书主题使用官方品牌蓝与中性色 token（截图实证）', () => {
    const block = themeBlock('feishu');
    expect(block).toContain('--color-indigo-600: #3370ff'); // 经典飞书蓝 B500（App 截图取色 #316efa 实证）
    expect(block).toContain('--color-indigo-500: #245bdb'); // B600 hover 向深走
    expect(block).toContain('--color-gray-100: #1f2329'); // N900 主文字
    expect(block).toContain('--color-gray-800: #ffffff'); // 卡片纯白
    expect(block).toContain('--color-gray-950: #ecedef'); // 侧栏壳 = 飞书 rail 灰（截图取色）
    expect(block).toContain('--color-gray-900: #fbfbfc'); // 画布近白：白底为主，区别于「现代浅色」的灰画布
  });

  it('飞书主题与现代浅色不趋同（白底为主 vs 灰画布）', () => {
    // 回归守卫：feishu 的画布(gray-900)必须显著白于 light 的画布，
    // 否则两个主题肉眼无法区分（2026-07-16 用户反馈）
    const feishu = themeBlock('feishu');
    const light = themeBlock('light');
    expect(feishu).toContain('--color-gray-900: #fbfbfc');
    // light 画布保持色调分层灰（tonal zinc），确保没人把两边改成同一取值
    expect(light).toContain('--color-gray-900: oklch(95.8% 0.002 286)');
    expect(light).toContain('--color-gray-950: oklch(92.5% 0.003 286)');
  });

  it('苹果主题使用 Apple 官方色板 token（apple.com CSS / iOS systemGray 系）', () => {
    const block = themeBlock('apple');
    expect(block).toContain('--color-indigo-600: #0071e3'); // apple.com CTA 按钮蓝（官网 CSS 实测）
    expect(block).toContain('--color-indigo-500: #0077ed'); // hover 向亮走一档（apple.com 实测）
    expect(block).toContain('--color-gray-900: #f7f7f7'); // 画布 = Settings 内容区（官方手册截图实测）
    expect(block).toContain('--color-gray-800: #ffffff'); // 卡片纯白
    expect(block).toContain('--color-gray-950: #f9f9f9'); // 侧栏 = Settings 侧栏（略亮于画布，实测）
    expect(block).toContain('--color-gray-750: #f2f2f7'); // iOS systemGray6
    expect(block).toContain('--color-gray-700: #e5e5ea'); // 分隔线 = iOS systemGray5
    expect(block).toContain('--color-gray-500: #8e8e93'); // iOS systemGray
    expect(block).toContain('--color-gray-100: #1d1d1f'); // apple.com 主文字
    expect(block).toContain('--ring: #0071e3');
  });

  it('苹果主题遵循 apple-design skill §15：平台系统字体优先', () => {
    // skill §15：系统字体自带 optical sizing 与 tracking 表，优先于自定义字体
    const block = themeBlock('apple');
    expect(block).toMatch(/--font-sans:\s*-apple-system/);
    expect(block).toMatch(/--font-mono:\s*ui-monospace/);
  });

  it('苹果主题交互细节遵循 skill §1/§12/§14', () => {
    // §1 Response：按压即时反馈。用独立 scale 属性（不覆盖 transform 工具类）
    const pressIdx = indexCss.indexOf("html[data-theme='apple'] button:active");
    expect(pressIdx, '缺按压反馈规则').toBeGreaterThan(-1);
    // §14：按压动效必须包在 prefers-reduced-motion 守卫内
    const motionGuard = indexCss.lastIndexOf(
      '@media (prefers-reduced-motion: no-preference)',
      pressIdx,
    );
    expect(motionGuard, '按压反馈未被 reduced-motion 守卫').toBeGreaterThan(-1);
    // §12 Materials：半透明材质顶栏（backdrop blur，内容从下方滚过）
    const chromeIdx = indexCss.indexOf("html[data-theme='apple'] header.sticky");
    expect(chromeIdx, '缺材质顶栏规则').toBeGreaterThan(-1);
    expect(indexCss.slice(chromeIdx, chromeIdx + 500)).toContain('backdrop-filter');
    // §14：prefers-reduced-transparency 时回退实底
    expect(indexCss).toContain('@media (prefers-reduced-transparency: reduce)');
  });

  it('三个现代浅色主题画布互不趋同（light 灰调分层 / feishu 近白 / apple 苹果灰）', () => {
    // 回归守卫：三者取值必须保持可区分（沿用 2026-07-16 light vs feishu 防趋同教训）
    expect(themeBlock('apple')).toContain('--color-gray-900: #f7f7f7');
    expect(themeBlock('feishu')).toContain('--color-gray-900: #fbfbfc');
    expect(themeBlock('light')).toContain('--color-gray-900: oklch(95.8% 0.002 286)');
  });

  it('三个现代浅色主题以形状语言互相区分（圆角差异化，2026-07-17）', () => {
    // 用户反馈三浅色主题肉眼无差异后确立：屏幕 90% 是白卡片，画布灰度 hex
    // 撑不起辨识度，必须有一眼可辨的形状/表面语言差异。
    // feishu 紧凑方正：官网 CSS 圆角以 4/6/8px 为主（feishucdn app-*.css 实测统计）
    const feishu = themeBlock('feishu');
    expect(feishu).toContain('--radius-md: 0.375rem');
    expect(feishu).toContain('--radius-lg: 0.375rem');
    expect(feishu).toContain('--radius-xl: 0.5rem');
    // apple 大圆角：apple.com 卡片 16-28px、iOS 分组卡片 10pt 起
    const apple = themeBlock('apple');
    expect(apple).toContain('--radius-lg: 1rem');
    expect(apple).toContain('--radius-xl: 1.25rem');
    expect(apple).toContain('--radius-2xl: 1.5rem');
    // light 保持默认圆角（不覆盖），作为中间基准
    expect(themeBlock('light')).not.toContain('--radius-');
  });

  it('苹果主题卡片用软阴影悬浮（iOS 分组卡片语言），不覆盖 shadow-* 工具类', () => {
    const idx = indexCss.indexOf(
      "html[data-theme='apple'] [class~='bg-gray-800']:not([class*='shadow'])",
    );
    expect(idx, '缺卡片软阴影规则').toBeGreaterThan(-1);
    expect(indexCss.slice(idx, idx + 400)).toContain('box-shadow');
  });

  it('结构级复刻层：飞书窄图标 rail（仅桌面端）+ 选中蓝 tint 方块', () => {
    // 2026-07-17 用户要求激进复刻后确立：飞书桌面侧栏 = 76px 图标 rail
    const railIdx = indexCss.indexOf("html[data-theme='feishu'] [data-shell-sidebar]");
    expect(railIdx, '缺飞书 rail 规则').toBeGreaterThan(-1);
    const mediaIdx = indexCss.lastIndexOf('@media (min-width: 64rem)', railIdx);
    expect(mediaIdx, '飞书 rail 必须包在 lg+ media query 内（移动端抽屉保持行布局）').toBeGreaterThan(-1);
    // 主列 padding 必须跟随 rail 宽度，否则内容被 240px 空档顶开
    expect(indexCss).toContain("html[data-theme='feishu'] [data-shell-main]");
    // 选中项 = 白色圆角 tile 包住图标+文字（iPad 官方截图实测 tile ≈白）
    expect(indexCss).toMatch(
      /html\[data-theme='feishu'\] \[data-shell-sidebar\] \[data-nav-item\]\[data-active='true'\] \{\s*background: #ffffff;\s*color: #3370ff;/,
    );
    // 头像置顶（飞书 rail 顶部 = 用户头像）
    expect(indexCss).toMatch(
      /html\[data-theme='feishu'\] \[data-shell-user-footer\] \{\s*order: -1;/,
    );
  });

  it('结构级复刻层：苹果 macOS Settings 侧栏 + 胶囊按钮', () => {
    // iOS 系统色 squircle 图标轮换
    expect(indexCss).toContain(':nth-of-type(10n + 1) svg { background: #007aff; }');
    expect(indexCss).toContain(':nth-of-type(10n + 10) svg { background: #ff2d55; }');
    // 选中行 = 实底 systemBlue 白字（macOS 侧栏选中语言）
    expect(indexCss).toMatch(
      /html\[data-theme='apple'\] \[data-nav-item\]\[data-active='true'\] \{\s*background: #0071e3;\s*color: #ffffff;/,
    );
    // apple.com 语言：按钮胶囊化；导航项以更高优先级覆盖回 Settings 的 6px
    expect(indexCss).toMatch(/html\[data-theme='apple'\] button \{\s*border-radius: 9999px;/);
    expect(indexCss).toMatch(/html\[data-theme='apple'\] \[data-nav-item\] \{[^}]*border-radius: 6px;/);
    // Settings 侧栏顶部搜索框（装饰性）+ 用户行上移（Apple 账户行位置）
    expect(indexCss).toContain("html[data-theme='apple'] [data-shell-sidebar]::before");
    expect(indexCss).toMatch(/\[data-shell-sidebar\]::before \{\s*content: 'Search';/);
  });

  it('品牌蓝实底上有白色选中高亮覆盖（蓝底蓝高亮不可见问题）', () => {
    // 回归守卫：全局 ::selection 是品牌蓝，用户气泡/主按钮是 bg-indigo-600
    // 实底蓝，必须有词级匹配的白色半透明覆盖（2026-07-16 用户反馈）
    expect(indexCss).toContain("[class~='bg-indigo-600']::selection");
    expect(indexCss).toContain("[class~='bg-indigo-600'] *::selection");
  });
});
