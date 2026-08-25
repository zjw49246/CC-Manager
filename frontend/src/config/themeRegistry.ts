export interface ThemeOption {
  value: string;
  label: string;
  /** modern = v2 设计（默认深/浅）；legacy = v1 保留主题；custom = 用户自定义配色 */
  group: 'modern' | 'legacy' | 'custom';
  /** custom 的实际 scheme 由背景色亮度运行时判定，此处为名义值 */
  scheme: 'dark' | 'light';
  /** 移动端状态栏 / PWA theme-color（custom 运行时取用户所选背景） */
  themeColor: string;
  /** 可选导航图标集；缺省使用 Lucide */
  iconSet?: string;
}

export const THEME_OPTIONS = [
  { value: 'dark', label: '深色', group: 'modern', scheme: 'dark', themeColor: '#131316' },
  { value: 'light', label: '浅色', group: 'modern', scheme: 'light', themeColor: '#e9e9ec' },
  { value: 'feishu', label: '飞书', group: 'modern', scheme: 'light', themeColor: '#ecedef', iconSet: 'feishu' },
  { value: 'apple', label: '苹果', group: 'modern', scheme: 'light', themeColor: '#f9f9f9', iconSet: 'sf' },
  { value: 'legacy', label: '经典深色', group: 'legacy', scheme: 'dark', themeColor: '#030712' },
  { value: 'ocean', label: '海蓝', group: 'legacy', scheme: 'dark', themeColor: '#06131f' },
  { value: 'forest', label: '森林', group: 'legacy', scheme: 'dark', themeColor: '#07130d' },
  { value: 'rose', label: '莓红', group: 'legacy', scheme: 'dark', themeColor: '#1a0b12' },
  { value: 'custom', label: '自定义', group: 'custom', scheme: 'dark', themeColor: '#131316' },
] as const satisfies readonly ThemeOption[];

export type Theme = typeof THEME_OPTIONS[number]['value'];
export const DEFAULT_THEME: Theme = 'light';
