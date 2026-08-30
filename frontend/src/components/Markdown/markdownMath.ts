import { decodeString } from 'micromark-util-decode-string';

interface MarkdownPosition {
  start?: { offset?: number };
  end?: { offset?: number };
}

interface MarkdownNode {
  type: string;
  value?: string;
  children?: MarkdownNode[];
  position?: MarkdownPosition;
  data?: Record<string, unknown>;
}

interface MarkdownFile {
  value?: unknown;
}

const SKIP_DESCENDANTS = new Set([
  'code',
  'definition',
  'html',
  'image',
  'imageReference',
  'inlineCode',
  'link',
  'linkReference',
  'math',
  'inlineMath',
]);

const MAX_DISPLAY_MATH_SOURCE_LENGTH = 100_000;

function normalizeMathValue(value: string): string {
  // Models occasionally escape a superscript star as `x^\*`. KaTeX treats
  // `\*` as an unknown command and paints it red; a bare `*` is the intended
  // TeX token. Restrict this repair to confirmed math nodes and leave `\\*`
  // (a line break followed by a star) untouched.
  return value.replace(/(^|[^\\])\\\*/g, '$1*');
}

function replaceFirstHastTextValue(node: unknown, value: string): boolean {
  if (!node || typeof node !== 'object') return false;
  const candidate = node as {
    type?: unknown;
    value?: unknown;
    children?: unknown;
  };
  if (candidate.type === 'text') {
    candidate.value = value;
    return true;
  }
  if (!Array.isArray(candidate.children)) return false;
  return candidate.children.some((child) => replaceFirstHastTextValue(child, value));
}

function normalizeExistingMathNode(node: MarkdownNode): void {
  if (typeof node.value !== 'string') return;
  const normalizedValue = normalizeMathValue(node.value);
  if (normalizedValue === node.value) return;
  node.value = normalizedValue;

  // mdast-util-math stores a second copy for mdast-to-hast. Keep that copy in
  // sync so `$$...$$` nodes receive the same repair as backslash-delimited math.
  const hChildren = node.data?.hChildren;
  if (Array.isArray(hChildren)) {
    hChildren.some((child) => replaceFirstHastTextValue(child, normalizedValue));
  }
}

function isMarkdownNode(value: unknown): value is MarkdownNode {
  return Boolean(
    value
    && typeof value === 'object'
    && typeof (value as { type?: unknown }).type === 'string',
  );
}

function sourceForNode(node: MarkdownNode, source: string): string | null {
  const start = node.position?.start?.offset;
  const end = node.position?.end?.offset;
  if (
    typeof start !== 'number'
    || typeof end !== 'number'
    || start < 0
    || end < start
    || end > source.length
  ) {
    return null;
  }
  return source.slice(start, end);
}

function isEscaped(source: string, index: number): boolean {
  let precedingBackslashes = 0;
  for (let cursor = index - 1; cursor >= 0 && source[cursor] === '\\'; cursor -= 1) {
    precedingBackslashes += 1;
  }
  return precedingBackslashes % 2 === 1;
}

function findDelimiter(
  source: string,
  closingCharacter: '(' | ')' | '[' | ']',
  fromIndex: number,
  toIndex: number = source.length,
): number {
  const boundedEnd = Math.min(toIndex, source.length);
  for (let index = fromIndex; index + 1 < boundedEnd; index += 1) {
    if (
      source[index] === '\\'
      && source[index + 1] === closingCharacter
      && !isEscaped(source, index)
    ) {
      return index;
    }
  }
  return -1;
}

function inlineMathNode(value: string): MarkdownNode {
  const normalizedValue = normalizeMathValue(value);
  return {
    type: 'inlineMath',
    value: normalizedValue,
    data: {
      hName: 'code',
      hProperties: { className: ['language-math', 'math-inline'] },
      hChildren: [{ type: 'text', value: normalizedValue }],
    },
  };
}

function displayMathNode(value: string): MarkdownNode {
  const normalizedValue = normalizeMathValue(value);
  return {
    type: 'math',
    value: normalizedValue,
    data: {
      hName: 'pre',
      hChildren: [{
        type: 'element',
        tagName: 'code',
        properties: { className: ['language-math', 'math-display'] },
        children: [{ type: 'text', value: normalizedValue }],
      }],
    },
  };
}

function stripOneLineEnding(value: string, fromStart: boolean): string {
  if (fromStart) return value.replace(/^(?:\r\n|\r|\n)/, '');
  return value.replace(/(?:\r\n|\r|\n)$/, '');
}

function containsProtectedNode(node: MarkdownNode): boolean {
  return SKIP_DESCENDANTS.has(node.type)
    || Boolean(node.children?.some((child) => containsProtectedNode(child)));
}

function findDisplayMathOpening(node: MarkdownNode, source: string): {
  start: number;
  opening: number;
} | null {
  const start = node.position?.start?.offset;
  const end = node.position?.end?.offset;
  if (
    typeof start !== 'number'
    || typeof end !== 'number'
    || start < 0
    || end < start
    || end > source.length
  ) {
    return null;
  }

  let opening = start;
  while (opening < end && (source[opening] === ' ' || source[opening] === '\t')) {
    opening += 1;
  }
  if (opening + 1 >= end || source[opening] !== '\\' || source[opening + 1] !== '[') {
    return null;
  }
  return { start, opening };
}

function containsOnlyHorizontalWhitespace(source: string, start: number, end: number): boolean {
  for (let index = start; index < end; index += 1) {
    if (source[index] !== ' ' && source[index] !== '\t') return false;
  }
  return true;
}

function parseDisplayMathRange(
  children: MarkdownNode[],
  startIndex: number,
  source: string,
): { node: MarkdownNode; endIndex: number } | null {
  const openingRange = findDisplayMathOpening(children[startIndex], source);
  if (!openingRange) return null;

  const { start, opening } = openingRange;
  let searchFrom = opening + 2;
  let previousEnd = start;

  for (let endIndex = startIndex; endIndex < children.length; endIndex += 1) {
    if (containsProtectedNode(children[endIndex])) return null;
    const end = children[endIndex].position?.end?.offset;
    if (
      typeof end !== 'number'
      || end < previousEnd
      || end > source.length
    ) {
      return null;
    }
    if (end - start > MAX_DISPLAY_MATH_SOURCE_LENGTH) return null;

    const closing = findDelimiter(source, ']', searchFrom, end);
    if (closing >= 0) {
      if (!containsOnlyHorizontalWhitespace(source, closing + 2, end)) return null;

      let value = source.slice(opening + 2, closing);
      value = stripOneLineEnding(value, true);
      value = stripOneLineEnding(value, false);
      return { node: displayMathNode(value), endIndex };
    }

    // Keep one character of overlap so a delimiter split at an AST boundary
    // is still found, while every other source character is scanned once.
    searchFrom = Math.max(searchFrom, end - 1);
    previousEnd = end;
  }
  return null;
}

function splitInlineMath(node: MarkdownNode, source: string): MarkdownNode[] | null {
  const raw = sourceForNode(node, source);
  if (raw === null) return null;

  const transformed: MarkdownNode[] = [];
  let cursor = 0;
  let searchFrom = 0;
  let found = false;

  while (searchFrom < raw.length - 1) {
    const opening = findDelimiter(raw, '(', searchFrom);
    if (opening < 0) break;

    const closing = findDelimiter(raw, ')', opening + 2);
    const nextOpening = findDelimiter(raw, '(', opening + 2);
    const lineEnding = raw.slice(opening + 2).search(/[\r\n]/);
    if (
      closing < 0
      || (lineEnding >= 0 && opening + 2 + lineEnding < closing)
    ) {
      searchFrom = opening + 2;
      continue;
    }
    if (nextOpening >= 0 && nextOpening < closing) {
      searchFrom = nextOpening;
      continue;
    }

    if (opening > cursor) {
      transformed.push({ type: 'text', value: decodeString(raw.slice(cursor, opening)) });
    }
    transformed.push(inlineMathNode(raw.slice(opening + 2, closing)));
    cursor = closing + 2;
    searchFrom = cursor;
    found = true;
  }

  if (!found) return null;
  if (cursor < raw.length) {
    transformed.push({ type: 'text', value: decodeString(raw.slice(cursor)) });
  }
  return transformed;
}

function transformChildren(parent: MarkdownNode, source: string): void {
  if (!parent.children || SKIP_DESCENDANTS.has(parent.type)) return;

  const transformed: MarkdownNode[] = [];
  for (let index = 0; index < parent.children.length; index += 1) {
    const displayMath = parseDisplayMathRange(parent.children, index, source);
    if (displayMath) {
      transformed.push(displayMath.node);
      index = displayMath.endIndex;
      continue;
    }

    const child = parent.children[index];
    if (
      (child.type === 'math' || child.type === 'inlineMath')
      && typeof child.value === 'string'
    ) {
      normalizeExistingMathNode(child);
    }
    if (child.type === 'text') {
      transformed.push(...(splitInlineMath(child, source) || [child]));
      continue;
    }

    transformChildren(child, source);
    transformed.push(child);
  }
  parent.children = transformed;
}

/**
 * Parse Codex's `\\(...\\)` and `\\[...\\]` notation after Markdown has
 * formed its AST. Source positions recover the escaped delimiters without
 * rewriting URLs, HTML, code, definitions, images, or link destinations.
 * Inline pairs are deliberately confined to one text node. Display math may
 * span sibling AST nodes because TeX lines such as a standalone `=` can be
 * parsed as Markdown headings; the explicit delimiters still have to occupy
 * the complete source range within one container. Narrow compatibility fixes
 * are applied only after content has been identified as a math node.
 */
export function remarkBackslashMath() {
  return (tree: unknown, file: MarkdownFile): void => {
    if (!isMarkdownNode(tree) || typeof file.value !== 'string') return;
    transformChildren(tree, file.value);
  };
}
