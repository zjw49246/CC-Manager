import { render } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { MarkdownRenderer } from './MarkdownRenderer';
import { remarkBackslashMath } from './markdownMath';

interface CapturedNode {
  type?: string;
  value?: string;
  children?: CapturedNode[];
  position?: {
    start?: { offset?: number };
    end?: { offset?: number };
  };
}

function nodesOfType(root: CapturedNode | null, type: string): CapturedNode[] {
  if (!root) return [];
  const matches = root.type === type ? [root] : [];
  for (const child of root.children || []) {
    matches.push(...nodesOfType(child, type));
  }
  return matches;
}

describe('MarkdownRenderer math support', () => {
  it('renders Codex backslash inline and whole-paragraph display math', () => {
    const markdown = String.raw`Inline \(q_1\) remains in prose.

\[
\nabla_z L_{\text{SFT}}=p_s-e_y
\]`;
    const { container } = render(<MarkdownRenderer content={markdown} />);

    expect(container.querySelectorAll('.katex')).toHaveLength(2);
    expect(container.querySelector('.katex-display')).not.toBeNull();
    expect(container.textContent).toContain('∇');
    expect(container.querySelector('.katex-html')).not.toBeNull();
  });

  it('renders display math split into Markdown nodes by a standalone equals line', () => {
    const markdown = String.raw`Diagonal AdaGrad gives

\[
R_T
=
\sum_{t=1}^T f_t(x_t)-\sum_{t=1}^T f_t(x^\*)
\le
O\left(\sum_{i=1}^d D_i \sqrt{G_{T,i}}\right)
\]`;
    const { container } = render(<MarkdownRenderer content={markdown} />);

    expect(container.querySelectorAll('.katex-display')).toHaveLength(1);
    expect(container.querySelector('.katex-error')).toBeNull();
    expect(container.querySelector('h1, h2')).toBeNull();
    expect(container.querySelector('.katex-html')?.textContent).toContain('≤');
  });

  it('renders compact display math nested in a list item', () => {
    const markdown = String.raw`- Gaussian lower bound

  \[ \Omega\!\left(\min\left\{\frac{\sigma^2 A^2 d^2}{\eta^4},\frac{\sigma^2 H R^2 d^2}{\eta^3}\right\}\right) \]`;
    const { container } = render(<MarkdownRenderer content={markdown} />);

    expect(container.querySelector('li .katex-display')).not.toBeNull();
    expect(container.querySelector('li .katex-html')?.textContent).toContain('Ω');
  });

  it('scans a large unclosed display formula without repeatedly copying prefixes', () => {
    const chunks = [String.raw`\[`, ...Array.from({ length: 20_000 }, () => 'x')];
    const source = chunks.join('\n\n');
    let offset = 0;
    const children = chunks.map((chunk) => {
      const start = offset;
      const end = start + chunk.length;
      offset = end + 2;
      return {
        type: 'paragraph',
        position: { start: { offset: start }, end: { offset: end } },
        children: [{
          type: 'text',
          value: chunk,
          position: { start: { offset: start }, end: { offset: end } },
        }],
      };
    });
    const tree = {
      type: 'root',
      position: { start: { offset: 0 }, end: { offset: source.length } },
      children,
    };

    const originalSlice = String.prototype.slice;
    let copiedCharacters = 0;
    const sliceSpy = vi.spyOn(String.prototype, 'slice').mockImplementation(function (
      this: string,
      start?: number,
      end?: number,
    ) {
      const result = originalSlice.call(this, start, end);
      copiedCharacters += result.length;
      return result;
    });

    let elapsedMs = 0;
    try {
      const startedAt = performance.now();
      remarkBackslashMath()(tree, { value: source });
      elapsedMs = performance.now() - startedAt;
    } finally {
      sliceSpy.mockRestore();
    }

    expect(source.length).toBeGreaterThan(60_000);
    expect(copiedCharacters).toBeLessThan(source.length * 2);
    expect(elapsedMs).toBeLessThan(1_000);
  });

  it('preserves fenced code nested between display delimiters', () => {
    const markdown = [
      String.raw`\[`,
      '',
      '> ```tex',
      '> protected_code',
      '> ```',
      '',
      String.raw`\]`,
    ].join('\n');
    const { container } = render(<MarkdownRenderer content={markdown} />);

    expect(container.querySelector('.katex')).toBeNull();
    expect(container.querySelector('blockquote pre code')?.textContent).toContain('protected_code');
  });

  it('preserves a link nested between display delimiters', () => {
    const markdown = String.raw`\[

> [protected link](https://example.test/protected)

\]`;
    const { container } = render(<MarkdownRenderer content={markdown} />);

    expect(container.querySelector('.katex')).toBeNull();
    expect(container.querySelector('blockquote a')?.getAttribute('href')).toBe(
      'https://example.test/protected',
    );
  });

  it('repairs an escaped superscript star only inside confirmed math nodes', () => {
    const markdown = [
      String.raw`Inline \(x^\*\).`,
      '',
      '```tex',
      String.raw`x^\*`,
      '```',
      '',
      '$$',
      String.raw`y^{\*}`,
      '$$',
    ].join('\n');
    const { container } = render(<MarkdownRenderer content={markdown} />);

    expect(container.querySelectorAll('.katex')).toHaveLength(2);
    expect(container.querySelector('[style*="color:#cc0000"]')).toBeNull();
    expect(
      Array.from(container.querySelectorAll('annotation')).map((node) => node.textContent),
    ).toEqual(['x^*', 'y^{*}']);
    expect(container.querySelector('pre code')?.textContent).toContain(String.raw`x^\*`);
  });

  it('keeps Markdown-like tokens inside whole-paragraph display math', () => {
    const markdown = String.raw`\[
\text{**not Markdown strong**}
\]`;
    const { container } = render(<MarkdownRenderer content={markdown} />);

    expect(container.querySelector('.katex-display')).not.toBeNull();
    expect(container.querySelector('strong')).toBeNull();
  });

  it('supports display dollars while leaving single-dollar prose literal', () => {
    const markdown = String.raw`The parameter $p$ stays literal.

$$
r^2
$$`;
    const { container } = render(<MarkdownRenderer content={markdown} />);

    expect(container.querySelectorAll('.katex')).toHaveLength(1);
    expect(container.querySelector('.katex-display')).not.toBeNull();
    expect(container.textContent).toContain('$p$ stays literal');
  });

  it('does not interpret ordinary currency as math', () => {
    const { container } = render(
      <MarkdownRenderer content={'Tickets cost $20 today and $30 tomorrow.'} />,
    );

    expect(container.querySelector('.katex')).toBeNull();
    expect(container.textContent).toContain('$20 today and $30 tomorrow');
  });

  it('preserves link, image, autolink, and reference destinations', () => {
    const markdown = String.raw`[docs](https://example.test/\(section\))

![plot](https://example.test/\(image\).png)

<https://example.test/\(autolink\)>

[reference][formula-ref]

[formula-ref]: https://example.test/\(reference\)`;
    const { container } = render(<MarkdownRenderer content={markdown} />);

    const docs = container.querySelector('a[href*="section"]');
    const image = container.querySelector('img');
    const autolink = container.querySelector('a[href*="autolink"]');
    const reference = container.querySelector('a[href*="reference"]');
    expect(docs?.getAttribute('href')).toBe('https://example.test/(section)');
    expect(image?.getAttribute('src')).toBe('https://example.test/(image).png');
    expect(autolink?.getAttribute('href')).not.toContain('$');
    expect(reference?.getAttribute('href')).toBe('https://example.test/(reference)');
    expect(container.querySelector('.katex')).toBeNull();
  });

  it('leaves inline HTML attributes untouched in the Markdown AST', () => {
    let capturedTree: CapturedNode | null = null;
    const captureTree = () => (tree: CapturedNode): void => {
      capturedTree = tree;
    };
    const markdown = String.raw`<span data-formula="\(not_math\)">safe</span>`;

    const { container } = render(
      <MarkdownRenderer content={markdown} remarkPlugins={[captureTree]} />,
    );

    expect(nodesOfType(capturedTree, 'html').map((node) => node.value)).toEqual([
      String.raw`<span data-formula="\(not_math\)">`,
      '</span>',
    ]);
    expect(container.querySelector('.katex')).toBeNull();
  });

  it('does not parse inline, fenced, or indented code as math', () => {
    const markdown = [
      'Inline code: `\\(not_inline_math\\)`.',
      '',
      '    \\[not_indented_math\\]',
      '',
      '```tex',
      '\\[',
      '\\nabla x',
      '\\]',
      '```',
    ].join('\n');
    const { container } = render(<MarkdownRenderer content={markdown} />);

    expect(container.querySelector('.katex')).toBeNull();
    expect(container.textContent).toContain(String.raw`\(not_inline_math\)`);
    expect(container.textContent).toContain(String.raw`\[not_indented_math\]`);
    expect(container.textContent).toContain(String.raw`\nabla x`);
  });

  it('does not pair delimiters across AST nodes or paragraphs', () => {
    const markdown = String.raw`Opening \(x

closing \) and opening display \[

x

closing display \]`;
    const { container } = render(<MarkdownRenderer content={markdown} />);

    expect(container.querySelector('.katex')).toBeNull();
    expect(container.textContent).toContain('Opening (x');
    expect(container.textContent).toContain('closing )');
  });

  it('does not let an old unmatched opener capture a later pair', () => {
    const markdown = String.raw`Old \( opening; later \(q\) is complete.`;
    const { container } = render(<MarkdownRenderer content={markdown} />);

    expect(container.querySelectorAll('.katex')).toHaveLength(1);
    expect(container.textContent).toContain('Old ( opening; later');
  });

  it('bounds KaTeX dimensions and rejects trusted links', () => {
    const markdown = String.raw`\[
\rule{1000000em}{1em}
\]

Inline \(\href{javascript:alert(1)}{click}\).`;
    const { container } = render(<MarkdownRenderer content={markdown} />);

    expect(container.querySelectorAll('.katex')).toHaveLength(2);
    expect(container.querySelector('.katex-html .rule')?.getAttribute('style')).toContain(
      'border-right-width: 20em',
    );
    expect(container.querySelector('.katex-html .rule')?.getAttribute('style')).not.toContain(
      '1000000em',
    );
    expect(container.querySelector('.katex a')).toBeNull();
  });
});
