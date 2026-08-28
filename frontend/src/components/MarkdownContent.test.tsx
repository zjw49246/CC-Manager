import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { MarkdownContent } from './MarkdownContent';


describe('MarkdownContent', () => {
  it('renders inline and display math while preserving GFM and code boundaries', () => {
    const markdown = [
      String.raw`Inline \(x^2\) and $$
\sum_{i=1}^n i
$$`,
      '| value |',
      '| --- |',
      '| [docs](https://example.test) |',
      String.raw`\`\(not math\)\` and escaped dollars: \$5 and \$7.`,
      '```tex',
      String.raw`\[not math\]`,
      '```',
    ].join('\n\n');
    const { container } = render(<MarkdownContent content={markdown} />);

    expect(container.querySelectorAll('.katex')).toHaveLength(2);
    expect(container.querySelector('.katex-display')).not.toBeNull();
    expect(container.querySelector('table')).not.toBeNull();
    expect(container.querySelector('a[href="https://example.test"]')).not.toBeNull();
    expect(container.querySelector('code')?.textContent).toContain(String.raw`\(not math\)`);
    expect(container.querySelector('pre code')?.textContent).toContain(String.raw`\[not math\]`);
    expect(container.textContent).toContain('$5 and $7');
  });

  it('keeps unfinished math readable during streaming', () => {
    const { container } = render(
      <MarkdownContent content={String.raw`Working on \(x^2`} />,
    );

    expect(container.querySelector('.katex')).toBeNull();
    expect(container.textContent).toContain('Working on (x^2');
  });

  it('constrains the document while keeping wide code locally scrollable', () => {
    const { container } = render(
      <MarkdownContent content={'Paragraph\n\n```text\n' + 'x'.repeat(500) + '\n```'} />,
    );

    const body = container.querySelector('.markdown-body');
    const pre = screen.getByText('x'.repeat(500)).closest('pre');
    expect(body).toHaveClass('min-w-0', 'max-w-full');
    expect(body).not.toHaveClass('overflow-x-auto');
    expect(pre).toHaveClass('max-w-full', 'overflow-x-auto');
  });
});
