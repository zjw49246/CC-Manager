import { describe, it, expect, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ExpandableText } from './ExpandableText';

const originalScrollHeight = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'scrollHeight');
const originalClientHeight = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'clientHeight');

function simulateClamped() {
  Object.defineProperty(HTMLElement.prototype, 'scrollHeight', {
    configurable: true,
    get() { return 200; },
  });
  Object.defineProperty(HTMLElement.prototype, 'clientHeight', {
    configurable: true,
    get() { return 40; },
  });
}

function restoreLayout() {
  if (originalScrollHeight) {
    Object.defineProperty(HTMLElement.prototype, 'scrollHeight', originalScrollHeight);
  } else {
    Object.defineProperty(HTMLElement.prototype, 'scrollHeight', {
      configurable: true,
      get() { return 0; },
    });
  }
  if (originalClientHeight) {
    Object.defineProperty(HTMLElement.prototype, 'clientHeight', originalClientHeight);
  } else {
    Object.defineProperty(HTMLElement.prototype, 'clientHeight', {
      configurable: true,
      get() { return 0; },
    });
  }
}

describe('ExpandableText', () => {
  afterEach(() => {
    restoreLayout();
  });

  it('renders the text content', () => {
    render(<ExpandableText text="Hello world" />);
    expect(screen.getByTestId('expandable-text')).toHaveTextContent('Hello world');
  });

  it('does not show toggle button when text fits (scrollHeight === clientHeight)', () => {
    render(<ExpandableText text="Short" />);
    expect(screen.queryByTestId('expand-toggle')).not.toBeInTheDocument();
  });

  it('shows "Show more" button when text is clamped', () => {
    simulateClamped();
    render(<ExpandableText text="A very long text that should be clamped" collapsedLines={2} />);
    expect(screen.getByTestId('expand-toggle')).toHaveTextContent('Show more');
  });

  it('toggles between expanded and collapsed on button click', async () => {
    simulateClamped();
    const user = userEvent.setup();
    render(<ExpandableText text="Long text content" collapsedLines={2} />);

    const toggle = screen.getByTestId('expand-toggle');
    expect(toggle).toHaveTextContent('Show more');

    await user.click(toggle);
    expect(screen.getByTestId('expand-toggle')).toHaveTextContent('Show less');

    await user.click(screen.getByTestId('expand-toggle'));
    expect(screen.getByTestId('expand-toggle')).toHaveTextContent('Show more');
  });

  it('expands when clicking on the text itself', async () => {
    simulateClamped();
    const user = userEvent.setup();
    render(<ExpandableText text="Clickable long text" collapsedLines={2} />);

    const el = screen.getByTestId('expandable-text');
    await user.click(el);
    expect(screen.getByTestId('expand-toggle')).toHaveTextContent('Show less');
  });

  it('supports keyboard activation (Enter key)', async () => {
    simulateClamped();
    const user = userEvent.setup();
    render(<ExpandableText text="Keyboard accessible text" collapsedLines={2} />);

    const el = screen.getByTestId('expandable-text');
    el.focus();
    await user.keyboard('{Enter}');
    expect(screen.getByTestId('expand-toggle')).toHaveTextContent('Show less');
  });

  it('supports keyboard activation (Space key)', async () => {
    simulateClamped();
    const user = userEvent.setup();
    render(<ExpandableText text="Space key text" collapsedLines={2} />);

    const el = screen.getByTestId('expandable-text');
    el.focus();
    await user.keyboard(' ');
    expect(screen.getByTestId('expand-toggle')).toHaveTextContent('Show less');
  });

  it('applies custom className', () => {
    render(<ExpandableText text="Styled text" className="text-red-500 text-sm" />);
    const el = screen.getByTestId('expandable-text');
    expect(el.className).toContain('text-red-500');
    expect(el.className).toContain('text-sm');
  });

  it('applies expandedClassName when expanded', async () => {
    simulateClamped();
    const user = userEvent.setup();
    render(
      <ExpandableText
        text="Styled expanded text"
        className="collapsed-style"
        expandedClassName="expanded-style"
        collapsedLines={2}
      />
    );

    expect(screen.getByTestId('expandable-text').className).toContain('collapsed-style');

    await user.click(screen.getByTestId('expand-toggle'));
    expect(screen.getByTestId('expandable-text').className).toContain('expanded-style');
  });

  it('falls back to className when expandedClassName is not provided', async () => {
    simulateClamped();
    const user = userEvent.setup();
    render(
      <ExpandableText text="Fallback text" className="my-class" collapsedLines={2} />
    );

    await user.click(screen.getByTestId('expand-toggle'));
    expect(screen.getByTestId('expandable-text').className).toContain('my-class');
  });

  it('has role=button and tabIndex when clamped', () => {
    simulateClamped();
    render(<ExpandableText text="Accessible text" collapsedLines={2} />);

    const el = screen.getByTestId('expandable-text');
    expect(el).toHaveAttribute('role', 'button');
    expect(el).toHaveAttribute('tabindex', '0');
  });

  it('does not have role=button when text fits', () => {
    render(<ExpandableText text="Short" />);
    const el = screen.getByTestId('expandable-text');
    expect(el).not.toHaveAttribute('role');
  });

  it('applies line clamp style when collapsed', () => {
    render(<ExpandableText text="Some text" collapsedLines={3} />);
    const el = screen.getByTestId('expandable-text');
    expect(el.style.display).toBe('-webkit-box');
    expect(el.style.webkitLineClamp).toBe('3');
    expect(el.style.overflow).toBe('hidden');
  });

  it('removes line clamp style when expanded', async () => {
    simulateClamped();
    const user = userEvent.setup();
    render(<ExpandableText text="Clamped text" collapsedLines={2} />);

    await user.click(screen.getByTestId('expand-toggle'));
    const expandedEl = screen.getByTestId('expandable-text');
    expect(expandedEl.style.display).not.toBe('-webkit-box');
  });

  it('defaults to collapsedLines=2', () => {
    render(<ExpandableText text="Default lines" />);
    const el = screen.getByTestId('expandable-text');
    expect(el.style.webkitLineClamp).toBe('2');
  });
});
