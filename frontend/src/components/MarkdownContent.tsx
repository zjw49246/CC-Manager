import { memo, useState } from 'react';
import type { ReactElement, ReactNode } from 'react';
import type { Components } from 'react-markdown';

import { copyToClipboard } from './clipboard';
import { Check, Copy } from './icons';
import { MarkdownRenderer } from './Markdown/MarkdownRenderer';

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  const handleCopy = () => {
    copyToClipboard(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };
  return (
    <button
      onClick={handleCopy}
      className="copy-btn absolute right-2 top-2 rounded bg-gray-700/80 p-1 text-gray-400 opacity-0 transition-opacity hover:bg-gray-600 hover:text-gray-200 group-hover:pointer-events-auto group-hover:opacity-100 pointer-events-none"
      title="Copy"
    >
      {copied ? <Check size={12} /> : <Copy size={12} />}
    </button>
  );
}

const markdownComponents: Components = {
  pre({ children }) {
    let codeText = '';
    if (children && typeof children === 'object' && 'props' in children) {
      const codeElement = children as ReactElement<{ children?: ReactNode }>;
      codeText = typeof codeElement.props.children === 'string'
        ? codeElement.props.children
        : '';
    }
    return (
      <div className="group relative my-2 min-w-0 max-w-full">
        {codeText && <CopyButton text={codeText} />}
        <pre className="max-w-full overflow-x-auto rounded-lg bg-gray-900 p-3 text-xs">{children}</pre>
      </div>
    );
  },
  code({ className, children, ...props }) {
    const isInline = !className;
    if (isInline) {
      return (
        <code className="rounded bg-gray-700/60 px-1.5 py-0.5 text-xs" {...props}>
          {children}
        </code>
      );
    }
    return <code className={`${className || ''} text-xs`} {...props}>{children}</code>;
  },
  a({ href, children }) {
    return (
      <a
        href={href}
        target="_blank"
        rel="noopener noreferrer"
        className="text-indigo-400 underline hover:text-indigo-300"
      >
        {children}
      </a>
    );
  },
  table({ children }) {
    return (
      <div className="my-2 overflow-x-auto">
        <table className="w-full border-collapse text-xs">{children}</table>
      </div>
    );
  },
  th({ children }) {
    return <th className="border border-gray-700 bg-gray-800/50 px-2 py-1 text-left">{children}</th>;
  },
  td({ children }) {
    return <td className="border border-gray-700 px-2 py-1">{children}</td>;
  },
};

interface MarkdownContentProps {
  content: string;
  className?: string;
}

export const MarkdownContent = memo(function MarkdownContent({
  content,
  className,
}: MarkdownContentProps) {
  return (
    <div className={`markdown-body min-w-0 max-w-full ${className || ''}`}>
      <MarkdownRenderer
        components={markdownComponents}
        content={content}
      />
    </div>
  );
});
