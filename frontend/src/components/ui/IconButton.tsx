import type { AnchorHTMLAttributes, ButtonHTMLAttributes, ReactNode } from 'react';
import styles from './IconButton.module.css';

interface Props {
  /** Required: icon-only controls need an accessible name. */
  label: string;
  children: ReactNode;
  size?: 'sm' | 'md';
}

export function IconButton({ label, children, size = 'md', className, type = 'button', ...rest }: Props & ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button type={type} aria-label={label} title={label} className={`${styles.icon} ${styles[size]} ${className ?? ''}`} {...rest}>
      {children}
    </button>
  );
}

export function IconLink({ label, children, size = 'md', className, ...rest }: Props & AnchorHTMLAttributes<HTMLAnchorElement>) {
  return (
    <a aria-label={label} title={label} className={`${styles.icon} ${styles[size]} ${className ?? ''}`} {...rest}>
      {children}
    </a>
  );
}
