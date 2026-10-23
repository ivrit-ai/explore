import type { ButtonHTMLAttributes, ReactNode } from 'react';
import styles from './IconButton.module.css';

interface Props {
  /** Required: icon-only controls need an accessible name. */
  label: string;
  children: ReactNode;
}

export function IconButton({ label, children, className, type = 'button', ...rest }: Props & ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button type={type} aria-label={label} title={label} className={`${styles.icon} ${className ?? ''}`} {...rest}>
      {children}
    </button>
  );
}

