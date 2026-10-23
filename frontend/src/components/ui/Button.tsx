import type { AnchorHTMLAttributes, ButtonHTMLAttributes, ReactNode } from 'react';
import { CheckIcon } from '../icons';
import { Waveform } from './Waveform';
import styles from './Button.module.css';

type Variant = 'primary' | 'secondary' | 'ghost';
type Size = 'md' | 'lg';
export type ButtonStatus = 'idle' | 'loading' | 'success';

interface CommonProps {
  variant?: Variant;
  size?: Size;
  icon?: ReactNode;
  /** Trailing element: an arrow or a count. */
  trailing?: ReactNode;
  status?: ButtonStatus;
  successLabel?: ReactNode;
  block?: boolean;
  children?: ReactNode;
}

function classes(variant: Variant, size: Size, block?: boolean, extra?: string) {
  return [styles.button, styles[variant], styles[size], block ? styles.block : '', extra ?? ''].join(' ');
}

function Content({ icon, trailing, status = 'idle', successLabel, children }: CommonProps) {
  if (status === 'loading') {
    return (
      <span className={styles.center}>
        <span className="sr-only">טוען</span>
        <Waveform />
      </span>
    );
  }
  if (status === 'success') {
    return (
      <>
        <span className={styles.label}>{successLabel ?? children}</span>
        <CheckIcon size={18} />
      </>
    );
  }
  return (
    <>
      {icon}
      <span className={styles.label}>{children}</span>
      {trailing && <span className={styles.trailing}>{trailing}</span>}
    </>
  );
}

export function Button({
  variant = 'secondary',
  size = 'md',
  icon,
  trailing,
  status,
  successLabel,
  block,
  children,
  className,
  type = 'button',
  ...rest
}: CommonProps & ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button
      type={type}
      className={classes(variant, size, block, className)}
      aria-busy={status === 'loading' || undefined}
      {...rest}
    >
      <Content icon={icon} trailing={trailing} status={status} successLabel={successLabel}>
        {children}
      </Content>
    </button>
  );
}

/** Same look as Button, for navigation and downloads. */
export function ButtonLink({
  variant = 'secondary',
  size = 'md',
  icon,
  trailing,
  block,
  children,
  className,
  ...rest
}: CommonProps & AnchorHTMLAttributes<HTMLAnchorElement>) {
  return (
    <a className={classes(variant, size, block, className)} {...rest}>
      <Content icon={icon} trailing={trailing}>
        {children}
      </Content>
    </a>
  );
}
