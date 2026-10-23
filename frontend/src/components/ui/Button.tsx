import type { AnchorHTMLAttributes, ButtonHTMLAttributes, ReactNode } from 'react';
import styles from './Button.module.css';

type Variant = 'primary' | 'secondary' | 'ghost';
type Size = 'md' | 'lg';

interface CommonProps {
  variant?: Variant;
  size?: Size;
  icon?: ReactNode;
  /** Trailing element: an arrow or a count. */
  trailing?: ReactNode;
  block?: boolean;
  children?: ReactNode;
}

function classes(variant: Variant, size: Size, block?: boolean, extra?: string) {
  return [styles.button, styles[variant], styles[size], block ? styles.block : '', extra ?? ''].join(' ');
}

function Content({ icon, trailing, children }: CommonProps) {
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
  block,
  children,
  className,
  type = 'button',
  ...rest
}: CommonProps & ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button type={type} className={classes(variant, size, block, className)} {...rest}>
      <Content icon={icon} trailing={trailing}>
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
