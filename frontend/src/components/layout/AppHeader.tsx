import { useState, type ReactNode } from 'react';
import { Link } from 'react-router-dom';
import { me } from '../../api/client';
import { useAsync } from '../../hooks/useAsync';
import { MenuIcon } from '../icons';
import { BottomSheet } from '../ui/BottomSheet';
import { IconButton } from '../ui/IconButton';
import styles from './AppHeader.module.css';

const LINKS = [
  { href: '/#about', label: 'אודות' },
  { href: 'https://huggingface.co/ivrit-ai', label: 'הנתונים ב-Huggingface' },
  { href: 'https://www.ivrit.ai/en/the-license/', label: 'רישיון' },
];

export function Brand() {
  return (
    <Link to="/" className={styles.brand}>
      <img src="/static/logo-light.png" alt="" className={styles.logo} />
      <span className={styles.name}>ivrit.ai</span>
      <span className={styles.explore}>EXPLORE</span>
    </Link>
  );
}

interface Props {
  /** Compact search form on results pages. */
  children?: ReactNode;
  /** Logged-in pages show the account menu; the login page must not fetch /api/me. */
  signedIn?: boolean;
  sticky?: boolean;
}

export function AppHeader({ children, signedIn = true, sticky }: Props) {
  const [menuOpen, setMenuOpen] = useState(false);
  const { data: user } = useAsync(signedIn ? (signal) => me(signal) : null, signedIn ? 'me' : 'anon');

  return (
    <header className={`${styles.header} ${sticky ? styles.sticky : ''}`}>
      <div className={styles.inner}>
        <Brand />
        {children}
        <nav aria-label="ניווט ראשי" className={styles.nav}>
          {LINKS.map((l) => (
            <a key={l.href} href={l.href} className={styles.link} {...(l.href.startsWith('http') ? { target: '_blank', rel: 'noopener noreferrer' } : {})}>
              {l.label}
            </a>
          ))}
          {user && (
            <a href="/logout" className={styles.link} title={user.email}>
              התנתקות
            </a>
          )}
        </nav>
        <IconButton label="תפריט" className={styles.menuButton} onClick={() => setMenuOpen(true)}>
          <MenuIcon />
        </IconButton>
      </div>
      <BottomSheet open={menuOpen} onClose={() => setMenuOpen(false)} title="תפריט">
        <nav aria-label="ניווט" className={styles.sheetNav}>
          {LINKS.map((l) => (
            <a key={l.href} href={l.href} className={styles.sheetLink} onClick={() => setMenuOpen(false)}>
              {l.label}
            </a>
          ))}
          <a href="/privacy" className={styles.sheetLink}>
            מדיניות פרטיות
          </a>
          {user && (
            <a href="/logout" className={styles.sheetLink}>
              התנתקות <span className={styles.email}>{user.email}</span>
            </a>
          )}
        </nav>
      </BottomSheet>
    </header>
  );
}
