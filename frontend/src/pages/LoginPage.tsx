import { useEffect } from 'react';
import { Brand } from '../components/layout/AppHeader';
import { Footer } from '../components/layout/Footer';
import styles from './LoginPage.module.css';

/** Where to land after sign-in: a same-origin path only, never another host. */
function returnTo(): string {
  const next = new URLSearchParams(window.location.search).get('next') ?? '/';
  return next.startsWith('/') && !next.startsWith('//') && !next.startsWith('/\\') ? next : '/';
}

export function LoginPage() {
  useEffect(() => {
    document.title = 'התחברות — ivrit.ai Explore';
  }, []);

  return (
    <div className={styles.page}>
      <main className={styles.main}>
        <div className={styles.panel}>
          <Brand />
          <h1 className={styles.title}>התחברות ל-ivrit.ai Explore</h1>
          <p className={styles.text}>הגישה למאגר דורשת התחברות עם חשבון Google.</p>
          {/* Full page navigation: the hosting platform runs the Google sign-in. */}
          <a href={`/xhost-auth/login?${new URLSearchParams({ return_to: returnTo() })}`} className={styles.google}>
            <img src="/static/google-logo.png" alt="" width="20" height="20" />
            התחברות עם Google
          </a>
        </div>
      </main>
      <Footer />
    </div>
  );
}
