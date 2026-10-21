import { useEffect } from 'react';
import { Brand } from '../components/layout/AppHeader';
import { Footer } from '../components/layout/Footer';
import styles from './LoginPage.module.css';

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
          {/* Full page navigation: the server starts the OAuth flow. */}
          <a href="/authorize" className={styles.google}>
            <img src="/static/google-logo.png" alt="" width="20" height="20" />
            התחברות עם Google
          </a>
        </div>
      </main>
      <Footer />
    </div>
  );
}
