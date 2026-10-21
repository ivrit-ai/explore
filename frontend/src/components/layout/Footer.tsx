import { Link } from 'react-router-dom';
import styles from './Footer.module.css';

export function Footer() {
  return (
    <footer className={styles.footer}>
      <p className={styles.inner}>
        אתר זה אוסף נתוני שימוש לשיפור חוויית הגלישה. השימוש באתר מהווה הסכמה ל<Link to="/privacy">מדיניות הפרטיות</Link>.
      </p>
    </footer>
  );
}
